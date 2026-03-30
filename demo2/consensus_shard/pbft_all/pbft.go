// The pbft consensus process

package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"blockEmulator/utils"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

type PbftConsensusNode struct {
	// the local config about pbft
	RunningNode *shard.Node // the node information
	ShardID     uint64      // denote the ID of the shard (or pbft), only one pbft consensus in a shard
	NodeID      uint64      // denote the ID of the node in the pbft (shard)

	// the data structure for blockchain
	CurChain *chain.BlockChain // all node in the shard maintain the same blockchain
	db       ethdb.Database    // to save the mpt

	// the global config about pbft
	pbftChainConfig     *params.ChainConfig          // the chain config in this pbft
	ip_nodeTable        map[uint64]map[uint64]string // denote the ip of the specific node
	takeoverRouting     map[uint64][]string          // target shard -> routing IPs during takeover
	takeoverRouteCursor map[uint64]uint64            // round-robin cursor for takeover routing
	routeLock           sync.Mutex
	node_nums           uint64 // the number of nodes in this pfbt, denoted by N
	malicious_nums      uint64 // f, 3f + 1 = N

	// view change
	view           atomic.Int32 // denote the view of this pbft, the main node can be inferred from this variant
	currentEpoch   atomic.Int32 // current epoch updated by CReconfig
	lastCommitTime atomic.Int64 // the time since last commit.
	viewChangeMap  map[ViewChangeData]map[uint64]bool
	newViewMap     map[ViewChangeData]map[uint64]bool
	vcLock         sync.Mutex // protects viewChangeMap and newViewMap

	// the control message and message checking utils in pbft
	sequenceID        uint64                          // the message sequence id of the pbft
	stopSignal        atomic.Bool                     // send stop signal
	pStop             chan uint64                     // channle for stopping consensus
	requestPool       map[string]*message.Request     // RequestHash to Request
	cntPrepareConfirm map[string]map[*shard.Node]bool // count the prepare confirm message, [messageHash][Node]bool
	cntCommitConfirm  map[string]map[*shard.Node]bool // count the commit confirm message, [messageHash][Node]bool
	isCommitBordcast  map[string]bool                 // denote whether the commit is broadcast
	isReply           map[string]bool                 // denote whether the message is reply
	height2Digest     map[uint64]string               // sequence (block height) -> request, fast read

	// pbft stage wait
	pbftStage              atomic.Int32 // 1->Preprepare, 2->Prepare, 3->Commit, 4->Done
	pbftLock               sync.Mutex
	conditionalVarpbftLock sync.Cond

	// locks about pbft
	sequenceLock sync.Mutex // the lock of sequence
	lock         sync.Mutex // lock the stage
	askForLock   sync.Mutex // lock for asking for a serise of requests

	// seqID of other Shards, to synchronize
	seqIDMap   map[uint64]uint64
	seqMapLock sync.Mutex

	// logger
	pl *pbft_log.PbftLog
	// tcp control
	tcpln       net.Listener
	tcpPoolLock sync.Mutex

	// to handle the message in the pbft
	ihm ExtraOpInConsensus

	// to handle the message outside of pbft
	ohm OpInterShards
}

func pbftVoteThreshold(nodeCount uint64) uint64 {
	if nodeCount == 0 {
		return 0
	}
	// A 3-node committee makes f=(N-1)/3 evaluate to 0, which collapses 2f+1 to 1
	// and causes PBFT / view-change to flap on single-node hiccups. Treat
	// undersized committees as degraded mode and require unanimity instead.
	if nodeCount < 4 {
		return nodeCount
	}
	threshold := 2*((nodeCount-1)/3) + 1
	if threshold > nodeCount {
		return nodeCount
	}
	return threshold
}

func (p *PbftConsensusNode) quorumThreshold() uint64 {
	return pbftVoteThreshold(p.node_nums)
}

// generate a pbft consensus for a node
func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig, messageHandleType string) *PbftConsensusNode {
	p := new(PbftConsensusNode)
	p.ip_nodeTable = params.IPmap_nodeTable
	p.takeoverRouting = make(map[uint64][]string)
	p.takeoverRouteCursor = make(map[uint64]uint64)
	p.node_nums = pcc.Nodes_perShard
	p.ShardID = shardID
	p.NodeID = nodeID
	p.pbftChainConfig = pcc
	fp := params.DatabaseWrite_path + "mptDB/ldb/s" + strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	p.db, err = rawdb.NewLevelDBDatabase(fp, 0, 1, "accountState", false)
	if err != nil {
		log.Panic(err)
	}
	p.CurChain, err = chain.NewBlockChain(pcc, p.db)
	if err != nil {
		log.Panic("cannot new a blockchain")
	}

	// Reputation mode runs are experiment-oriented and should start from a
	// clean logical chain. Reusing persisted blocks from previous runs can make
	// nodes inside the same shard load different heights, which then causes
	// sequence/parent mismatches, view-change storms, and false leader timeouts.
	if messageHandleType == "Reputation" {
		if p.CurChain != nil && p.CurChain.CurrentBlock != nil && p.CurChain.CurrentBlock.Header.Number > 0 {
			fmt.Printf("[BOOT] S%dN%d loaded historical chain height=%d, reset to genesis for clean reputation run\n",
				shardID, nodeID, p.CurChain.CurrentBlock.Header.Number)
		}
		p.CurChain.ResetToGenesis()
	}

	p.RunningNode = &shard.Node{
		NodeID:                 nodeID,
		ShardID:                shardID,
		IPaddr:                 p.ip_nodeTable[shardID][nodeID],
		NodeType:               shard.NodeType_Consensus,
		SuperviseTargetShardID: params.NoShardID,
		SupervisedByShardID:    ^uint64(0), // sentinel: not yet assigned
	}
	if messageHandleType == "Reputation" {
		p.RunningNode.NodeType = params.GetNodeTypeForReputation(shardID, p.pbftChainConfig.ShardNums)
	}

	p.stopSignal.Store(false)
	p.sequenceID = p.CurChain.CurrentBlock.Header.Number + 1
	p.pStop = make(chan uint64)
	p.requestPool = make(map[string]*message.Request)
	p.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
	p.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	p.isCommitBordcast = make(map[string]bool)
	p.isReply = make(map[string]bool)
	p.height2Digest = make(map[uint64]string)
	p.malicious_nums = (p.node_nums - 1) / 3

	// init view & last commit time
	p.view.Store(0)
	p.currentEpoch.Store(0)
	p.lastCommitTime.Store(time.Now().Add(time.Second * 5).UnixMilli())
	p.viewChangeMap = make(map[ViewChangeData]map[uint64]bool)
	p.newViewMap = make(map[ViewChangeData]map[uint64]bool)

	p.seqIDMap = make(map[uint64]uint64)

	p.pl = pbft_log.NewPbftLog(shardID, nodeID)

	// choose how to handle the messages in pbft or beyond pbft
	switch string(messageHandleType) {
	case "CLPA_Broker":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod_forBroker{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPABrokerOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "CLPA":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPARelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "Broker":
		p.ihm = &RawBrokerPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawBrokerOutsideModule{
			pbftNode: p,
		}
	case "Reputation":
		repMod := NewReputationSupervisionMod(p)
		p.ihm = NewReputationPbftInsideExtraHandleMod(p, repMod)
		p.ohm = NewReputationRelayOutsideModuleWithRepMod(p, repMod)
	default:
		p.ihm = &RawRelayPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawRelayOutsideModule{
			pbftNode: p,
		}
	}

	// set pbft stage now
	p.conditionalVarpbftLock = *sync.NewCond(&p.pbftLock)
	p.pbftStage.Store(1)

	return p
}

// 在 PbftConsensusNode 中增加一个方法
func (p *PbftConsensusNode) PerformPoWAndJoin() {
	// 1. 获取种子 (模拟：节点需要先知道系统的种子，可以通过配置文件或首次握手获取)
	systemSeed := int64(12345)

	fmt.Printf("[Node %d] Start solving PoW puzzle...\n", p.NodeID)

	// 2. 解决难题 (挖矿) - 这是一个耗时操作
	nonce := utils.SolvePuzzle(systemSeed, p.NodeID)

	fmt.Printf("[Node %d] PoW Solved! Nonce: %d. Sending Register Request...\n", p.NodeID, nonce)

	// 3. 发送注册请求给 Supervisor
	req := message.JoinRequestMsg{
		NodeID:  p.NodeID,
		ShardID: p.ShardID,
		Nonce:   nonce,
	}
	reqBytes, _ := json.Marshal(req)
	msg := message.MergeMessage(message.CJoinRequest, reqBytes)

	networks.TcpDial(msg, params.SupervisorAddr)

	// 4. (可选) 阻塞等待 Supervisor 的确认
	// 在模拟器中，为了简化，我们可以假设算出答案就允许运行。
	// 如果要严格同步，可以使用 channel 阻塞，直到收到 CJoinResponse
}

// handle the raw message, send it to corresponded interfaces
func (p *PbftConsensusNode) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	// pbft inside message type
	case message.CPrePrepare:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handlePrePrepare(content)
	case message.CPrepare:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handlePrepare(content)
	case message.CCommit:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handleCommit(content)

	case message.ViewChangePropose:
		go p.handleViewChangeMsg(content)
	case message.NewChange:
		go p.handleNewViewMsg(content)

	case message.CRequestOldrequest:
		p.handleRequestOldSeq(content)
	case message.CSendOldrequest:
		p.handleSendOldSeq(content)

	case message.CStop:
		p.WaitToStop()

	// handle the message from outside
	default:
		go p.ohm.HandleMessageOutsidePBFT(msgType, content)
	}
}

func (p *PbftConsensusNode) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		if p.stopSignal.Load() {
			return
		}
		switch err {
		case nil:
			p.tcpPoolLock.Lock()
			p.handleMessage(clientRequest)
			p.tcpPoolLock.Unlock()
		case io.EOF:
			return
		default:
			// TCP 连接池环境下，远端重连会导致旧连接被强制关闭，属于正常生命周期
			errStr := err.Error()
			if !strings.Contains(errStr, "forcibly closed") &&
				!strings.Contains(errStr, "connection reset") &&
				!strings.Contains(errStr, "broken pipe") {
				log.Printf("error: %v\n", err)
			}
			return
		}
	}
}

// A consensus node starts tcp-listen.
func (p *PbftConsensusNode) TcpListen() {
	ln, err := net.Listen("tcp", p.RunningNode.IPaddr)
	p.tcpln = ln
	if err != nil {
		log.Panic(err)
	}
	for {
		conn, err := p.tcpln.Accept()
		if err != nil {
			return
		}
		go p.handleClientRequest(conn)
	}
}

// When receiving a stop message, this node try to stop.
func (p *PbftConsensusNode) WaitToStop() {
	p.pl.Plog.Println("handling stop message")
	p.stopSignal.Store(true)
	networks.CloseAllConnInPool()
	p.tcpln.Close()
	p.closePbft()
	p.pl.Plog.Println("handled stop message in TCPListen Routine")
	// 非阻塞发送：Storage 节点的 Propose 循环不读 pStop channel，
	// 阻塞发送会导致 WaitToStop 永远卡住。stopSignal 已设为 true，
	// 所有 goroutine 都会通过检查 stopSignal 自行退出。
	select {
	case p.pStop <- 1:
	default:
	}
}

// close the pbft
func (p *PbftConsensusNode) closePbft() {
	p.CurChain.CloseBlockChain()
}
