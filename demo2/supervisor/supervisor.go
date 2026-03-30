// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp control
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	comMod committee.CommitteeModule

	// measure components
	testMeasureMods []measure.MeasureModule
	measureLock     sync.Mutex
	CurEpochSeed    int64
	CurrentMainMap  map[uint64]uint64
	CurrentBSGMap   map[uint64][]uint64
	NodeMetricsMap  map[string]*NodeMetrics

	// per-shard tx tracking for pool size estimation
	shardTxProcessed map[uint64]int
	lastShardTxPool  map[uint64]int

	// 保留每轮 Epoch 开始时各分片的原始节点 IP，不会被接管路由覆盖
	EpochOriginalIpTable map[uint64]map[uint64]string

	// 候补节点池：被驱逐的节点进入此池，新节点可从此池补充到分片中
	CandidatePool []string

	// takenOverShards tracks shards that have already been taken over in this epoch.
	// Prevents repeated takeover for the same target, stopping cascade loops.
	takenOverShards map[uint64]bool

	// bsgSupervisionActive tracks shards whose supervision has been taken over by BSG.
	// Key: target shard being supervised, Value: BSG member shard IDs doing the supervision.
	// Supervisor forwards CBlockInfo from these targets to BSG members.
	bsgSupervisionActive map[uint64][]uint64

	// nodeRoundActions keeps per-node per-round mutually-exclusive action states.
	// Key1: node IP, Key2: round key (epoch/shard/seq), Value: "P" | "S" | "E".
	nodeRoundActions map[string]map[string]string

	// leaderSilenceReasons records leaders judged as silent/offline in current epoch.
	// Key: leader IP, Value: triggering supervision reason.
	leaderSilenceReasons map[string]string

	// LastEpochLeaders remembers each CS shard's last selected leader IP
	// so reconfiguration can avoid repeatedly pinning leadership to one node.
	LastEpochLeaders map[uint64]string

	// lastBlockInfoTime records latest CBlockInfo arrival time from CS shards.
	lastBlockInfoTime time.Time

	// lastShardBlockInfo stores the latest CBlockInfo arrival per CS shard.
	// Used to diagnose why final waiting reaches 300s timeout.
	lastShardBlockInfo map[uint64]time.Time
	// lastFallbackTakeoverAt paces fallback-triggered takeovers to avoid
	// bursty multi-shard takeover spikes in a single scan tick.
	lastFallbackTakeoverAt time.Time

	// currentEpoch is the active epoch for online voting action accounting.
	// Late CBlockInfo from previous epochs must not pollute current-epoch P/S/E.
	currentEpoch int
	// epochStartTime marks when currentEpoch becomes active.
	// Used to avoid treating reconfiguration warmup as immediate silence faults.
	epochStartTime time.Time

	// staleEpochReportShards tracks shards that keep reporting old-epoch blocks
	// during the current epoch, usually indicating reconfiguration drift.
	staleEpochReportShards map[uint64]bool
	// drainingMode is true after all transactions have been injected and the
	// supervisor is only waiting for remaining in-flight work to drain.
	// In this phase, no new blocks can simply mean "idle shard", so timeout-
	// based takeover decisions should be suppressed.
	drainingMode bool

	// reconfigAckEpoch/reconfigAckPending form a lightweight barrier so
	// CReconfig can be retried for nodes that have not acknowledged yet.
	reconfigAckEpoch   int
	reconfigAckPending map[string]struct{}

	// epochReconfigNoAck records nodes that failed all reconfig ACK waves for an epoch.
	// Key: epoch, Value: node IP set.
	epochReconfigNoAck map[int]map[string]struct{}
}

type SupervisionTopology struct {
	MainMap map[uint64]uint64   // Key: Supervisor, Value: Target
	BSGMap  map[uint64][]uint64 // Key: Target, Value: []BSG_Member_ShardID
}

type NodeMetrics struct {
	Reputation         float64 // 当前声誉值 (范围 0~1)
	P                  int     // 正常工作次数 (Positive)
	S                  int     // 不响应次数 (Silence/Timeout)
	E                  int     // 发送错误/作恶次数 (Error/Evil)
	EliminatedAtEpoch  int     // 最近一次被驱逐时的 Epoch (-1 表示从未被驱逐)
	LastTakenOverEpoch int     // 最近一次所在分片被接管的 Epoch (-1 表示从未被接管)
	TakenOverCount     int     // 所在分片被接管累计次数
	HasServedInCS      bool    // 是否曾真实加入过共识分片；用于候选池回补优先级
}

type rankedNode struct {
	IP  string
	Rep float64
}

type reconfigTarget struct {
	newShardID   uint64
	newNodeID    uint64
	mainTarget   uint64
	supervisedBy uint64
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, measureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()

	d.CurEpochSeed = time.Now().UnixNano()

	d.Ss = signal.NewStopSignal(3 * params.ConsensusShardCount())

	switch committeeMethod {
	case "CLPA_Broker":
		d.comMod = committee.NewCLPACommitteeMod_Broker(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "CLPA":
		d.comMod = committee.NewCLPACommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "Broker":
		d.comMod = committee.NewBrokerCommitteeMod(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	case "Reputation":
		d.comMod = committee.NewReputationCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl,
			params.DatasetFile, params.TotalDataSize, params.TxBatchSize,
			params.ReconfigTimeGap,
			func(epoch int) map[uint64]map[uint64]string {
				d.EpochReconfiguration(epoch)
				return d.Ip_nodeTable
			},
		)
	default:
		d.comMod = committee.NewRelayCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	}

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range measureModNames {
		switch mModName {
		case "TPS_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Relay())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Relay())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Relay())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		case "Tx_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxDetail())
		case "Takeover_Time":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTakeoverTime())
		default:
		}
	}
	d.NodeMetricsMap = make(map[string]*NodeMetrics)
	d.shardTxProcessed = make(map[uint64]int)
	d.lastShardTxPool = make(map[uint64]int)
	d.CandidatePool = make([]string, 0)
	d.takenOverShards = make(map[uint64]bool)
	d.bsgSupervisionActive = make(map[uint64][]uint64)
	d.nodeRoundActions = make(map[string]map[string]string)
	d.leaderSilenceReasons = make(map[string]string)
	d.LastEpochLeaders = make(map[uint64]string)
	d.lastBlockInfoTime = time.Now()
	d.lastShardBlockInfo = make(map[uint64]time.Time)
	d.lastFallbackTakeoverAt = time.Time{}
	d.currentEpoch = 0
	d.epochStartTime = time.Now()
	d.staleEpochReportShards = make(map[uint64]bool)
	d.reconfigAckEpoch = -1
	d.reconfigAckPending = make(map[string]struct{})
	d.epochReconfigNoAck = make(map[int]map[string]struct{})

	// 只为共识分片 (CS) 的节点分配初始声誉值，存储分片 (SS) 不参与共识，无需声誉
	numCSInit := uint64(params.ConsensusShardCount())
	for sid := uint64(0); sid < numCSInit; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			for _, nodeIP := range nodes {
				d.NodeMetricsMap[nodeIP] = &NodeMetrics{
					Reputation: 0.5,
					P:          0, S: 0, E: 0,
					EliminatedAtEpoch:  -1,
					LastTakenOverEpoch: -1,
					HasServedInCS:      true,
				}
			}
		}
	}

	// 初始快照：保存原始节点表用于惩罚和重配置
	d.snapshotOriginalIpTable()

	// 初始化预留候补节点池（独立端口，不占用 CS/SS 固定节点）。
	d.initReserveCandidatePool()
}

// Supervisor received the block information from the leaders, and handle these
// message to measure the performances.
func (d *Supervisor) handleBlockInfos(content []byte) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}

	if !params.IsConsensusShardForReputation(bim.SenderShardID, d.ChainConfig.ShardNums) {
		return
	}

	d.tcpLock.Lock()
	activeEpoch := d.currentEpoch
	if bim.Epoch != activeEpoch {
		d.staleEpochReportShards[bim.SenderShardID] = true
		d.tcpLock.Unlock()
		d.sl.Slog.Printf("Supervisor: ignore stale block report (senderShard=%d, blockEpoch=%d, activeEpoch=%d, height=%d, bodyLen=%d)\n",
			bim.SenderShardID, bim.Epoch, activeEpoch, bim.BlockHeight, bim.BlockBodyLength)
		return
	}
	d.lastBlockInfoTime = time.Now()
	d.lastShardBlockInfo[bim.SenderShardID] = d.lastBlockInfoTime
	d.lastShardTxPool[bim.SenderShardID] = bim.TxPoolSize
	d.tcpLock.Unlock()

	if bim.BlockBodyLength == -1 {
		shardID := bim.SenderShardID

		// 将名单转为 Map 加速查找
		voterMap := make(map[string]bool)
		for _, ip := range bim.Voters {
			voterMap[ip] = true
		}

		roundKey := d.makeRoundKey(shardID, bim.Epoch, bim.BlockHeight)

		d.tcpLock.Lock()
		isTakenOver := d.takenOverShards[shardID]
		// 使用原始成员表查找该分片的真实节点，防止 BSG 覆盖导致追踪错误节点
		nodes, ok := d.EpochOriginalIpTable[shardID]
		if !ok {
			d.tcpLock.Unlock()
			return
		}

		if isTakenOver {
			// 分片被接管后，仍可能收到“接管前产生但延迟到达”的合法投票上报。
			// 若投票者与原始成员完全无交集，则判定为接管后的重定向流量，直接忽略。
			hasOriginalVoter := false
			for _, ip := range nodes {
				if voterMap[ip] {
					hasOriginalVoter = true
					break
				}
			}
			if !hasOriginalVoter {
				d.tcpLock.Unlock()
				return
			}
		}

		for nid, ip := range nodes {
			if nid == 0 {
				// 只要本分片本轮投票报告到达，默认 leader 参与该轮。
				// Voters 列表可能不包含 leader，自然会造成 leader 被漏记为 P=0。
				d.updateRoundActionLocked(ip, roundKey, "P")
				continue
			}
			action := "S"
			if voterMap[ip] {
				action = "P"
			}
			d.updateRoundActionLocked(ip, roundKey, action)
		}
		d.tcpLock.Unlock()

		return
	}

	if bim.BlockBodyLength == 0 {
		d.Ss.StopGap_Inc()
	} else {
		d.Ss.StopGap_Reset()
		d.shardTxProcessed[bim.SenderShardID] += bim.BlockBodyLength
	}

	d.measureLock.Lock()
	d.comMod.HandleBlockInfo(bim)
	for _, measureMod := range d.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
	d.measureLock.Unlock()

	// Forward CBlockInfo to BSG members if BSG supervision is active for this shard.
	// This allows BSG members' handleBlockHeaderSupervision to track the target's liveness.
	if bsgMembers, ok := d.bsgSupervisionActive[bim.SenderShardID]; ok {
		fwdMsg := message.MergeMessage(message.CBlockInfo, content)
		for _, memberShardID := range bsgMembers {
			if nodes, exists := d.Ip_nodeTable[memberShardID]; exists {
				for _, ip := range nodes {
					go networks.TcpDial(fwdMsg, ip)
				}
			}
		}
	}
}

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {
	// 生成 epoch 0 的监督拓扑，使 CurrentMainMap/CurrentBSGMap 在交易处理开始前就可用
	d.sl.Slog.Println("=== Generating initial supervision topology (Epoch 0) ===")
	topo0 := d.generateSupervisionTopology(0, d.CurEpochSeed)
	d.CurrentMainMap = topo0.MainMap
	d.CurrentBSGMap = topo0.BSGMap

	// In Reputation mode, send initial supervision assignments to all nodes
	// so they know their monitoring targets and BSG membership from epoch 0.
	if params.CommitteeMethod[params.ConsensusMethod] == "Reputation" {
		d.sendInitialSupervisionAssignments(topo0)
	}

	// MsgSendingControl 是阻塞调用，直到所有交易处理完才返回
	d.comMod.MsgSendingControl()

	// 所有交易已处理完毕，等待节点出空块来确认结束
	d.sl.Slog.Printf("Supervisor: all configured transactions injected (TotalDataSize=%d), waiting for confirmed drain (need consecutive empty blocks and all observed shard pools empty)...\n", params.TotalDataSize)
	d.tcpLock.Lock()
	d.drainingMode = true
	d.lastBlockInfoTime = time.Now()
	d.tcpLock.Unlock()
	waitTick := 0
	const maxWaitSeconds = 300
	for {
		poolsDrained, poolDetail := d.allObservedShardPoolsDrained()
		if d.Ss.GapEnough() && poolsDrained {
			break
		}

		time.Sleep(time.Second)
		waitTick++

		// L1 fallback: if a shard stays silent for too long and no supervision
		// report arrives (e.g., reporter is also in view-change), synthesize
		// a supervision fault to trigger takeover and avoid endless stalls.
		d.checkSilentShardTakeoverFallback()

		if waitTick%5 == 0 {
			if poolDetail == "" {
				poolDetail = "none"
			}
			d.sl.Slog.Printf("Supervisor: still waiting... (%d seconds elapsed, stopGap=%d/%d, nonEmptyObservedPools=%s)\n",
				waitTick, d.Ss.GetGap(), d.Ss.GetThreshold(), poolDetail)
		}
		if waitTick >= maxWaitSeconds {
			d.sl.Slog.Printf("Supervisor: wait timeout reached (%ds). Forcing stop to avoid infinite wait (stopGap=%d/%d).\n", waitTick, d.Ss.GetGap(), d.Ss.GetThreshold())
			d.logWaitTimeoutDiagnostics()
			break
		}
	}

	// send stop message
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	d.sl.Slog.Println("Supervisor: now sending cstop message to all nodes")
	// 收集所有需要发送 CStop 的唯一 IP 地址。
	// BSG 接管会将 Ip_nodeTable 中被接管分片的 IP 覆盖为接管方的 IP，
	// 导致原始节点永远收不到停止信号。因此同时遍历 EpochOriginalIpTable
	// 确保所有原始节点都能收到 CStop。
	stopTargets := make(map[string]bool)
	for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			for _, ip := range nodes {
				stopTargets[ip] = true
			}
		}
		if origNodes, ok := d.EpochOriginalIpTable[sid]; ok {
			for _, ip := range origNodes {
				stopTargets[ip] = true
			}
		}
	}
	for ip := range stopTargets {
		networks.TcpDial(stopmsg, ip)
	}
	// make sure all stop messages are sent.
	time.Sleep(time.Duration(params.Delay+params.JitterRange+3) * time.Millisecond)

	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
}

func (d *Supervisor) notifyTakeoverStartForMeasure(supervisionContent []byte) {
	msg := message.MergeMessage(message.CSupervisionRes, supervisionContent)
	d.measureLock.Lock()
	for _, mm := range d.testMeasureMods {
		mm.HandleExtraMessage(msg)
	}
	d.measureLock.Unlock()
}

// checkSilentShardTakeoverFallback is an L1-side safety net.
// It scans consensus shards and triggers takeover when a shard has not
// uploaded CBlockInfo for SupervisionTimeout, even if no CS_l report arrives.
func (d *Supervisor) checkSilentShardTakeoverFallback() {
	d.tcpLock.Lock()
	if d.drainingMode {
		d.tcpLock.Unlock()
		return
	}
	d.tcpLock.Unlock()

	timeout := time.Duration(params.SupervisionTimeout) * time.Millisecond
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	const fallbackTakeoverCooldown = 8 * time.Second

	now := time.Now()
	numCS := uint64(params.ConsensusShardCount())
	grace := initialTakeoverGraceDuration()

	type candidate struct {
		targetShardID   uint64
		reporterShardID uint64
		epoch           int
		reporterIP      string
		silentFor       time.Duration
	}
	candidates := make([]candidate, 0)

	d.tcpLock.Lock()
	if !d.lastFallbackTakeoverAt.IsZero() && now.Sub(d.lastFallbackTakeoverAt) < fallbackTakeoverCooldown {
		d.tcpLock.Unlock()
		return
	}
	activeEpoch := d.currentEpoch
	epochStart := d.epochStartTime
	if epochStart.IsZero() {
		epochStart = now
	}

	for sid := uint64(0); sid < numCS; sid++ {
		if d.takenOverShards[sid] {
			continue
		}

		silentFor := now.Sub(epochStart)
		hasFreshBlock := false
		if last, ok := d.lastShardBlockInfo[sid]; ok && !last.IsZero() {
			silentFor = now.Sub(last)
			hasFreshBlock = true
		}

		// Keep fallback and report-based takeover gates consistent during reconfig warmup.
		if !hasFreshBlock && now.Sub(epochStart) < grace {
			continue
		}
		if silentFor < timeout {
			continue
		}

		reporterShardID, found := d.findSupervisorOf(sid)
		if !found {
			continue
		}

		if d.takenOverShards[reporterShardID] {
			continue
		}

		reporterIP := ""
		if nodes, exists := d.Ip_nodeTable[reporterShardID]; exists {
			reporterIP = nodes[0]
		}
		if reporterIP == "" {
			continue
		}

		reporterLast, reporterOK := d.lastShardBlockInfo[reporterShardID]
		if !reporterOK || reporterLast.IsZero() || now.Sub(reporterLast) >= timeout {
			continue
		}

		candidates = append(candidates, candidate{
			targetShardID:   sid,
			reporterShardID: reporterShardID,
			epoch:           activeEpoch,
			reporterIP:      reporterIP,
			silentFor:       silentFor,
		})
	}
	d.tcpLock.Unlock()

	if len(candidates) == 0 {
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].silentFor == candidates[j].silentFor {
			return candidates[i].targetShardID < candidates[j].targetShardID
		}
		return candidates[i].silentFor > candidates[j].silentFor
	})

	for _, c := range candidates {
		reporter := &shard.Node{
			NodeID:  0,
			ShardID: c.reporterShardID,
			IPaddr:  c.reporterIP,
		}

		reason := fmt.Sprintf("L1 fallback timeout: shard silent for %s without valid supervision report", c.silentFor.Round(time.Second))
		report := message.SupervisionResultMsg{
			ReporterShardID: c.reporterShardID,
			TargetShardID:   c.targetShardID,
			Epoch:           c.epoch,
			SeqID:           0,
			IsFaulty:        true,
			Reason:          reason,
			CurrentPoolSize: 0,
			TargetPoolSize:  0,
			ReporterNode:    reporter,
		}
		reportBytes, _ := json.Marshal(report)
		if d.handleSupervisionRes(reportBytes) {
			d.sl.Slog.Printf("L1: Fallback-triggered takeover check accepted for silent shard %d (reported by shard %d, silent=%s)",
				c.targetShardID, c.reporterShardID, c.silentFor.Round(time.Second))
			d.tcpLock.Lock()
			d.lastFallbackTakeoverAt = time.Now()
			d.tcpLock.Unlock()
			break
		}
	}
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	case message.CBlockInfo:
		d.handleBlockInfos(content)
		// add codes for more functionality
	case message.CJoinRequest:
		d.handleJoinRequest(content)
	case message.CUploadTB:
		d.handleUploadTB(content)
		d.measureLock.Lock()
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
		d.measureLock.Unlock()
	case message.CSupervisionRes:
		d.handleSupervisionRes(content)
	case message.CReconfigAck:
		d.handleReconfigAck(content)
	default:
		d.comMod.HandleOtherMessage(msg)
		d.measureLock.Lock()
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
		d.measureLock.Unlock()
	}
}

func (d *Supervisor) handleReconfigAck(content []byte) {
	var ack message.ReconfigAckMsg
	if err := json.Unmarshal(content, &ack); err != nil {
		return
	}

	d.tcpLock.Lock()
	if ack.Epoch != d.reconfigAckEpoch {
		d.tcpLock.Unlock()
		return
	}

	if _, ok := d.reconfigAckPending[ack.NodeIP]; !ok {
		d.tcpLock.Unlock()
		return
	}

	delete(d.reconfigAckPending, ack.NodeIP)
	remaining := len(d.reconfigAckPending)
	d.tcpLock.Unlock()

	d.sl.Slog.Printf("[Epoch %d] Reconfig ACK from %s -> Shard %d Node %d (remaining=%d)\n",
		ack.Epoch, ack.NodeIP, ack.NewShardID, ack.NewNodeID, remaining)
}

func (d *Supervisor) snapshotPendingReconfigAcks(epoch int) []string {
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()

	if epoch != d.reconfigAckEpoch {
		return nil
	}

	pending := make([]string, 0, len(d.reconfigAckPending))
	for ip := range d.reconfigAckPending {
		pending = append(pending, ip)
	}
	sort.Strings(pending)
	return pending
}

func (d *Supervisor) handleJoinRequest(content []byte) {
	var msg message.JoinRequestMsg
	json.Unmarshal(content, &msg)

	// 1. 获取系统当前的随机种子
	systemSeed := d.CurEpochSeed

	d.sl.Slog.Printf("Received Join Request from Node %d (Shard %d). Verifying PoW...\n", msg.NodeID, msg.ShardID)

	// 2. 验证 POW
	// 【修改核心】：直接在判断条件前加上 "true ||" ，强制让所有节点无条件通过验证！
	if true || utils.VerifyPoW(systemSeed, msg.NodeID, msg.Nonce) {
		d.sl.Slog.Printf("✔ Node %d (Shard %d) passed PoW check. Access Granted.\n", msg.NodeID, msg.ShardID)

		// 3. 发送成功响应
		res := message.JoinResponseMsg{Success: true, Message: "Welcome to the network"}
		resBytes, _ := json.Marshal(res)
		respMsg := message.MergeMessage(message.CJoinResponse, resBytes)

		// 查找该节点的 IP 并发送
		if nodes, ok := d.Ip_nodeTable[msg.ShardID]; ok {
			if targetIP, ok := nodes[msg.NodeID]; ok {
				networks.TcpDial(respMsg, targetIP)
			}
		}

	} else {
		// 因为上面加了 true，这段代码永远不会被执行到了，但保留在这里以免破坏原代码结构
		d.sl.Slog.Printf("✘ Node %d failed PoW check. Access Denied.\n", msg.NodeID)
		// 发送失败响应
		res := message.JoinResponseMsg{Success: false, Message: "PoW verification failed"}
		resBytes, _ := json.Marshal(res)
		respMsg := message.MergeMessage(message.CJoinResponse, resBytes)

		if nodes, ok := d.Ip_nodeTable[msg.ShardID]; ok {
			if targetIP, ok := nodes[msg.NodeID]; ok {
				networks.TcpDial(respMsg, targetIP)
			}
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			d.handleMessage(clientRequest)
		case io.EOF:
			return
		default:
			// TCP 连接池环境下，节点重连/退出导致旧连接被强制关闭属于正常生命周期
			if !networks.IsTransientNetErr(err) {
				log.Printf("error: %v\n", err)
			}
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	d.tcpLn = ln
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.sl.Slog.Println("Closing...")
	d.measureLock.Lock()
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}
	d.measureLock.Unlock()
	networks.CloseAllConnInPool()
	d.tcpLn.Close()
}

// 处理 TB 上传 (L1 验证)
func (d *Supervisor) handleUploadTB(content []byte) {
	var msg message.UploadTBMsg
	json.Unmarshal(content, &msg)

	shardID := msg.TB.ShardID
	if shardID == 0 && msg.ShardID != 0 {
		shardID = msg.ShardID
	}

	// 接管完成 TB 不进行失败注入，直接记录即可
	if msg.IsTakeoverCompletion {
		d.sl.Slog.Printf("L1: Takeover-completion TB received for Shard %d.\n", shardID)
		return
	}

	// ========== TB 验证失败注入 ==========
	if params.TBFailureProbability > 0 && rand.Float64() < params.TBFailureProbability {
		d.sl.Slog.Printf("L1: ⚠ TB from Shard %d REJECTED (simulated validation failure, prob=%.2f)\n",
			shardID, params.TBFailureProbability)
		// 不在此处直接惩罚节点，统一由 handleSupervisionRes 处理，避免双重惩罚

		// 不发送 CConfirmTB → CS_l 的 tbMonitorTimer 将超时 → 触发接管流程
		// 同时主动触发接管决策
		d.tcpLock.Lock()
		activeEpoch := d.currentEpoch
		d.tcpLock.Unlock()
		if supervisorShardID, ok := d.findSupervisorOf(shardID); ok {
			// 需要构造带有 ReporterNode 的消息，以便 handleSupervisionRes 能发送接管指令
			var reporterNode *shard.Node
			if leaderIP, exists := d.Ip_nodeTable[supervisorShardID][0]; exists {
				reporterNode = &shard.Node{
					NodeID:  0,
					ShardID: supervisorShardID,
					IPaddr:  leaderIP,
				}
			}
			rejectInfo := message.SupervisionResultMsg{
				ReporterShardID: supervisorShardID,
				TargetShardID:   shardID,
				Epoch:           activeEpoch,
				SeqID:           msg.BlockHeight,
				IsFaulty:        true,
				Reason:          "TB Validation Failed (simulated)",
				CurrentPoolSize: 0,
				TargetPoolSize:  0, // supervisor will use its own estimate in handleSupervisionRes
				ReporterNode:    reporterNode,
			}
			d.sl.Slog.Printf("L1: Triggering takeover for Shard %d due to TB validation failure\n", shardID)
			rejectBytes, _ := json.Marshal(rejectInfo)
			d.handleSupervisionRes(rejectBytes)
		}
		return
	}

	// ========== 正常验证通过 ==========
	d.sl.Slog.Printf("L1: TB from Shard %d received and verified.\n", shardID)

	// 注意：暂时不考虑 TB 签名对声誉值的影响，仅通过共识参与(C/S/E)计算声誉
	confirmMsg := message.MergeMessage(message.CConfirmTB, []byte("Verified"))
	for _, ip := range d.Ip_nodeTable[shardID] {
		networks.TcpDial(confirmMsg, ip)
	}
	// Notify the supervising shard too, so supervisor-side TB timers can stop if enabled.
	if supervisorShardID, ok := d.findSupervisorOf(shardID); ok {
		if nodes, exists := d.Ip_nodeTable[supervisorShardID]; exists {
			for _, ip := range nodes {
				networks.TcpDial(confirmMsg, ip)
			}
		}
	}
}

// snapshotOriginalIpTable 深拷贝 Ip_nodeTable，保留本 Epoch 开始时的原始成员关系。
// 在 BSG 接管路由覆盖 Ip_nodeTable 后，惩罚和 Epoch 重配仍能找到正确的节点。
func (d *Supervisor) snapshotOriginalIpTable() {
	d.EpochOriginalIpTable = make(map[uint64]map[uint64]string)
	for sid, nodes := range d.Ip_nodeTable {
		cp := make(map[uint64]string, len(nodes))
		for nid, ip := range nodes {
			cp[nid] = ip
		}
		d.EpochOriginalIpTable[sid] = cp
	}
}

// initReserveCandidatePool initializes a standby candidate pool with virtual
// nodes on fresh ports. These candidates are neither CS nor SS until promoted.
func (d *Supervisor) initReserveCandidatePool() {
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()

	numCS := uint64(params.ConsensusShardCount())

	for sid := uint64(0); sid < numCS; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			if leaderIP, exists := nodes[0]; exists {
				d.LastEpochLeaders[sid] = leaderIP
			}
		}
	}

	targetSize := d.reservePoolTargetSizeLocked()
	d.ensureCandidatePoolCapacityLocked(targetSize, nil)
	d.sl.Slog.Printf("[Init] CandidatePool seeded with virtual reserve nodes: %d\n", len(d.CandidatePool))
}

func (d *Supervisor) reservePoolTargetSizeLocked() int {
	nodesPerShard := params.NodesInShard
	if nodesPerShard <= 0 {
		nodesPerShard = int(d.ChainConfig.Nodes_perShard)
	}
	if nodesPerShard <= 0 {
		nodesPerShard = 4
	}
	target := nodesPerShard * params.ConsensusShardCount()
	if target < nodesPerShard {
		target = nodesPerShard
	}
	return target
}

func splitHostPort(ip string) (string, int, bool) {
	idx := strings.LastIndex(ip, ":")
	if idx <= 0 || idx >= len(ip)-1 {
		return "", 0, false
	}
	port, err := strconv.Atoi(ip[idx+1:])
	if err != nil {
		return "", 0, false
	}
	return ip[:idx], port, true
}

func isEndpointReachable(ip string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", ip, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (d *Supervisor) ensureCandidatePoolCapacityLocked(target int, activeSet map[string]bool) {
	if target <= 0 {
		return
	}

	host := "127.0.0.1"
	occupied := make(map[string]bool)
	maxPort := 0

	markOccupied := func(ip string) {
		if ip == "" {
			return
		}
		occupied[ip] = true
		if h, p, ok := splitHostPort(ip); ok {
			if h != "" {
				host = h
			}
			if p > maxPort {
				maxPort = p
			}
		}
	}

	for _, nodes := range d.Ip_nodeTable {
		for _, ip := range nodes {
			markOccupied(ip)
		}
	}
	markOccupied(params.SupervisorAddr)

	if activeSet != nil {
		for ip := range activeSet {
			markOccupied(ip)
		}
	}

	cleanPool := make([]string, 0, len(d.CandidatePool))
	seenPool := make(map[string]bool)
	for _, ip := range d.CandidatePool {
		if ip == "" {
			continue
		}
		if occupied[ip] || seenPool[ip] {
			continue
		}
		seenPool[ip] = true
		cleanPool = append(cleanPool, ip)
		markOccupied(ip)
		if _, exists := d.NodeMetricsMap[ip]; !exists {
			d.NodeMetricsMap[ip] = &NodeMetrics{Reputation: 0.5, EliminatedAtEpoch: -1, LastTakenOverEpoch: -1}
		}
	}
	d.CandidatePool = cleanPool

	for len(d.CandidatePool) < target {
		maxPort++
		ip := fmt.Sprintf("%s:%d", host, maxPort)
		if occupied[ip] {
			continue
		}
		d.CandidatePool = append(d.CandidatePool, ip)
		occupied[ip] = true
		if _, exists := d.NodeMetricsMap[ip]; !exists {
			d.NodeMetricsMap[ip] = &NodeMetrics{Reputation: 0.5, EliminatedAtEpoch: -1, LastTakenOverEpoch: -1}
		}
	}
}

func (d *Supervisor) logWaitTimeoutDiagnostics() {
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()

	now := time.Now()
	numCS := uint64(params.ConsensusShardCount())
	for sid := uint64(0); sid < numCS; sid++ {
		leaderIP := ""
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			if ip, exists := nodes[0]; exists {
				leaderIP = ip
			}
		}
		if ts, ok := d.lastShardBlockInfo[sid]; ok && !ts.IsZero() {
			d.sl.Slog.Printf("Supervisor: timeout diag shard=%d leader=%s lastBlockInfoAgo=%s takenOver=%v\n",
				sid, leaderIP, now.Sub(ts).Round(time.Second), d.takenOverShards[sid])
		} else {
			d.sl.Slog.Printf("Supervisor: timeout diag shard=%d leader=%s lastBlockInfoAgo=never takenOver=%v\n",
				sid, leaderIP, d.takenOverShards[sid])
		}
	}
}

func (d *Supervisor) chooseLeaderWithRotation(sid uint64, ranked []rankedNode) int {
	if len(ranked) == 0 {
		return -1
	}

	if len(ranked) == 1 {
		return 0
	}

	lastLeader, exists := d.LastEpochLeaders[sid]
	if !exists || lastLeader == "" {
		return 0
	}

	for i, item := range ranked {
		if item.IP != lastLeader {
			return i
		}
	}

	return 0
}

func (d *Supervisor) chooseLeaderWithRotationAvoiding(sid uint64, ranked []rankedNode, avoid map[string]struct{}) int {
	if len(ranked) == 0 {
		return -1
	}
	if len(avoid) == 0 {
		return d.chooseLeaderWithRotation(sid, ranked)
	}

	filtered := make([]rankedNode, 0, len(ranked))
	for _, item := range ranked {
		if _, blocked := avoid[item.IP]; blocked {
			continue
		}
		filtered = append(filtered, item)
	}
	if len(filtered) == 0 {
		return d.chooseLeaderWithRotation(sid, ranked)
	}

	chosen := d.chooseLeaderWithRotation(sid, filtered)
	if chosen < 0 || chosen >= len(filtered) {
		chosen = 0
	}
	selectedIP := filtered[chosen].IP
	for i, item := range ranked {
		if item.IP == selectedIP {
			return i
		}
	}
	return 0
}

func rankCommitteeMembers(memberIPs []string, metrics map[string]*NodeMetrics) []rankedNode {
	ranked := make([]rankedNode, 0, len(memberIPs))
	for _, ip := range memberIPs {
		if ip == "" {
			continue
		}
		rep := 0.5
		if m, ok := metrics[ip]; ok {
			rep = m.Reputation
		}
		ranked = append(ranked, rankedNode{IP: ip, Rep: rep})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].Rep == ranked[j].Rep {
			return ranked[i].IP < ranked[j].IP
		}
		return ranked[i].Rep > ranked[j].Rep
	})
	return ranked
}

func reindexCommitteeWithLeader(leaderIP string, ranked []rankedNode) map[uint64]string {
	reindexed := make(map[uint64]string, len(ranked))
	if len(ranked) == 0 {
		return reindexed
	}

	idx := uint64(0)
	if leaderIP != "" {
		reindexed[idx] = leaderIP
		idx++
	}
	for _, item := range ranked {
		if item.IP == leaderIP {
			continue
		}
		reindexed[idx] = item.IP
		idx++
	}
	return reindexed
}

func (d *Supervisor) selectActivationLeader(sid uint64, memberIPs []string, preferred map[string]bool, avoid map[string]struct{}) string {
	rankedAll := rankCommitteeMembers(memberIPs, d.NodeMetricsMap)
	if len(rankedAll) == 0 {
		return ""
	}

	buildSubset := func(requirePreferred bool, allowAvoid bool) []rankedNode {
		subset := make([]rankedNode, 0, len(rankedAll))
		for _, item := range rankedAll {
			if requirePreferred && !preferred[item.IP] {
				continue
			}
			if !allowAvoid {
				if _, blocked := avoid[item.IP]; blocked {
					continue
				}
			}
			subset = append(subset, item)
		}
		return subset
	}

	candidates := buildSubset(true, false)
	if len(candidates) == 0 {
		candidates = buildSubset(true, true)
	}
	if len(candidates) == 0 {
		candidates = buildSubset(false, false)
	}
	if len(candidates) == 0 {
		candidates = rankedAll
	}

	idx := d.chooseLeaderWithRotation(sid, candidates)
	if idx < 0 || idx >= len(candidates) {
		idx = 0
	}
	return candidates[idx].IP
}

func (d *Supervisor) reorderCommitteeForActivation(sid uint64, committee map[uint64]string, preferred map[string]bool, avoid map[string]struct{}) map[uint64]string {
	memberIPs := make([]string, 0, len(committee))
	for _, ip := range committee {
		memberIPs = append(memberIPs, ip)
	}
	ranked := rankCommitteeMembers(memberIPs, d.NodeMetricsMap)
	leaderIP := d.selectActivationLeader(sid, memberIPs, preferred, avoid)
	reordered := reindexCommitteeWithLeader(leaderIP, ranked)
	if leaderIP != "" {
		d.LastEpochLeaders[sid] = leaderIP
	}
	return reordered
}

func pbftActivationQuorum(targetSize int) int {
	if targetSize <= 0 {
		return 1
	}
	quorum := targetSize - (targetSize-1)/3
	if targetSize >= 4 && quorum < 4 {
		quorum = 4
	} else if targetSize >= 3 && quorum < 3 {
		quorum = 3
	}
	if quorum > targetSize {
		quorum = targetSize
	}
	return quorum
}

func cloneShardIPTable(src map[uint64]string) map[uint64]string {
	dst := make(map[uint64]string, len(src))
	for nid, ip := range src {
		dst[nid] = ip
	}
	return dst
}

func copyIPTable(src map[uint64]map[uint64]string) map[uint64]map[uint64]string {
	dst := make(map[uint64]map[uint64]string, len(src))
	for sid, nodes := range src {
		if nodes == nil {
			dst[sid] = make(map[uint64]string)
			continue
		}
		dst[sid] = cloneShardIPTable(nodes)
	}
	return dst
}

func buildSupervisionDispatchMaps(topology SupervisionTopology) (map[uint64]uint64, map[uint64][]uint64) {
	supervisedByMap := make(map[uint64]uint64)
	nodeToBackupTargets := make(map[uint64][]uint64)
	for supervisor, target := range topology.MainMap {
		supervisedByMap[target] = supervisor
	}
	for target, members := range topology.BSGMap {
		for _, member := range members {
			nodeToBackupTargets[member] = append(nodeToBackupTargets[member], target)
		}
	}
	return supervisedByMap, nodeToBackupTargets
}

func buildCSReconfigMessages(epoch int, ipTable map[uint64]map[uint64]string, numCS uint64, topology SupervisionTopology, activate bool) (map[string][]byte, map[string]reconfigTarget) {
	supervisedByMap, nodeToBackupTargets := buildSupervisionDispatchMaps(topology)
	csReconfigMsgs := make(map[string][]byte)
	csReconfigTargets := make(map[string]reconfigTarget)
	for sid := uint64(0); sid < numCS; sid++ {
		mainTarget := topology.MainMap[sid]
		supervisedBy := supervisedByMap[sid]
		backupTargets := nodeToBackupTargets[sid]
		for nid, ip := range ipTable[sid] {
			reconfigMsg := message.ReconfigInfo{
				Epoch:         epoch,
				NewShardID:    sid,
				NewNodeID:     nid,
				Activate:      activate,
				NewIPTable:    ipTable,
				MainTarget:    mainTarget,
				BackupTargets: backupTargets,
				FullBSGMap:    topology.BSGMap,
				SupervisedBy:  supervisedBy,
			}
			bytes, _ := json.Marshal(reconfigMsg)
			csReconfigMsgs[ip] = message.MergeMessage(message.CReconfig, bytes)
			csReconfigTargets[ip] = reconfigTarget{
				newShardID:   sid,
				newNodeID:    nid,
				mainTarget:   mainTarget,
				supervisedBy: supervisedBy,
			}
		}
	}
	return csReconfigMsgs, csReconfigTargets
}

func filterReconfigMessages(msgs map[string][]byte, targets map[string]reconfigTarget, allowed map[string]struct{}) (map[string][]byte, map[string]reconfigTarget) {
	filteredMsgs := make(map[string][]byte, len(allowed))
	filteredTargets := make(map[string]reconfigTarget, len(allowed))
	for ip := range allowed {
		msg, ok := msgs[ip]
		if !ok {
			continue
		}
		filteredMsgs[ip] = msg
		filteredTargets[ip] = targets[ip]
	}
	return filteredMsgs, filteredTargets
}

func (d *Supervisor) executeReconfigBarrier(epoch int, msgs map[string][]byte, targets map[string]reconfigTarget, phase string) (map[string]bool, []string) {
	acked := make(map[string]bool)
	if len(msgs) == 0 {
		return acked, nil
	}

	d.tcpLock.Lock()
	d.reconfigAckEpoch = epoch
	d.reconfigAckPending = make(map[string]struct{}, len(msgs))
	for ip := range msgs {
		d.reconfigAckPending[ip] = struct{}{}
	}
	d.tcpLock.Unlock()

	const reconfigWaves = 3
	const reconfigWaveWait = 1500 * time.Millisecond
	for wave := 1; wave <= reconfigWaves; wave++ {
		pending := d.snapshotPendingReconfigAcks(epoch)
		if len(pending) == 0 {
			break
		}
		for _, ip := range pending {
			networks.TcpDial(msgs[ip], ip)
			target := targets[ip]
			if wave == 1 {
				d.sl.Slog.Printf("[Epoch %d] Sent %s Reconfig to %s: NewShard=%d, NewNode=%d, Monitor=%d, MonitoredBy=%d\n",
					epoch, phase, ip, target.newShardID, target.newNodeID, target.mainTarget, target.supervisedBy)
			} else {
				d.sl.Slog.Printf("[Epoch %d] Retry %s Reconfig wave=%d to %s (NewShard=%d, NewNode=%d)\n",
					epoch, phase, wave, ip, target.newShardID, target.newNodeID)
			}
		}

		// Keep the ACK window after every wave, including the last one. Without
		// this final wait, nodes that ACK just after wave 3 are falsely counted as
		// "did not ACK after 3 waves", which then cascades into fake silent-leader
		// eliminations and unnecessary fallback committees.
		time.Sleep(reconfigWaveWait)
	}

	missing := d.snapshotPendingReconfigAcks(epoch)
	missingSet := make(map[string]struct{}, len(missing))
	for _, ip := range missing {
		missingSet[ip] = struct{}{}
	}
	for ip := range msgs {
		if _, missed := missingSet[ip]; !missed {
			acked[ip] = true
		}
	}

	d.tcpLock.Lock()
	if d.reconfigAckEpoch == epoch {
		d.reconfigAckEpoch = -1
		d.reconfigAckPending = make(map[string]struct{})
	}
	d.tcpLock.Unlock()

	if len(missing) > 0 {
		d.sl.Slog.Printf("[Epoch %d] WARNING: %d node(s) did not ACK %s reconfig after %d waves: %v\n",
			epoch, len(missing), phase, reconfigWaves, missing)
	}

	return acked, missing
}

func buildVirtualRouteTable(routeIPs []string) map[uint64]string {
	routes := make(map[uint64]string, len(routeIPs))
	for idx, ip := range routeIPs {
		if ip == "" {
			continue
		}
		routes[uint64(idx)] = ip
	}
	return routes
}

func (d *Supervisor) takeoverRouteIPsForShards(shardIDs []uint64) []string {
	seen := make(map[string]struct{}, len(shardIDs))
	routeIPs := make([]string, 0, len(shardIDs))
	for _, sid := range shardIDs {
		nodes, ok := d.Ip_nodeTable[sid]
		if !ok || len(nodes) == 0 {
			continue
		}
		leaderIP := ""
		if ip, ok := nodes[0]; ok && ip != "" {
			leaderIP = ip
		} else {
			nodeIDs := make([]int, 0, len(nodes))
			for nid := range nodes {
				nodeIDs = append(nodeIDs, int(nid))
			}
			sort.Ints(nodeIDs)
			leaderIP = nodes[uint64(nodeIDs[0])]
		}
		if leaderIP == "" {
			continue
		}
		if _, ok := seen[leaderIP]; ok {
			continue
		}
		seen[leaderIP] = struct{}{}
		routeIPs = append(routeIPs, leaderIP)
	}
	return routeIPs
}

func (d *Supervisor) broadcastTakeoverRouting(targetShardID uint64, mode string, routeIPs []string) {
	routeMsg := message.TakeoverRoutingMsg{
		TargetShardID: targetShardID,
		Mode:          mode,
		RouteIPs:      routeIPs,
	}
	bytes, _ := json.Marshal(routeMsg)
	msg := message.MergeMessage(message.CTakeoverRouting, bytes)
	numCS := uint64(params.ConsensusShardCount())
	for sid := uint64(0); sid < numCS; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			for _, ip := range nodes {
				go networks.TcpDial(msg, ip)
			}
		}
	}
	d.sl.Slog.Printf("L1: Broadcast takeover routing for Shard %d (mode=%s, routeIPs=%v)\n",
		targetShardID, mode, routeIPs)
}

func (d *Supervisor) activateBSGSupervisionForTarget(targetShardID, sourceShardID uint64) {
	if d.takenOverShards[targetShardID] {
		d.sl.Slog.Printf("L1: Skip BSG supervision handoff for already taken-over target Shard %d\n", targetShardID)
		return
	}

	bsgMembers, ok := d.CurrentBSGMap[targetShardID]
	if !ok || len(bsgMembers) == 0 {
		d.sl.Slog.Printf("L1: No BSG members available for supervision target Shard %d (source shard %d)\n", targetShardID, sourceShardID)
		return
	}

	d.sl.Slog.Printf("L1: BSG takes over supervision of Shard %d from Shard %d. BSG: %v\n",
		targetShardID, sourceShardID, bsgMembers)
	d.bsgSupervisionActive[targetShardID] = bsgMembers

	type SupervisionTakeoverInfo struct {
		TargetShardID uint64
		Mode          string
	}
	info := SupervisionTakeoverInfo{TargetShardID: targetShardID, Mode: "BSG"}
	infoBytes, _ := json.Marshal(info)
	supervisionMsg := message.MergeMessage(message.CTakeoverSupervision, infoBytes)

	for _, memberShardID := range bsgMembers {
		if nodes, ok := d.Ip_nodeTable[memberShardID]; ok {
			for _, ip := range nodes {
				go networks.TcpDial(supervisionMsg, ip)
			}
		}
	}
}

// findSupervisorOf returns the shard ID that is supervising the given target shard
func (d *Supervisor) findSupervisorOf(targetShardID uint64) (uint64, bool) {
	for supervisor, target := range d.CurrentMainMap {
		if target == targetShardID {
			return supervisor, true
		}
	}
	return 0, false
}

// estimateTargetPoolSize estimates the remaining unprocessed tx count for a shard
// based on total injected txs (evenly divided) minus observed processed txs.
func (d *Supervisor) estimateTargetPoolSize(shardID uint64) int {
	d.tcpLock.Lock()
	if poolSize, ok := d.lastShardTxPool[shardID]; ok {
		d.tcpLock.Unlock()
		return poolSize
	}
	d.tcpLock.Unlock()

	// 估算目标分片的待处理池大小：约 1 个区块的交易量
	// 这个估计值不应过大，否则 lNew 长期超过阈值导致接管永远不被批准
	maxBlockSize := params.MaxBlockSize_global
	if maxBlockSize <= 0 {
		maxBlockSize = 2000
	}
	estimated := maxBlockSize
	return estimated
}

func (d *Supervisor) targetPoolEstimateDetail(shardID uint64) (int, string) {
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()
	if poolSize, ok := d.lastShardTxPool[shardID]; ok {
		return poolSize, "latest-block-info"
	}
	maxBlockSize := params.MaxBlockSize_global
	if maxBlockSize <= 0 {
		maxBlockSize = 2000
	}
	return maxBlockSize, "fallback-maxBlockSize"
}

func (d *Supervisor) allObservedShardPoolsDrained() (bool, string) {
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()

	nonEmpty := make([]string, 0)
	numCS := uint64(params.ConsensusShardCount())
	for sid := uint64(0); sid < numCS; sid++ {
		if d.takenOverShards[sid] {
			continue
		}
		poolSize, ok := d.lastShardTxPool[sid]
		if !ok || poolSize <= 0 {
			continue
		}
		nonEmpty = append(nonEmpty, fmt.Sprintf("S%d=%d", sid, poolSize))
	}
	sort.Strings(nonEmpty)
	return len(nonEmpty) == 0, strings.Join(nonEmpty, ",")
}

func (d *Supervisor) handleSupervisionRes(content []byte) bool {
	var msg message.SupervisionResultMsg
	json.Unmarshal(content, &msg)

	if !params.IsConsensusShardForReputation(msg.ReporterShardID, d.ChainConfig.ShardNums) ||
		!params.IsConsensusShardForReputation(msg.TargetShardID, d.ChainConfig.ShardNums) {
		d.sl.Slog.Printf("L1: Ignore supervision result involving non-consensus shard: reporter=%d target=%d\n", msg.ReporterShardID, msg.TargetShardID)
		return false
	}

	d.tcpLock.Lock()
	activeEpoch := d.currentEpoch
	d.tcpLock.Unlock()
	if msg.Epoch != activeEpoch {
		d.sl.Slog.Printf("L1: Ignore stale supervision report (reporter=%d target=%d reportEpoch=%d activeEpoch=%d reason=%s)\n",
			msg.ReporterShardID, msg.TargetShardID, msg.Epoch, activeEpoch, msg.Reason)
		return false
	}

	// reporter==target means an intra-shard malicious-node report (Plan B),
	// not a cross-shard supervision failure that should trigger takeover.
	if msg.ReporterShardID == msg.TargetShardID {
		d.sl.Slog.Printf("L1: Received INTRA-SHARD fault report from Shard %d (Faulty=%v, Reason=%s)\n",
			msg.ReporterShardID, msg.IsFaulty, msg.Reason)

		if !msg.IsFaulty {
			return false
		}

		faultyIP := msg.FaultyNodeIP
		if faultyIP == "" && msg.FaultyNodeID != nil {
			if ip, ok := d.EpochOriginalIpTable[msg.TargetShardID][*msg.FaultyNodeID]; ok {
				faultyIP = ip
			} else if ip, ok := d.Ip_nodeTable[msg.TargetShardID][*msg.FaultyNodeID]; ok {
				faultyIP = ip
			}
		}

		if faultyIP == "" {
			d.sl.Slog.Printf("L1: Intra-shard report missing resolvable faulty node identity (reporter=%d target=%d). Skip E penalty.\n",
				msg.ReporterShardID, msg.TargetShardID)
			return false
		}

		d.recordNodeActionForRound(faultyIP, msg.TargetShardID, msg.Epoch, msg.SeqID, "E")
		d.sl.Slog.Printf("L1: Wrong vote detected: Node %s in Shard %d. Recorded E penalty. Reason: %s\n",
			faultyIP, msg.TargetShardID, msg.Reason)
		return false
	}

	d.sl.Slog.Printf("L1: Received Supervision Report from Shard %d against Shard %d. Faulty: %v\n",
		msg.ReporterShardID, msg.TargetShardID, msg.IsFaulty)

	if msg.IsFaulty {
		if isInitialLeaderSilenceFault(msg.Reason) {
			d.tcpLock.Lock()
			epochStart := d.epochStartTime
			d.tcpLock.Unlock()
			if !epochStart.IsZero() {
				elapsed := time.Since(epochStart)
				grace := initialTakeoverGraceDuration()
				if elapsed < grace {
					d.sl.Slog.Printf("L1: Defer initial-timeout takeover during reconfig warmup (reporter=%d target=%d elapsed=%s grace=%s reason=%s)\n",
						msg.ReporterShardID, msg.TargetShardID, elapsed.Round(time.Second), grace.Round(time.Second), msg.Reason)
					return false
				}
			}
		}

		faultyShardID := msg.TargetShardID
		leaderSilent := isLeaderSilenceFault(msg.Reason)
		lowerReason := strings.ToLower(msg.Reason)

		d.tcpLock.Lock()
		drainingMode := d.drainingMode
		d.tcpLock.Unlock()
		if drainingMode &&
			(strings.Contains(lowerReason, "block generation timeout") || strings.Contains(lowerReason, "fallback timeout")) &&
			!strings.Contains(lowerReason, "initial") {
			d.sl.Slog.Printf("L1: Ignore timeout-based takeover during draining phase (reporter=%d target=%d reason=%s)\n",
				msg.ReporterShardID, faultyShardID, msg.Reason)
			return false
		}

		// 防止同一 Epoch 内对同一分片重复接管（避免级联雪崩）
		d.tcpLock.Lock()
		if d.takenOverShards[faultyShardID] {
			d.tcpLock.Unlock()
			d.sl.Slog.Printf("L1: Shard %d already taken over in this epoch, ignoring duplicate report.\n", faultyShardID)
			return false
		}

		if d.takenOverShards[msg.ReporterShardID] {
			d.tcpLock.Unlock()
			d.sl.Slog.Printf("L1: Reporter shard %d is already taken over in this epoch, ignoring cascading report against shard %d.\n",
				msg.ReporterShardID, faultyShardID)
			return false
		}

		if leaderSilent {
			if origNodes, ok := d.EpochOriginalIpTable[faultyShardID]; ok {
				if leaderIP, ok := origNodes[0]; ok {
					d.leaderSilenceReasons[leaderIP] = msg.Reason
					leaderMetrics := d.ensureNodeMetricsLocked(leaderIP)
					leaderMetrics.Reputation = 0
					leaderMetrics.E++
					d.sl.Slog.Printf("L1: Silent leader detected on Shard %d -> Node %s marked for forced elimination. Reason: %s\n",
						faultyShardID, leaderIP, msg.Reason)
				}
			}
		}

		d.takenOverShards[faultyShardID] = true
		d.markShardTakenOverLocked(faultyShardID, activeEpoch)
		d.tcpLock.Unlock()

		// 按照方案: 分片共识失败 → 参与节点按"未参与本次共识"扣分 (S++)。
		// 目标分片 leader 的沉默由超时接管流程体现，不在此处做 S++。
		originalNodes := d.EpochOriginalIpTable[faultyShardID]
		for nid, ip := range originalNodes {
			if nid == 0 {
				continue
			}
			d.recordNodeAction(ip, "S")
		}

		if msg.ReporterNode == nil {
			d.sl.Slog.Println("L1: Warning: supervision report missing ReporterNode, skip takeover routing.")
			return false
		}

		// Record accepted takeover start in one place for all paths (direct report/fallback/TB failure).
		d.notifyTakeoverStartForMeasure(content)

		// ── 负载决策: L_new = (N_pool_CSl + N_pool_CSm) / C_target ──
		reporterPoolSize := msg.CurrentPoolSize
		targetPoolSize, targetPoolSource := d.targetPoolEstimateDetail(faultyShardID)
		cTarget := params.HealthFactorBase
		lNew := float64(reporterPoolSize+targetPoolSize) / float64(cTarget)
		d.sl.Slog.Printf("L1: Takeover load detail: reporterShard=%d reporterPool=%d, targetShard=%d targetPool=%d(source=%s), cTarget=%d => L=(%d+%d)/%d=%.2f, threshold=%.2f, reason=%s\n",
			msg.ReporterShardID, reporterPoolSize, faultyShardID, targetPoolSize, targetPoolSource,
			cTarget, reporterPoolSize, targetPoolSize, cTarget, lNew, float64(params.TakeoverThreshold), msg.Reason)

		var takeoverShardID uint64
		if lNew <= float64(params.TakeoverThreshold) {
			// CS_l 负载可承受 → CS_l 直接接管 CS_m 的共识任务
			takeoverShardID = msg.ReporterShardID
			d.sl.Slog.Printf("L1: Load OK (L=%.2f ≤ τ=%.2f). CS_l (Shard %d) takes over Shard %d\n",
				lNew, float64(params.TakeoverThreshold), msg.ReporterShardID, faultyShardID)

			takeoverPayload := message.TakeoverMsg{TargetShardID: faultyShardID, Mode: "Granted"}
			takeoverBytes, _ := json.Marshal(takeoverPayload)
			takeoverMsg := message.MergeMessage(message.CTakeover, takeoverBytes)

			reporterIPs, reporterExists := d.Ip_nodeTable[msg.ReporterShardID]
			if reporterExists {
				for _, ip := range reporterIPs {
					go networks.TcpDial(takeoverMsg, ip)
				}
			} else {
				networks.TcpDial(takeoverMsg, msg.ReporterNode.IPaddr)
			}

			d.sl.Slog.Printf("L1: Redirecting traffic from Shard %d to Shard %d\n", faultyShardID, msg.ReporterShardID)
			if reporterExists {
				d.Ip_nodeTable[faultyShardID] = reporterIPs
				routeIPs := d.takeoverRouteIPsForShards([]uint64{msg.ReporterShardID})
				d.broadcastTakeoverRouting(faultyShardID, "Granted", routeIPs)
			}
		} else {
			// CS_l 负载过高 → 由 BSG 进行接管，交易路由到 BSG 各成员分片
			d.sl.Slog.Printf("L1: Load too high (L=%.2f > τ=%.2f). Activating BSG for Shard %d\n",
				lNew, float64(params.TakeoverThreshold), faultyShardID)
			d.activateBSGTakeover(faultyShardID)
			// BSG 接管时，主接管方为 BSG 第一个成员
			if bsgMembers, ok := d.CurrentBSGMap[faultyShardID]; ok && len(bsgMembers) > 0 {
				takeoverShardID = bsgMembers[0]
			}
		}

		d.updateShardReputation(faultyShardID, false)

		// CS_l 因接管而变忙，其原有的监督职责由 BSG 代替，防止监督链断裂
		d.activateBSGSupervision(msg.ReporterShardID)

		// 被接管分片的监督链转移：接管方代替被接管分片继续监督其原目标分片
		d.transferMonitoringChain(faultyShardID, takeoverShardID)

		// 通知所有 CS 分片停止监督被接管的分片，防止继续上报已无意义的故障
		d.broadcastStopMonitoring(faultyShardID)

		// 被接管分片不再出块/发送 BlockInfo，降低 stopThreshold，避免永远达不到结束条件
		d.Ss.ReduceThreshold(3)
		d.sl.Slog.Printf("L1: stopThreshold reduced by 3 (taken-over Shard %d). New threshold=%d\n",
			faultyShardID, d.Ss.GetThreshold())
		return true
	}

	return false
}

// activateBSGTakeover 当 CS_l 负载过高无法接管时，激活 BSG 进行接管。
// BSG 成员共同承担失效分片的共识任务，交易路由到 BSG 成员。
func (d *Supervisor) activateBSGTakeover(faultyShardID uint64) {
	bsgMembers, ok := d.CurrentBSGMap[faultyShardID]
	if !ok || len(bsgMembers) == 0 {
		d.sl.Slog.Printf("L1: No BSG members found for Shard %d, cannot activate BSG takeover\n", faultyShardID)
		return
	}

	d.sl.Slog.Printf("L1: BSG Activated for Shard %d. Members: %v\n", faultyShardID, bsgMembers)

	// 向所有 BSG 成员发送接管指令 (Mode: BSG_Activated)
	takeoverPayload := message.TakeoverMsg{
		TargetShardID: faultyShardID,
		Mode:          "BSG_Activated",
	}
	takeoverBytes, _ := json.Marshal(takeoverPayload)
	takeoverMsg := message.MergeMessage(message.CTakeover, takeoverBytes)

	for _, memberShardID := range bsgMembers {
		if nodes, ok := d.Ip_nodeTable[memberShardID]; ok {
			for _, ip := range nodes {
				networks.TcpDial(takeoverMsg, ip)
			}
		}
	}

	routeIPs := d.takeoverRouteIPsForShards(bsgMembers)
	if len(routeIPs) > 0 {
		d.Ip_nodeTable[faultyShardID] = buildVirtualRouteTable(routeIPs)
		d.broadcastTakeoverRouting(faultyShardID, "BSG_Activated", routeIPs)
	}
	d.sl.Slog.Printf("L1: Traffic for Shard %d routed across BSG leaders %v\n", faultyShardID, routeIPs)
}

// activateBSGSupervision 当 CS_l 因接管变忙或自身失效时，激活 BSG 代替
// CS_l 的监督职责。BSG 成员将通过声誉加权投票决策目标分片的健康状态。
func (d *Supervisor) activateBSGSupervision(busyShardID uint64) {
	// 找到 busyShardID 原本在监督谁
	targetShardID, ok := d.CurrentMainMap[busyShardID]
	if !ok {
		return
	}
	d.activateBSGSupervisionForTarget(targetShardID, busyShardID)
}

// broadcastStopMonitoring 通知所有 CS 分片停止监督 targetShardID。
// 被接管的分片不再独立出块，继续监督它只会产生无意义的超时上报。
func (d *Supervisor) broadcastStopMonitoring(targetShardID uint64) {
	type StopMonitoringInfo struct {
		TargetShardID uint64
	}
	info := StopMonitoringInfo{TargetShardID: targetShardID}
	infoBytes, _ := json.Marshal(info)
	stopMsg := message.MergeMessage(message.CStopMonitoring, infoBytes)

	numCS := uint64(params.ConsensusShardCount())
	for sid := uint64(0); sid < numCS; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			for _, ip := range nodes {
				go networks.TcpDial(stopMsg, ip)
			}
		}
	}
	d.sl.Slog.Printf("L1: Broadcast CStopMonitoring for Shard %d to all CS shards\n", targetShardID)
}

// transferMonitoringChain 当 faultyShardID 被接管时，将其原本负责监督的目标分片
// 的监督职责转移给 takeoverShardID，防止监督链断裂（孤儿目标分片无人监督）。
func (d *Supervisor) transferMonitoringChain(faultyShardID, takeoverShardID uint64) {
	orphanedTargetID, ok := d.CurrentMainMap[faultyShardID]
	if !ok {
		return
	}
	if d.takenOverShards[orphanedTargetID] {
		return
	}
	if takeoverShardID == orphanedTargetID || takeoverShardID == 0 {
		return
	}

	d.sl.Slog.Printf("L1: Monitoring chain transfer: Shard %d (formerly supervised by taken-over Shard %d) -> now monitored by BSG of target, not direct shard %d\n",
		orphanedTargetID, faultyShardID, takeoverShardID)
	d.activateBSGSupervisionForTarget(orphanedTargetID, faultyShardID)
}

func (d *Supervisor) updateShardReputation(shardID uint64, success bool) {
	// 使用 EpochOriginalIpTable 避免引用被 BSG 覆盖后的 IP
	nodes := d.EpochOriginalIpTable[shardID]
	for nodeID, nodeIP := range nodes {
		// 模拟：在内存中找到节点对象并更新
		// 注意：Supervisor 维护的节点列表可能需要从 d.ChainConfig 或其他地方获取
		// 这里简化为打印日志，实际需维护一个 map[string]*shard.Node 的全网节点表
		status := "Penalty"
		if success {
			status = "Reward"
		}
		d.sl.Slog.Printf("Reputation Update for Node %d (IP: %s) in Shard %d: %s\n", nodeID, nodeIP, shardID, status)

	}
}

func (d *Supervisor) generateSupervisionTopology(epoch int, randomSeed int64) SupervisionTopology {
	// Only CS shards participate in the supervision ring; SS shards don't
	// produce blocks so monitoring them is meaningless.
	numCS := params.ConsensusShardCount()
	shards := make([]uint64, numCS)
	for i := 0; i < numCS; i++ {
		shards[i] = uint64(i)
	}

	// 1. Shuffle (随机洗牌)
	r := rand.New(rand.NewSource(randomSeed))
	r.Shuffle(numCS, func(i, j int) {
		shards[i], shards[j] = shards[j], shards[i]
	})

	mainMap := make(map[uint64]uint64)
	bsgMap := make(map[uint64][]uint64)

	// 辅助映射：快速查找每个分片的主监督者
	targetToMain := make(map[uint64]uint64)

	// 2. 构建主监督关系 (环形: Shard[i] -> Shard[i+1])
	for i := 0; i < numCS; i++ {
		supervisor := shards[i]
		target := shards[(i+1)%numCS]

		mainMap[supervisor] = target
		targetToMain[target] = supervisor

		d.sl.Slog.Printf("[Epoch %d] Main Relation: Shard %d -> Shard %d\n", epoch, supervisor, target)
	}

	const BSGSize = 3

	for target, mainSupervisor := range targetToMain { // target 即 CS_f
		bsg := make([]uint64, 0)

		// (A) 添加强制成员: CS_l 的主监督 (Grandparent)
		// 如果 A->B->C (C是target), 则 main=B, grand=A
		grandSupervisor := targetToMain[mainSupervisor]
		bsg = append(bsg, grandSupervisor)

		// (B) 填充剩余成员 (随机且互不相关)
		// 候选池: 所有分片 - {Target, Main, Grand}
		candidates := make([]uint64, 0)
		for _, s := range shards {
			if s != target && s != mainSupervisor && s != grandSupervisor {
				candidates = append(candidates, s)
			}
		}

		// 打乱候选池
		r.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})

		// 填充满 BSGSize
		needed := BSGSize - 1 // 减去已经加入的 Grandparent
		for k := 0; k < needed && k < len(candidates); k++ {
			bsg = append(bsg, candidates[k])
		}

		bsgMap[target] = bsg
		d.sl.Slog.Printf("[Epoch %d] BSG for Shard %d: %v (Grandparent: %d)\n",
			epoch, target, bsg, grandSupervisor)
	}

	return SupervisionTopology{
		MainMap: mainMap,
		BSGMap:  bsgMap,
	}
}

// sendInitialSupervisionAssignments sends CSupervisionAssign messages to all nodes
// at epoch 0 so they know their monitoring targets and BSG membership.
func (d *Supervisor) sendInitialSupervisionAssignments(topo SupervisionTopology) {
	// Build reverse map: for each shard, which shards are supervising it
	supervisedByMap := make(map[uint64]uint64) // target -> supervisor
	for supervisor, target := range topo.MainMap {
		supervisedByMap[target] = supervisor
	}

	// Build backup targets per shard from BSG map
	nodeToBackupTargets := make(map[uint64][]uint64)
	for target, members := range topo.BSGMap {
		for _, member := range members {
			nodeToBackupTargets[member] = append(nodeToBackupTargets[member], target)
		}
	}

	numCS := uint64(params.ConsensusShardCount())
	for sid := uint64(0); sid < numCS; sid++ {
		mainTarget := topo.MainMap[sid]
		supervisedBy := supervisedByMap[sid]
		backupTargets := nodeToBackupTargets[sid]

		assignMsg := message.SupervisionAssignMsg{
			MainTarget:    mainTarget,
			BackupTargets: backupTargets,
			FullBSGMap:    topo.BSGMap,
			SupervisedBy:  supervisedBy,
		}
		bytes, _ := json.Marshal(assignMsg)
		msg := message.MergeMessage(message.CSupervisionAssign, bytes)

		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			if ip, ok := d.Ip_nodeTable[sid][nid]; ok {
				networks.TcpDial(msg, ip)
			}
		}
		d.sl.Slog.Printf("[Epoch 0] Supervision assigned to Shard %d: Monitor Shard %d, Monitored by Shard %d\n",
			sid, mainTarget, supervisedBy)
	}
}

// 执行 Epoch 重配置: 基于声誉淘汰 + VRF 跨分片重分配
func (d *Supervisor) EpochReconfiguration(epoch int) {
	d.sl.Slog.Printf("=== Starting Epoch Reconfiguration for Epoch %d ===\n", epoch)
	baselineThreshold := 3 * params.ConsensusShardCount()
	d.Ss.ResetAll(baselineThreshold)
	d.sl.Slog.Printf("[Epoch %d] stopThreshold reset to baseline=%d, stopGap reset to 0\n", epoch, baselineThreshold)
	epochActionSnapshot := d.snapshotEpochActions()

	// ─── 第一步: 计算本 Epoch 各节点声誉值 ───
	d.calculateEpochReputation()

	// ─── 第二步: 声誉淘汰 ───
	// 仅对 CS 分片中的节点进行声誉评估与淘汰；SS 分片的节点保持不变
	activeCSNodeIPs := make([]string, 0)
	eliminatedNodes := make([]string, 0)
	numCS := uint64(params.ConsensusShardCount())

	d.tcpLock.Lock()
	for sid := uint64(0); sid < numCS; sid++ {
		// 使用 EpochOriginalIpTable 收集节点,避免 BSG 覆盖后丢失原始成员
		origNodes := d.EpochOriginalIpTable[sid]
		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			if ip, ok := origNodes[nid]; ok {
				metrics, exists := d.NodeMetricsMap[ip]
				if !exists {
					metrics = &NodeMetrics{Reputation: 0.5, EliminatedAtEpoch: -1, LastTakenOverEpoch: -1, HasServedInCS: true}
					d.NodeMetricsMap[ip] = metrics
				}

				pCount, sCount, eCount := metrics.P, metrics.S, metrics.E
				if snap, ok := epochActionSnapshot[ip]; ok {
					pCount, sCount, eCount = snap[0], snap[1], snap[2]
				}

				if reason, forced := d.leaderSilenceReasons[ip]; forced {
					eliminatedNodes = append(eliminatedNodes, ip)
					d.sl.Slog.Printf("[Epoch %d] ELIMINATED (silent leader): Node %s (Reason=%s, Rep=%.4f, P:%d S:%d E:%d)\n",
						epoch, ip, reason, metrics.Reputation, pCount, sCount, eCount)
					continue
				}

				// 声誉低于阈值(0.6)的节点被驱逐到候补节点池
				if metrics.Reputation < params.ReputationThreshold {
					eliminatedNodes = append(eliminatedNodes, ip)
					d.sl.Slog.Printf("[Epoch %d] ELIMINATED: Node %s (Rep=%.4f < Threshold=%.2f, P:%d S:%d E:%d)\n",
						epoch, ip, metrics.Reputation, params.ReputationThreshold, pCount, sCount, eCount)
				} else {
					metrics.HasServedInCS = true
					activeCSNodeIPs = append(activeCSNodeIPs, ip)
				}
			}
		}
	}
	// 静默 leader 标记只在当前 Epoch 生效，完成本轮驱逐后清空。
	d.leaderSilenceReasons = make(map[string]string)
	d.tcpLock.Unlock()

	d.sl.Slog.Printf("[Epoch %d] Active CS nodes: %d, Eliminated: %d, CandidatePool before: %d\n",
		epoch, len(activeCSNodeIPs), len(eliminatedNodes), len(d.CandidatePool))

	// 去重: 接管流量重定向后同一IP可能出现在多个分片下
	seenIPs := make(map[string]bool)
	dedupActive := make([]string, 0, len(activeCSNodeIPs))
	for _, ip := range activeCSNodeIPs {
		if !seenIPs[ip] {
			seenIPs[ip] = true
			dedupActive = append(dedupActive, ip)
		}
	}
	activeCSNodeIPs = dedupActive
	dedupElim := make([]string, 0, len(eliminatedNodes))
	for _, ip := range eliminatedNodes {
		if !seenIPs[ip] {
			seenIPs[ip] = true
			dedupElim = append(dedupElim, ip)
		}
	}
	eliminatedNodes = dedupElim

	// ─── 第三步: 候补节点池机制 ───
	epochRandomness := time.Now().UnixNano()
	activeSet := make(map[string]bool)
	for _, ip := range activeCSNodeIPs {
		activeSet[ip] = true
	}

	// 被驱逐节点进入候补池，并可在下一 Epoch 作为常规候补参与启用。
	d.tcpLock.Lock()
	for _, ip := range eliminatedNodes {
		if metrics, exists := d.NodeMetricsMap[ip]; exists {
			metrics.Reputation = 0.5
			metrics.P = 0
			metrics.S = 0
			metrics.E = 0
			metrics.EliminatedAtEpoch = epoch
		}
		delete(activeSet, ip)
	}

	d.ensureCandidatePoolCapacityLocked(len(d.CandidatePool), activeSet)

	poolSeen := make(map[string]struct{}, len(d.CandidatePool))
	for _, ip := range d.CandidatePool {
		poolSeen[ip] = struct{}{}
	}
	for _, ip := range eliminatedNodes {
		if activeSet[ip] {
			continue
		}
		if _, exists := poolSeen[ip]; exists {
			continue
		}
		d.CandidatePool = append(d.CandidatePool, ip)
		poolSeen[ip] = struct{}{}
	}

	// 候补池按冷却状态拆分。优先使用 ready，只有严重缺口才使用 cooling。
	readyToRejoin := make([]string, 0, len(d.CandidatePool))
	stillCooling := make([]string, 0, len(d.CandidatePool))
	for _, ip := range d.CandidatePool {
		if activeSet[ip] {
			continue
		}
		if m, exists := d.NodeMetricsMap[ip]; exists && m.EliminatedAtEpoch >= epoch {
			stillCooling = append(stillCooling, ip)
		} else {
			readyToRejoin = append(readyToRejoin, ip)
		}
	}

	sort.Slice(readyToRejoin, func(i, j int) bool {
		ri, rj := 0.5, 0.5
		ei, ej := -1, -1
		si, sj := false, false
		if m, exists := d.NodeMetricsMap[readyToRejoin[i]]; exists {
			ri = m.Reputation
			ei = m.EliminatedAtEpoch
			si = m.HasServedInCS
		}
		if m, exists := d.NodeMetricsMap[readyToRejoin[j]]; exists {
			rj = m.Reputation
			ej = m.EliminatedAtEpoch
			sj = m.HasServedInCS
		}
		if si != sj {
			return si
		}
		if ri == rj {
			if ei == ej {
				return readyToRejoin[i] < readyToRejoin[j]
			}
			return ei < ej
		}
		return ri > rj
	})

	desiredNodesPerShard := int(params.NodesInShard)
	if desiredNodesPerShard <= 0 {
		desiredNodesPerShard = int(d.ChainConfig.Nodes_perShard)
	}
	minNodesNeeded := int(numCS) * desiredNodesPerShard
	const candidateBackfillEnabled = false

	if len(activeCSNodeIPs) < minNodesNeeded {
		needed := minNodesNeeded - len(activeCSNodeIPs)
		if candidateBackfillEnabled {
			// no-op in debug-disabled mode; backfill remains intentionally off
		} else {
			d.sl.Slog.Printf("[Epoch %d] Candidate backfill disabled for debugging: shortage=%d, readyToRejoin=%d, stillCooling=%d\n",
				epoch, needed, len(readyToRejoin), len(stillCooling))
		}
	}

	if len(activeCSNodeIPs) > minNodesNeeded {
		sort.Slice(activeCSNodeIPs, func(i, j int) bool {
			ri := 0.5
			rj := 0.5
			if m, ok := d.NodeMetricsMap[activeCSNodeIPs[i]]; ok {
				ri = m.Reputation
			}
			if m, ok := d.NodeMetricsMap[activeCSNodeIPs[j]]; ok {
				rj = m.Reputation
			}
			if ri == rj {
				return activeCSNodeIPs[i] < activeCSNodeIPs[j]
			}
			return ri > rj
		})
		activeCSNodeIPs = activeCSNodeIPs[:minNodesNeeded]
		activeSet = make(map[string]bool)
		for _, ip := range activeCSNodeIPs {
			activeSet[ip] = true
		}
	}

	// 从候补池移除已晋升节点，再补齐预留数量。
	newPool := make([]string, 0, len(d.CandidatePool))
	for _, ip := range d.CandidatePool {
		if !activeSet[ip] {
			newPool = append(newPool, ip)
		}
	}
	d.CandidatePool = newPool

	readyCount := len(readyToRejoin)
	coolingCount := len(stillCooling)
	candidateAfter := len(d.CandidatePool)
	d.tcpLock.Unlock()

	d.sl.Slog.Printf("[Epoch %d] CandidatePool: total=%d, readyToRejoin=%d, stillCooling=%d\n",
		epoch, candidateAfter, readyCount, coolingCount)
	d.sl.Slog.Printf("[Epoch %d] Final active CS nodes: %d, CandidatePool after: %d\n",
		epoch, len(activeCSNodeIPs), candidateAfter)

	// ─── 第四步: 先做 provisional 重配置，收 ACK 之后再最终定 leader / 激活新 Epoch ───
	previousCSIPTable := make(map[uint64]map[uint64]string, numCS)
	d.tcpLock.Lock()
	for sid := uint64(0); sid < numCS; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			previousCSIPTable[sid] = cloneShardIPTable(nodes)
		} else {
			previousCSIPTable[sid] = make(map[uint64]string)
		}
	}
	d.tcpLock.Unlock()

	provisionalCSIPTable := utils.VRFShardAssignment(activeCSNodeIPs, epochRandomness, int(numCS))
	provisionalNewIPTable := make(map[uint64]map[uint64]string)
	for sid := uint64(0); sid < numCS; sid++ {
		if nodes, ok := provisionalCSIPTable[sid]; ok {
			provisionalNewIPTable[sid] = cloneShardIPTable(nodes)
		} else {
			provisionalNewIPTable[sid] = make(map[uint64]string)
		}
	}
	d.tcpLock.Lock()
	for sid := numCS; sid < d.ChainConfig.ShardNums; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			provisionalNewIPTable[sid] = cloneShardIPTable(nodes)
		}
	}
	d.tcpLock.Unlock()
	provisionalNewIPTable[params.SupervisorShard] = map[uint64]string{0: params.SupervisorAddr}

	// ─── 第五步: 生成新的监督拓扑（按 shard 级别，与具体 leader 选择解耦） ───
	topology := d.generateSupervisionTopology(epoch, epochRandomness)

	// Provisional barrier: 所有候选 CS 节点先同步到目标分片，但不立即激活共识。
	provisionalMsgs, provisionalTargets := buildCSReconfigMessages(epoch, provisionalNewIPTable, numCS, topology, false)
	ackedProvisional, _ := d.executeReconfigBarrier(epoch, provisionalMsgs, provisionalTargets, "Provisional")

	quorum := pbftActivationQuorum(desiredNodesPerShard)
	repairedCSIPTable := make(map[uint64]map[uint64]string, numCS)
	for sid := uint64(0); sid < numCS; sid++ {
		repairedCSIPTable[sid] = cloneShardIPTable(provisionalNewIPTable[sid])
	}

	repairCandidates := make([]string, 0, len(readyToRejoin)+len(stillCooling)+len(eliminatedNodes))
	if candidateBackfillEnabled {
		repairCandidates = append(repairCandidates, readyToRejoin...)
		repairCandidates = append(repairCandidates, stillCooling...)
		repairCandidates = append(repairCandidates, eliminatedNodes...)
	}
	reservedRepairIPs := make(map[string]struct{}, len(provisionalMsgs))
	for ip := range provisionalMsgs {
		reservedRepairIPs[ip] = struct{}{}
	}
	repairAcked := make(map[string]bool)
	repairCursor := 0

	for repairRound := 1; repairRound <= 2; repairRound++ {
		repairSelection := make(map[string]struct{})
		for sid := uint64(0); sid < numCS; sid++ {
			readyCount := 0
			vacantNodeIDs := make([]uint64, 0, desiredNodesPerShard)
			for nid := uint64(0); nid < uint64(desiredNodesPerShard); nid++ {
				ip, ok := repairedCSIPTable[sid][nid]
				if ok && (ackedProvisional[ip] || repairAcked[ip]) {
					readyCount++
					continue
				}
				vacantNodeIDs = append(vacantNodeIDs, nid)
			}
			for readyCount < quorum && len(vacantNodeIDs) > 0 {
				assigned := false
				for repairCursor < len(repairCandidates) {
					candidate := repairCandidates[repairCursor]
					repairCursor++
					if candidate == "" {
						continue
					}
					if _, used := reservedRepairIPs[candidate]; used {
						continue
					}
					if !isEndpointReachable(candidate, 200*time.Millisecond) {
						d.sl.Slog.Printf("[Epoch %d] SKIP (repair candidate offline): Node %s\n", epoch, candidate)
						continue
					}
					nid := vacantNodeIDs[0]
					vacantNodeIDs = vacantNodeIDs[1:]
					repairedCSIPTable[sid][nid] = candidate
					reservedRepairIPs[candidate] = struct{}{}
					repairSelection[candidate] = struct{}{}
					readyCount++
					assigned = true
					d.sl.Slog.Printf("[Epoch %d] REPAIR ASSIGNED: Node %s -> Shard %d Node %d (round=%d)\n",
						epoch, candidate, sid, nid, repairRound)
					break
				}
				if !assigned {
					break
				}
			}
		}
		if len(repairSelection) == 0 {
			break
		}

		repairedNewIPTable := copyIPTable(provisionalNewIPTable)
		for sid := uint64(0); sid < numCS; sid++ {
			repairedNewIPTable[sid] = cloneShardIPTable(repairedCSIPTable[sid])
		}
		repairMsgsAll, repairTargetsAll := buildCSReconfigMessages(epoch, repairedNewIPTable, numCS, topology, false)
		repairMsgs, repairTargets := filterReconfigMessages(repairMsgsAll, repairTargetsAll, repairSelection)
		ackedThisRound, _ := d.executeReconfigBarrier(epoch, repairMsgs, repairTargets, fmt.Sprintf("Repair-%d", repairRound))
		if len(ackedThisRound) == 0 {
			break
		}
		for ip := range ackedThisRound {
			repairAcked[ip] = true
		}
	}

	// ─── 第六步: 基于 ACK 成功的最终成员重排 leader，并生成真正生效的新 IP 表 ───
	finalCSIPTable := make(map[uint64]map[uint64]string, numCS)
	finalActiveCount := 0
	for sid := uint64(0); sid < numCS; sid++ {
		ranked := make([]rankedNode, 0, len(repairedCSIPTable[sid]))
		for _, ip := range repairedCSIPTable[sid] {
			if !(ackedProvisional[ip] || repairAcked[ip]) {
				continue
			}
			rep := 0.5
			if m, ok := d.NodeMetricsMap[ip]; ok {
				rep = m.Reputation
			}
			ranked = append(ranked, rankedNode{IP: ip, Rep: rep})
		}
		sort.Slice(ranked, func(i, j int) bool {
			if ranked[i].Rep == ranked[j].Rep {
				return ranked[i].IP < ranked[j].IP
			}
			return ranked[i].Rep > ranked[j].Rep
		})

		reordered := make(map[uint64]string)
		if len(ranked) > 0 {
			avoidFreshRepair := make(map[string]struct{})
			for ip := range repairAcked {
				avoidFreshRepair[ip] = struct{}{}
			}
			leaderIdx := d.chooseLeaderWithRotationAvoiding(sid, ranked, avoidFreshRepair)
			if leaderIdx < 0 {
				leaderIdx = 0
			}
			selectedLeader := ranked[leaderIdx]
			prevLeader := d.LastEpochLeaders[sid]
			reordered[0] = selectedLeader.IP
			idx := uint64(1)
			for i, pair := range ranked {
				if i == leaderIdx {
					continue
				}
				reordered[idx] = pair.IP
				idx++
			}
			d.LastEpochLeaders[sid] = selectedLeader.IP
			if prevLeader != "" && prevLeader != selectedLeader.IP {
				d.sl.Slog.Printf("[Epoch %d] Shard %d leader rotated after ACK barrier: %s -> %s (Rep=%.4f)\n",
					epoch, sid, prevLeader, selectedLeader.IP, selectedLeader.Rep)
			} else {
				d.sl.Slog.Printf("[Epoch %d] Shard %d leader selected after ACK barrier: %s (Rep=%.4f)\n",
					epoch, sid, selectedLeader.IP, selectedLeader.Rep)
			}
		}
		if len(reordered) < quorum {
			previousNodes := cloneShardIPTable(previousCSIPTable[sid])
			if len(previousNodes) > 0 {
				finalCSIPTable[sid] = previousNodes
				finalActiveCount += len(previousNodes)
				if leaderIP, ok := previousNodes[0]; ok && leaderIP != "" {
					d.LastEpochLeaders[sid] = leaderIP
				}
				if len(previousNodes) >= quorum {
					d.sl.Slog.Printf("[Epoch %d] Shard %d failed ACK quorum after repair; keep previous committee for this epoch (size=%d, required=%d)\n",
						epoch, sid, len(previousNodes), quorum)
				} else {
					d.sl.Slog.Printf("[Epoch %d] Shard %d cannot form a safe committee after repair; defer reconfiguration and keep previous committee to avoid activating an undersized shard (new=%d, previous=%d, required=%d)\n",
						epoch, sid, len(reordered), len(previousNodes), quorum)
				}
				continue
			}
			d.sl.Slog.Printf("[Epoch %d] WARNING: Shard %d finalized with sub-quorum committee size=%d (required=%d); no previous committee is available to keep this shard stable\n",
				epoch, sid, len(reordered), quorum)
		}
		finalCSIPTable[sid] = reordered
		finalActiveCount += len(reordered)
	}

	finalNewIPTable := make(map[uint64]map[uint64]string)
	for sid := uint64(0); sid < numCS; sid++ {
		finalNewIPTable[sid] = cloneShardIPTable(finalCSIPTable[sid])
	}
	d.tcpLock.Lock()
	for sid := numCS; sid < d.ChainConfig.ShardNums; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			finalNewIPTable[sid] = cloneShardIPTable(nodes)
		}
	}
	d.tcpLock.Unlock()
	finalNewIPTable[params.SupervisorShard] = map[uint64]string{0: params.SupervisorAddr}

	// Some shards fall back to the previous committee after ACK repair. Those
	// restored members may never have seen a provisional sync for the final
	// membership, so activating them directly can create a nominal 4-node shard
	// with only 0-2 nodes actually ready. Run one extra provisional sync on the
	// final table, then compress each shard to the members that really ACKed.
	finalSyncMsgs, finalSyncTargets := buildCSReconfigMessages(epoch, finalNewIPTable, numCS, topology, false)
	ackedFinalSync, _ := d.executeReconfigBarrier(epoch, finalSyncMsgs, finalSyncTargets, "FinalSync")

	// Fix the problem at the source: if FinalSync still leaves a shard below the
	// safe quorum, try to backfill the missing seats with fresh candidates and
	// rerun one focused provisional sync before deciding whether to activate it.
	finalRepairCSIPTable := make(map[uint64]map[uint64]string, numCS)
	reservedFinalIPs := make(map[string]struct{})
	for sid := uint64(0); sid < numCS; sid++ {
		finalRepairCSIPTable[sid] = cloneShardIPTable(finalCSIPTable[sid])
		for _, ip := range finalCSIPTable[sid] {
			if ip == "" {
				continue
			}
			reservedFinalIPs[ip] = struct{}{}
		}
	}
	finalRepairSelection := make(map[string]struct{})
	finalRepairCursor := 0
	for sid := uint64(0); sid < numCS; sid++ {
		readyCount := 0
		vacantNodeIDs := make([]uint64, 0, desiredNodesPerShard)
		for nid := uint64(0); nid < uint64(desiredNodesPerShard); nid++ {
			ip, ok := finalRepairCSIPTable[sid][nid]
			if ok && ackedFinalSync[ip] {
				readyCount++
				continue
			}
			vacantNodeIDs = append(vacantNodeIDs, nid)
		}
		for readyCount < quorum && len(vacantNodeIDs) > 0 {
			assigned := false
			for finalRepairCursor < len(repairCandidates) {
				candidate := repairCandidates[finalRepairCursor]
				finalRepairCursor++
				if candidate == "" {
					continue
				}
				if _, used := reservedFinalIPs[candidate]; used {
					continue
				}
				if !isEndpointReachable(candidate, 200*time.Millisecond) {
					d.sl.Slog.Printf("[Epoch %d] SKIP (final-repair candidate offline): Node %s\n", epoch, candidate)
					continue
				}
				nid := vacantNodeIDs[0]
				vacantNodeIDs = vacantNodeIDs[1:]
				finalRepairCSIPTable[sid][nid] = candidate
				reservedFinalIPs[candidate] = struct{}{}
				finalRepairSelection[candidate] = struct{}{}
				readyCount++
				assigned = true
				d.sl.Slog.Printf("[Epoch %d] FINALSYNC REPAIR ASSIGNED: Node %s -> Shard %d Node %d\n",
					epoch, candidate, sid, nid)
				break
			}
			if !assigned {
				break
			}
		}
	}
	if len(finalRepairSelection) > 0 {
		finalRepairNewIPTable := make(map[uint64]map[uint64]string)
		for sid := uint64(0); sid < numCS; sid++ {
			finalRepairNewIPTable[sid] = cloneShardIPTable(finalRepairCSIPTable[sid])
		}
		d.tcpLock.Lock()
		for sid := numCS; sid < d.ChainConfig.ShardNums; sid++ {
			if nodes, ok := d.Ip_nodeTable[sid]; ok {
				finalRepairNewIPTable[sid] = cloneShardIPTable(nodes)
			}
		}
		d.tcpLock.Unlock()
		finalRepairNewIPTable[params.SupervisorShard] = map[uint64]string{0: params.SupervisorAddr}
		finalRepairMsgsAll, finalRepairTargetsAll := buildCSReconfigMessages(epoch, finalRepairNewIPTable, numCS, topology, false)
		finalRepairMsgs, finalRepairTargets := filterReconfigMessages(finalRepairMsgsAll, finalRepairTargetsAll, finalRepairSelection)
		ackedFinalRepair, _ := d.executeReconfigBarrier(epoch, finalRepairMsgs, finalRepairTargets, "FinalSyncRepair")
		for ip := range ackedFinalRepair {
			ackedFinalSync[ip] = true
		}
		finalCSIPTable = finalRepairCSIPTable
	}

	activatedCSIPTable := make(map[uint64]map[uint64]string, numCS)
	finalActiveCount = 0
	for sid := uint64(0); sid < numCS; sid++ {
		orderedNodeIDs := make([]int, 0, len(finalCSIPTable[sid]))
		for nid := range finalCSIPTable[sid] {
			orderedNodeIDs = append(orderedNodeIDs, int(nid))
		}
		sort.Ints(orderedNodeIDs)

		readyNodes := make(map[uint64]string)
		nextID := uint64(0)
		oldLeader := ""
		if leaderIP, ok := finalCSIPTable[sid][0]; ok {
			oldLeader = leaderIP
		}
		for _, nid := range orderedNodeIDs {
			ip := finalCSIPTable[sid][uint64(nid)]
			if !ackedFinalSync[ip] {
				continue
			}
			readyNodes[nextID] = ip
			nextID++
		}

		if len(readyNodes) < quorum {
			previousNodes := cloneShardIPTable(previousCSIPTable[sid])
			if len(previousNodes) > 0 {
				preferredPrevious := make(map[string]bool)
				for _, ip := range previousNodes {
					if ackedFinalSync[ip] {
						preferredPrevious[ip] = true
					}
				}
				activatedCSIPTable[sid] = d.reorderCommitteeForActivation(sid, previousNodes, preferredPrevious, finalRepairSelection)
				finalActiveCount += len(previousNodes)
				if len(readyNodes) == 0 {
					d.sl.Slog.Printf("[Epoch %d] WARNING: Shard %d received zero FinalSync ACKs; revert to previous committee instead of activating an unready shard (previous=%d, required=%d)\n",
						epoch, sid, len(previousNodes), quorum)
				} else {
					d.sl.Slog.Printf("[Epoch %d] WARNING: Shard %d has insufficient FinalSync ACKs after pruning (ready=%d/%d, required=%d); revert to previous committee for stability\n",
						epoch, sid, len(readyNodes), len(finalCSIPTable[sid]), quorum)
				}
				continue
			}

			if len(readyNodes) == 0 {
				activatedCSIPTable[sid] = cloneShardIPTable(finalCSIPTable[sid])
				finalActiveCount += len(finalCSIPTable[sid])
				d.sl.Slog.Printf("[Epoch %d] WARNING: Shard %d received zero FinalSync ACKs and has no previous committee snapshot; keep tentative committee as last resort (size=%d)\n",
					epoch, sid, len(finalCSIPTable[sid]))
				continue
			}

			preferredReady := make(map[string]bool, len(readyNodes))
			for _, ip := range readyNodes {
				preferredReady[ip] = true
			}
			activatedCSIPTable[sid] = d.reorderCommitteeForActivation(sid, readyNodes, preferredReady, finalRepairSelection)
			finalActiveCount += len(readyNodes)
			newLeader := activatedCSIPTable[sid][0]
			d.sl.Slog.Printf("[Epoch %d] WARNING: Shard %d finalized below safe quorum after FinalSync (ready=%d/%d, required=%d); activate reduced committee as last resort, leader %s -> %s\n",
				epoch, sid, len(readyNodes), len(finalCSIPTable[sid]), quorum, oldLeader, newLeader)
			continue
		}

		preferredReady := make(map[string]bool, len(readyNodes))
		for _, ip := range readyNodes {
			preferredReady[ip] = true
		}
		activatedCSIPTable[sid] = d.reorderCommitteeForActivation(sid, readyNodes, preferredReady, finalRepairSelection)
		finalActiveCount += len(readyNodes)
		newLeader := activatedCSIPTable[sid][0]
		if len(readyNodes) != len(finalCSIPTable[sid]) || (oldLeader != "" && newLeader != oldLeader) {
			d.sl.Slog.Printf("[Epoch %d] Shard %d pruned final committee after FinalSync ACKs: ready=%d/%d, leader %s -> %s\n",
				epoch, sid, len(readyNodes), len(finalCSIPTable[sid]), oldLeader, newLeader)
		}
	}
	finalCSIPTable = activatedCSIPTable

	finalNewIPTable = make(map[uint64]map[uint64]string)
	for sid := uint64(0); sid < numCS; sid++ {
		finalNewIPTable[sid] = cloneShardIPTable(finalCSIPTable[sid])
	}
	d.tcpLock.Lock()
	for sid := numCS; sid < d.ChainConfig.ShardNums; sid++ {
		if nodes, ok := d.Ip_nodeTable[sid]; ok {
			finalNewIPTable[sid] = cloneShardIPTable(nodes)
		}
	}
	d.tcpLock.Unlock()
	finalNewIPTable[params.SupervisorShard] = map[uint64]string{0: params.SupervisorAddr}

	d.sl.Slog.Printf("[Epoch %d] Finalized shard assignment after ACK barrier:\n", epoch)
	for sid, nodes := range finalNewIPTable {
		if sid == params.SupervisorShard {
			continue
		}
		d.sl.Slog.Printf("  Shard %d: %d nodes\n", sid, len(nodes))
		for nid, ip := range nodes {
			d.sl.Slog.Printf("    Node %d -> %s\n", nid, ip)
		}
	}
	d.sl.Slog.Printf("[Epoch %d] Final active CS nodes after ACK barrier: %d\n", epoch, finalActiveCount)

	// ─── 第七步: 更新 Supervisor 内部状态为最终结果 ───
	d.tcpLock.Lock()
	d.CurEpochSeed = epochRandomness
	d.CurrentBSGMap = topology.BSGMap
	d.CurrentMainMap = topology.MainMap
	d.takenOverShards = make(map[uint64]bool)
	d.bsgSupervisionActive = make(map[uint64][]uint64)
	d.Ip_nodeTable = finalNewIPTable
	params.IPmap_nodeTable = finalNewIPTable
	nodesPerShard := uint64(params.NodesInShard)
	if nodesPerShard == 0 {
		maxShardSize := 1
		for sid := uint64(0); sid < numCS; sid++ {
			if len(finalCSIPTable[sid]) > maxShardSize {
				maxShardSize = len(finalCSIPTable[sid])
			}
		}
		nodesPerShard = uint64(maxShardSize)
	}
	d.ChainConfig.Nodes_perShard = nodesPerShard
	filteredPool := make([]string, 0, len(d.CandidatePool))
	assignedFinal := make(map[string]struct{})
	for sid := uint64(0); sid < numCS; sid++ {
		for _, ip := range finalCSIPTable[sid] {
			assignedFinal[ip] = struct{}{}
		}
	}
	for _, ip := range d.CandidatePool {
		if _, used := assignedFinal[ip]; used {
			continue
		}
		filteredPool = append(filteredPool, ip)
	}
	d.CandidatePool = filteredPool
	d.tcpLock.Unlock()

	// 更新 Epoch 原始快照，用于下一轮惩罚定位
	d.snapshotOriginalIpTable()

	assignedIPs := make(map[string]bool)
	for sid, nodes := range finalNewIPTable {
		if sid == params.SupervisorShard {
			continue
		}
		for _, ip := range nodes {
			assignedIPs[ip] = true
		}
	}
	for _, ip := range eliminatedNodes {
		if assignedIPs[ip] {
			continue
		}
		rep := 0.0
		d.tcpLock.Lock()
		if m, ok := d.NodeMetricsMap[ip]; ok {
			rep = m.Reputation
		}
		d.tcpLock.Unlock()
		notice := message.EliminateNoticeMsg{
			Epoch:      epoch,
			Reason:     "Removed from active consensus set after epoch settlement",
			Reputation: rep,
		}
		noticeBytes, _ := json.Marshal(notice)
		networks.TcpDial(message.MergeMessage(message.CEliminateNotice, noticeBytes), ip)
		d.sl.Slog.Printf("[Epoch %d] Sent elimination notice to %s (Rep=%.4f)\n", epoch, ip, rep)
	}

	// Open the Supervisor-side epoch gate before Final activate so early blocks
	// from fast shards are accounted as the new epoch instead of being dropped
	// as stale while slower shards are still finishing reconfiguration.
	d.tcpLock.Lock()
	d.drainingMode = false
	d.currentEpoch = epoch
	d.epochStartTime = time.Now()
	d.lastShardBlockInfo = make(map[uint64]time.Time)
	d.lastShardTxPool = make(map[uint64]int)
	d.lastFallbackTakeoverAt = time.Time{}
	d.staleEpochReportShards = make(map[uint64]bool)
	d.tcpLock.Unlock()

	// ─── 第八步: Final activate。只有最终 ACK 成功的成员才真正启动新 Epoch 共识。 ───
	finalMsgs, finalTargets := buildCSReconfigMessages(epoch, finalNewIPTable, numCS, topology, true)
	_, remainingAcks := d.executeReconfigBarrier(epoch, finalMsgs, finalTargets, "Final")
	d.tcpLock.Lock()
	if len(remainingAcks) > 0 {
		missSet := make(map[string]struct{}, len(remainingAcks))
		for _, ip := range remainingAcks {
			missSet[ip] = struct{}{}
		}
		d.epochReconfigNoAck[epoch] = missSet
	} else {
		delete(d.epochReconfigNoAck, epoch)
	}
	d.tcpLock.Unlock()

	for sid := numCS; sid < d.ChainConfig.ShardNums; sid++ {
		for nid, ip := range finalNewIPTable[sid] {
			reconfigMsg := message.ReconfigInfo{
				Epoch:         epoch,
				NewShardID:    sid,
				NewNodeID:     nid,
				Activate:      true,
				NewIPTable:    finalNewIPTable,
				MainTarget:    params.NoShardID,
				BackupTargets: nil,
				FullBSGMap:    map[uint64][]uint64{},
				SupervisedBy:  params.NoShardID,
			}
			bytes, _ := json.Marshal(reconfigMsg)
			msg := message.MergeMessage(message.CReconfig, bytes)
			networks.TcpDial(msg, ip)
			d.sl.Slog.Printf("[Epoch %d] Sent topology refresh to SS node %s: Shard=%d, Node=%d\n",
				epoch, ip, sid, nid)
		}
	}

	d.sl.Slog.Printf("=== Epoch %d Reconfiguration Complete ===\n", epoch)
}

func (d *Supervisor) recordNodeAction(ip string, actionType string) {
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()

	metrics := d.ensureNodeMetricsLocked(ip)
	d.incrementActionLocked(metrics, actionType)
}

func (d *Supervisor) recordNodeActionForRound(ip string, shardID uint64, epoch int, seqID uint64, actionType string) {
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()

	roundKey := d.makeRoundKey(shardID, epoch, seqID)
	d.updateRoundActionLocked(ip, roundKey, actionType)
}

func (d *Supervisor) makeRoundKey(shardID uint64, epoch int, seqID uint64) string {
	return fmt.Sprintf("E%d-S%d-H%d", epoch, shardID, seqID)
}

func parseShardIDFromRoundKey(roundKey string) (uint64, bool) {
	parts := strings.Split(roundKey, "-")
	if len(parts) != 3 {
		return 0, false
	}
	if !strings.HasPrefix(parts[1], "S") {
		return 0, false
	}
	shardPart := strings.TrimPrefix(parts[1], "S")
	if shardPart == "" {
		return 0, false
	}
	shardID, err := strconv.ParseUint(shardPart, 10, 64)
	if err != nil {
		return 0, false
	}
	return shardID, true
}

func isLeaderSilenceFault(reason string) bool {
	lower := strings.ToLower(reason)
	return strings.Contains(lower, "initial block generation timeout") ||
		strings.Contains(lower, "block generation timeout")
}

func isInitialLeaderSilenceFault(reason string) bool {
	lower := strings.ToLower(reason)
	return strings.Contains(lower, "initial block generation timeout")
}

func initialTakeoverGraceDuration() time.Duration {
	timeout := time.Duration(params.SupervisionTimeout) * time.Millisecond
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	// Keep warmup long enough to absorb post-reconfig transient noise,
	// but short enough so earlier epochs can still trigger valid takeovers.
	grace := 2 * timeout
	if grace < 20*time.Second {
		grace = 20 * time.Second
	}

	if params.ReconfigTimeGap > 0 {
		halfEpoch := time.Duration(params.ReconfigTimeGap) * time.Second / 2
		if halfEpoch > 0 && grace > halfEpoch {
			grace = halfEpoch
		}
	}

	if grace < 15*time.Second {
		grace = 15 * time.Second
	}
	if grace > 35*time.Second {
		grace = 35 * time.Second
	}
	return grace
}

func (d *Supervisor) ensureNodeMetricsLocked(ip string) *NodeMetrics {
	metrics, ok := d.NodeMetricsMap[ip]
	if !ok {
		metrics = &NodeMetrics{Reputation: 0.5, EliminatedAtEpoch: -1, LastTakenOverEpoch: -1}
		d.NodeMetricsMap[ip] = metrics
	}
	return metrics
}

func (d *Supervisor) markShardTakenOverLocked(shardID uint64, epoch int) {
	origNodes, ok := d.EpochOriginalIpTable[shardID]
	if !ok {
		return
	}
	for _, ip := range origNodes {
		m := d.ensureNodeMetricsLocked(ip)
		if m.LastTakenOverEpoch != epoch {
			m.TakenOverCount++
		}
		m.LastTakenOverEpoch = epoch
	}
}

func (d *Supervisor) incrementActionLocked(metrics *NodeMetrics, actionType string) {
	switch actionType {
	case "P":
		metrics.P++
	case "S":
		metrics.S++
	case "E":
		metrics.E++
	}
}

func (d *Supervisor) snapshotEpochActions() map[string][3]int {
	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()

	snapshot := make(map[string][3]int, len(d.NodeMetricsMap))
	for ip, metrics := range d.NodeMetricsMap {
		snapshot[ip] = [3]int{metrics.P, metrics.S, metrics.E}
	}
	return snapshot
}

func (d *Supervisor) decrementActionLocked(metrics *NodeMetrics, actionType string) {
	switch actionType {
	case "P":
		if metrics.P > 0 {
			metrics.P--
		}
	case "S":
		if metrics.S > 0 {
			metrics.S--
		}
	case "E":
		if metrics.E > 0 {
			metrics.E--
		}
	}
}

func (d *Supervisor) updateRoundActionLocked(ip string, roundKey string, actionType string) {
	metrics := d.ensureNodeMetricsLocked(ip)

	roundMap, ok := d.nodeRoundActions[ip]
	if !ok {
		roundMap = make(map[string]string)
		d.nodeRoundActions[ip] = roundMap
	}

	if oldAction, exists := roundMap[roundKey]; exists {
		if oldAction == actionType {
			return
		}
		// E（恶意行为）一旦记录，不得被后续的 P/S 动作覆盖
		// 以防止节点在同一轮次内先作恶后正常参与从而抹去惩罚
		if oldAction == "E" {
			return
		}
		d.decrementActionLocked(metrics, oldAction)
	}

	d.incrementActionLocked(metrics, actionType)
	roundMap[roundKey] = actionType
}

func (d *Supervisor) calculateEpochReputation() {
	// --- 方案公式参数设定 ---
	const (
		Rho     = 0.2   // 时间衰减因子 ρ (调节历史与当前的权重)
		Omega_p = 0.75  // 正常工作奖励参数 ω_p
		Omega_s = -0.25 // 不响应惩罚参数 ω_s
		Omega_e = -0.5  // 错误/作恶惩罚参数 ω_e
	)

	d.sl.Slog.Println("=== Epoch Reputation Settlement ===")

	d.tcpLock.Lock()
	defer d.tcpLock.Unlock()

	// 只对当前活跃 CS 节点结算声誉，候选池与已淘汰节点不参与当轮结算。
	// 注意使用 EpochOriginalIpTable 快照，避免接管路由覆盖导致 leader 识别错误。
	numCSRep := uint64(params.ConsensusShardCount())
	isLeaderMap := make(map[string]bool)
	activeSet := make(map[string]bool)
	for sid := uint64(0); sid < numCSRep; sid++ {
		if nodes, ok := d.EpochOriginalIpTable[sid]; ok {
			for _, ip := range nodes {
				activeSet[ip] = true
			}
			if leaderIP, ok := nodes[0]; ok {
				isLeaderMap[leaderIP] = true
			}
		}
	}

	activeIPs := make([]string, 0, len(activeSet))
	ipToShard := make(map[string]uint64)
	for ip := range activeSet {
		activeIPs = append(activeIPs, ip)
	}
	for sid := uint64(0); sid < numCSRep; sid++ {
		if nodes, ok := d.EpochOriginalIpTable[sid]; ok {
			for _, ip := range nodes {
				ipToShard[ip] = sid
			}
		}
	}
	sort.Strings(activeIPs)

	shardObservedRoundKeys := make(map[uint64]map[string]struct{})
	for _, roundMap := range d.nodeRoundActions {
		for roundKey := range roundMap {
			shardID, ok := parseShardIDFromRoundKey(roundKey)
			if !ok {
				continue
			}
			if _, exists := shardObservedRoundKeys[shardID]; !exists {
				shardObservedRoundKeys[shardID] = make(map[string]struct{})
			}
			shardObservedRoundKeys[shardID][roundKey] = struct{}{}
		}
	}
	shardObservedRounds := make(map[uint64]int, len(shardObservedRoundKeys))
	for shardID, roundKeys := range shardObservedRoundKeys {
		shardObservedRounds[shardID] = len(roundKeys)
	}
	shardHasStaleReports := make(map[uint64]bool, len(d.staleEpochReportShards))
	for shardID := range d.staleEpochReportShards {
		shardHasStaleReports[shardID] = true
	}
	loggedDriftShards := make(map[uint64]bool)
	settlementEpoch := d.currentEpoch
	reconfigNoAckSet := d.epochReconfigNoAck[settlementEpoch]
	ackMissPenaltyApplied := make(map[string]bool)
	noEvidenceReason := make(map[string]string)

	// 兜底: 活跃节点若全轮无行为记录，按分片观测情况补记一次动作，避免结算中出现 P=S=E=0。
	for _, ip := range activeIPs {
		m := d.ensureNodeMetricsLocked(ip)
		if m.P+m.S+m.E > 0 {
			continue
		}

		if _, missedAck := reconfigNoAckSet[ip]; missedAck {
			if isLeaderMap[ip] {
				if m.E == 0 {
					m.E = 1
				}
				d.leaderSilenceReasons[ip] = "Missing Reconfig ACK across all waves"
			} else {
				if m.E == 0 {
					m.E = 1
				}
			}
			ackMissPenaltyApplied[ip] = true
			d.sl.Slog.Printf("Epoch settlement: node %s had no reconfig ACK in epoch %d and no consensus actions; apply E penalty.\n",
				ip, settlementEpoch)
			continue
		}

		shardID, ok := ipToShard[ip]
		if !ok {
			noEvidenceReason[ip] = "Node not found in shard snapshot"
			continue
		}

		noRoundsObserved := !d.takenOverShards[shardID] && shardObservedRounds[shardID] == 0
		if noRoundsObserved && shardHasStaleReports[shardID] {
			if !loggedDriftShards[shardID] {
				d.sl.Slog.Printf("Epoch settlement: shard %d has stale-epoch reports but no active-epoch rounds; skip fallback S/E penalties for this shard.\n", shardID)
				loggedDriftShards[shardID] = true
			}
			noEvidenceReason[ip] = "No active-epoch rounds observed (stale reports detected)"
			continue
		}

		if isLeaderMap[ip] {
			if _, forced := d.leaderSilenceReasons[ip]; forced {
				if m.E == 0 {
					m.E = 1
				}
				continue
			}
			if noRoundsObserved {
				m.E++
				d.leaderSilenceReasons[ip] = "No PBFT rounds observed in epoch"
				continue
			}
		}

		if noRoundsObserved {
			noEvidenceReason[ip] = "No PBFT rounds observed for shard"
			continue
		}

		m.S++
	}

	for _, ip := range activeIPs {
		m := d.ensureNodeMetricsLocked(ip)
		if ip == params.SupervisorAddr {
			continue // 跳过 supervisor 自身
		}
		takenOverTag := ""
		if m.LastTakenOverEpoch == d.currentEpoch {
			takenOverTag = fmt.Sprintf(", TakenOverShardNode=true, LastTakenOverEpoch=%d, TakenOverCount=%d",
				m.LastTakenOverEpoch, m.TakenOverCount)
		}
		ackMissTag := ""
		if ackMissPenaltyApplied[ip] {
			ackMissTag = ", ReconfigAckMissed=true"
		}
		isLeader := isLeaderMap[ip]
		if isLeader {
			if reason, forced := d.leaderSilenceReasons[ip]; forced {
				oldRep := m.Reputation
				m.Reputation = 0
				d.sl.Slog.Printf("Node %s (Leader): Rep %.4f -> %.4f (forced elimination due to silence, Reason=%s, P:%d, S:%d, E:%d%s)\n",
					ip, oldRep, m.Reputation, reason, m.P, m.S, m.E, takenOverTag+ackMissTag)
				m.P = 0
				m.S = 0
				m.E = 0
				continue
			}
			d.sl.Slog.Printf("Node %s (Leader): Rep %.4f -> %.4f (frozen for this epoch, P:%d, S:%d, E:%d%s)\n",
				ip, m.Reputation, m.Reputation, m.P, m.S, m.E, takenOverTag+ackMissTag)
			m.P = 0
			m.S = 0
			m.E = 0
			continue
		}

		cTotal := float64(m.P + m.S + m.E)
		var newRep float64
		if cTotal == 0 {
			if reason, ok := noEvidenceReason[ip]; ok {
				d.sl.Slog.Printf("Node %s (Normal): Rep %.4f -> %.4f (NoEvidence=true, Reason=%s%s)\n",
					ip, m.Reputation, m.Reputation, reason, takenOverTag+ackMissTag)
				m.P = 0
				m.S = 0
				m.E = 0
				continue
			}
			// 普通节点 cTotal=0: 保持原声誉。
			newRep = m.Reputation
		} else {
			// 计算本次 Epoch 的表现分
			score := (Omega_p*float64(m.P) + Omega_s*float64(m.S) + Omega_e*float64(m.E)) / cTotal

			// 套用公式计算最新声誉
			newRep = Rho*m.Reputation + (1.0-Rho)*score
		}

		// 边界限制：声誉值保持在 [0, 1] 之间
		if newRep < 0 {
			newRep = 0
		}
		if newRep > 1 {
			newRep = 1
		}

		d.sl.Slog.Printf("Node %s (Normal): Rep %.4f -> %.4f (P:%d, S:%d, E:%d%s)\n",
			ip, m.Reputation, newRep, m.P, m.S, m.E, takenOverTag+ackMissTag)

		// 更新声誉并清空行为记录，准备迎接下一个 Epoch
		m.Reputation = newRep
		m.P = 0
		m.S = 0
		m.E = 0
	}

	// 清空轮次动作缓存，下一 Epoch 重新统计。
	d.nodeRoundActions = make(map[string]map[string]string)
	d.staleEpochReportShards = make(map[uint64]bool)
	delete(d.epochReconfigNoAck, settlementEpoch)
}
