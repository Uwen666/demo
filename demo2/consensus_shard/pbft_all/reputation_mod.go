package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type ReputationSupervisionMod struct {
	pbftNode *PbftConsensusNode

	stateWaitChans map[uint64]chan *message.ReturnStateMsg
	stateLock      sync.Mutex

	monitorTimer  *time.Timer
	superviseLock sync.Mutex

	tbMonitorTimer              *time.Timer
	isTakingOver                bool
	takeoverTarget              uint64
	emergencySupervisingTargets map[uint64]bool
	voteBox                     map[uint64][]*message.BSGVoteMsg
	voteLock                    sync.Mutex
	decisionMade                map[uint64]bool
	fullBSGMap                  map[uint64][]uint64
	mapLock                     sync.RWMutex
	tbSigMap                    map[uint64]map[uint64][]byte
	tbSigLock                   sync.Mutex
	shardRepTable               map[uint64]float64

	// BSG 监督定时器: 每个紧急监督目标维护独立的计时器
	bsgMonitorTimers map[uint64]*time.Timer

	// reconfigGraceUntil: UnixMilli timestamp until which supervision monitoring
	// is paused after a reconfiguration, giving all nodes time to settle.
	reconfigGraceUntil int64
}

type NodeRep struct {
	NodeID uint64
	Rep    float64
}

func NewReputationSupervisionMod(p *PbftConsensusNode) *ReputationSupervisionMod {
	return &ReputationSupervisionMod{
		pbftNode:                    p,
		stateWaitChans:              make(map[uint64]chan *message.ReturnStateMsg),
		emergencySupervisingTargets: make(map[uint64]bool),
		fullBSGMap:                  make(map[uint64][]uint64),
		tbSigMap:                    make(map[uint64]map[uint64][]byte),
		shardRepTable:               make(map[uint64]float64),
		bsgMonitorTimers:            make(map[uint64]*time.Timer),
	}
}

func (r *ReputationSupervisionMod) initialSupervisionTimeout(grace time.Duration) time.Duration {
	timeout := time.Duration(params.SupervisionTimeout*2) * time.Millisecond
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	// 重配置后需要给迁移、恢复和首轮 view-change 留出足够缓冲，
	// 否则容易在 epoch 切换后集中触发误判接管。
	minAfterGrace := grace + 15*time.Second
	if timeout < minAfterGrace {
		timeout = minAfterGrace
	}

	if r.pbftNode != nil && r.pbftNode.pbftChainConfig != nil {
		blockInterval := time.Duration(r.pbftNode.pbftChainConfig.BlockInterval) * time.Millisecond
		if blockInterval > 0 {
			minByBlock := grace + blockInterval*3
			if timeout < minByBlock {
				timeout = minByBlock
			}
		}
	}

	return timeout
}

func (r *ReputationSupervisionMod) broadcastToShard(targetShardId uint64, msg []byte) {
	nodes, ok := r.pbftNode.ip_nodeTable[targetShardId]
	if !ok {
		fmt.Printf("Error: Shard %d not found In IP table\n", targetShardId)
		return
	}
	for _, ip := range nodes {
		networks.TcpDial(msg, ip)
	}
}

func (r *ReputationSupervisionMod) IsPrimary() bool {
	// view 是 atomic.Int32
	view := uint64(r.pbftNode.view.Load())
	return view%r.pbftNode.node_nums == r.pbftNode.NodeID
}

func (r *ReputationSupervisionMod) HandlePrePrepare(ppMsg *message.PrePrepare) {
	if r.pbftNode.RunningNode.NodeType == shard.NodeType_Storage {
		return
	}
	block := core.DecodeB(ppMsg.RequestMsg.Msg.Content)
	fmt.Printf("[CS %d] Received PrePrepare for Block %d\n", r.pbftNode.ShardID, block.Header.Number)

	// Fire SS state request asynchronously — do NOT block PBFT consensus.
	// The SS state check is for supervision monitoring purposes only;
	// blocking here causes PBFT rounds to exceed the ViewChange timeout.
	go r.requestSSStateAsync(block)

	if r.isTakingOver {
		fmt.Printf("[CS %d] Processing Takover Txs Shard %d\n", r.pbftNode.ShardID, r.takeoverTarget)
	}
}

// requestSSStateAsync sends CGetState to the paired SS and supervising shard
// in a background goroutine, so PBFT consensus is never blocked.
func (r *ReputationSupervisionMod) requestSSStateAsync(block *core.Block) {
	accounts := make([]string, 0)
	for _, tx := range block.Body {
		accounts = append(accounts, tx.Sender, tx.Recipient)
	}

	getStateReq := message.GetStateMsg{
		BlockHeight:   block.Header.Number,
		AccountList:   accounts,
		SenderNode:    r.pbftNode.NodeID,
		SenderShardID: r.pbftNode.ShardID,
	}
	reqBytes, _ := json.Marshal(&getStateReq)
	msg := message.MergeMessage(message.CGetState, reqBytes)

	storageShardID := params.GetPairedStorageShardForReputation(r.pbftNode.ShardID, r.pbftNode.pbftChainConfig.ShardNums)
	r.broadcastToShard(storageShardID, msg)

	supervisedBy := r.pbftNode.RunningNode.SupervisedByShardID
	if supervisedBy != ^uint64(0) && supervisedBy != r.pbftNode.ShardID {
		r.broadcastToShard(supervisedBy, msg)
	}
	if r.isTakingOver {
		fmt.Printf("[CS %d] Takeover: Requesting state from Target Shard %d\n", r.pbftNode.ShardID, r.takeoverTarget)
		targetStorageShardID := params.GetPairedStorageShardForReputation(r.takeoverTarget, r.pbftNode.pbftChainConfig.ShardNums)
		r.broadcastToShard(targetStorageShardID, msg)
	}
}

func (r *ReputationSupervisionMod) HandlePrepare(pMsg *message.Prepare) {}
func (r *ReputationSupervisionMod) HandleCommit(cMsg *message.Commit) {

	if r.pbftNode.RunningNode.NodeType == shard.NodeType_Consensus {

		// 1. 解析区块获取详细信息 (用于构建 TB)
		digestStr := hex.EncodeToString(cMsg.Digest)
		req, ok := r.pbftNode.requestPool[digestStr]
		if !ok {
			// 如果本地还没有收到 PrePrepare，可能无法处理，直接返回
			return
		}
		block := core.DecodeB(req.Msg.Content)
		blockHash := block.Header.Hash()
		tb := message.TimeBeacon{
			ShardID:     r.pbftNode.ShardID,
			BlockHeight: block.Header.Number,
			BlockHash:   blockHash,                // 区块哈希
			StateRoot:   block.Header.StateRoot,   // 状态树根
			TxRoot:      block.Header.TxRoot,      // 交易树根
			ReceiptRoot: block.Header.ReceiptRoot, // 收据树根
			Timestamp:   time.Now().Unix(),
		}

		// 2. 检查是否有资格签名 (Top 10% + VRF)
		if r.checkSigningEligibility(tb.BlockHeight) {

			// 3. 对 TB 结构体进行签名 (序列化后 Hash 再 Sign)
			tbBytes, _ := json.Marshal(tb)
			sig := r.signData(tbBytes) // 模拟签名函数

			if r.IsPrimary() {
				// Leader 自己存
				r.handleTBSignInternal(r.pbftNode.NodeID, tb, sig)
			} else {
				// Replica 发送给 Leader
				signMsg := message.TBSignMsg{
					TB:        tb,
					NodeID:    r.pbftNode.NodeID,
					Signature: sig,
					VRFProof:  []byte("proof"),
				}
				bytes, _ := json.Marshal(signMsg)
				msg := message.MergeMessage(message.CTBSign, bytes)

				// 发送给 Leader
				currentView := uint64(r.pbftNode.view.Load())

				shardNodeCount := r.pbftNode.node_nums

				if shardNodeCount > 0 {
					leaderNodeID := currentView % shardNodeCount
					if leaderIP, ok := r.pbftNode.ip_nodeTable[r.pbftNode.ShardID][leaderNodeID]; ok {
						networks.TcpDial(msg, leaderIP)
					}
				}
			}
		}
	}
}

func (r *ReputationSupervisionMod) handleTBSign(content []byte) {
	var msg message.TBSignMsg
	json.Unmarshal(content, &msg)

	// 1. 验证资格 (Leader 复核 VRF)
	// 在真实 VRF 中，这里需要使用 msg.VRFProof 和公钥进行验证
	// 在我们的模拟实现(utils.VRFVerify)中，输入是公开的(NodeID+Height)，
	// 所以 Leader 可以直接重算一遍来验证发送者是否撒谎
	const SelectionThreshold = 0.3
	if !utils.VRFVerify(msg.NodeID, int64(msg.TB.BlockHeight), SelectionThreshold) {
		fmt.Printf("[Leader]  Node %d sent signature but failed VRF check.\n", msg.NodeID)
		return
	}

	// 2. 资格验证通过，存入签名集合
	r.handleTBSignInternal(msg.NodeID, msg.TB, msg.Signature)
}

func (r *ReputationSupervisionMod) handleTBSignInternal(nodeID uint64, tb message.TimeBeacon, sig []byte) {
	height := tb.BlockHeight
	r.tbSigLock.Lock()
	defer r.tbSigLock.Unlock()

	if _, ok := r.tbSigMap[height]; !ok {
		r.tbSigMap[height] = make(map[uint64][]byte)
	}
	r.tbSigMap[height][nodeID] = sig

	// 3. 检查聚合阈值
	// 动态适配小分片: 4 节点 × 30% VRF 选中率 ≈ 期望 1.2 个签名
	// 要求 ≥2 会导致 ~65% 区块无法聚合 TB → P 永远为 0
	minSigs := 2
	if r.pbftNode.node_nums <= 4 {
		minSigs = 1 // 小分片至少 1 个签名即可提交
	}
	MinSignaturesRequired := minSigs

	if len(r.tbSigMap[height]) == MinSignaturesRequired {
		fmt.Printf("[Leader] ⚡ TB Aggregated (Height %d). Uploading...\n", height)

		uploadMsg := message.UploadTBMsg{
			TB:         tb,
			Signatures: r.tbSigMap[height],
		}
		bytes, _ := json.Marshal(uploadMsg)
		sendMsg := message.MergeMessage(message.CUploadTB, bytes)
		networks.TcpDial(sendMsg, params.SupervisorAddr)

		// 清理防止内存泄漏
		delete(r.tbSigMap, height)
	}
}

func (r *ReputationSupervisionMod) signData(data []byte) []byte {
	// 实际应使用 r.pbftNode.RunningNode.PrivateKey 进行签名
	h := sha256.Sum256(data)
	return h[:]
}

func (r *ReputationSupervisionMod) HandleViewChange(vcMsg *message.ViewChangeMsg) {}
func (r *ReputationSupervisionMod) HandleNewView(viewMsg *message.NewViewMsg)     {}

func (r *ReputationSupervisionMod) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CGetState:
		// SS 处理请求
		if r.pbftNode.RunningNode.NodeType != shard.NodeType_Storage {
			// 如果我是 CS_l，我可能也收到了这个广播（作为监听）
			r.checkSupervisionTrigger(content)
			return true
		}

		var req message.GetStateMsg
		json.Unmarshal(content, &req)

		// SS 模拟返回状态
		res := message.ReturnStateMsg{
			BlockHeight: req.BlockHeight,
			StateRoot:   []byte("mock_root"),
		}
		resBytes, _ := json.Marshal(res)
		sendMsg := message.MergeMessage(message.CReturnState, resBytes)

		targetIP := r.pbftNode.ip_nodeTable[req.SenderShardID][req.SenderNode]
		networks.TcpDial(sendMsg, targetIP)

	case message.CReturnState:
		// CS 处理返回
		var res message.ReturnStateMsg
		json.Unmarshal(content, &res)
		r.stateLock.Lock()
		if ch, ok := r.stateWaitChans[res.BlockHeight]; ok {
			ch <- &res
			delete(r.stateWaitChans, res.BlockHeight)
		}
		r.stateLock.Unlock()

	case message.CTakeoverSupervision:
		type SupervisionTakeoverInfo struct {
			TargetShardID uint64
			Mode          string
		}
		var info SupervisionTakeoverInfo
		json.Unmarshal(content, &info)
		if info.Mode == "" {
			info.Mode = "BSG"
		}

		fmt.Printf("[Node S%dN%d] ALERT: Taking over SUPERVISION task for Shard %d (mode=%s)\n",
			r.pbftNode.ShardID, r.pbftNode.NodeID, info.TargetShardID, info.Mode)

		if info.Mode == "Direct" {
			r.superviseLock.Lock()
			r.pbftNode.RunningNode.SuperviseTargetShardID = info.TargetShardID
			if r.monitorTimer != nil {
				r.monitorTimer.Stop()
			}
			monitorTimeoutMs := params.SupervisionTimeout
			blockIntervalMs := int(r.pbftNode.pbftChainConfig.BlockInterval)
			minRequired := blockIntervalMs*2 + 1000
			if monitorTimeoutMs < minRequired {
				monitorTimeoutMs = minRequired
			}
			targetShardID := info.TargetShardID
			r.monitorTimer = time.AfterFunc(time.Duration(monitorTimeoutMs)*time.Millisecond, func() {
				r.reportFailure("Block Generation Timeout", targetShardID)
			})
			r.superviseLock.Unlock()
			return true
		}

		r.superviseLock.Lock()
		r.emergencySupervisingTargets[info.TargetShardID] = true
		r.superviseLock.Unlock()

		// 初始化 BSG 投票箱并启动监督定时器
		r.initVoting()
		r.startBSGMonitorTimer(info.TargetShardID)

	case message.CTakeoverRouting:
		var info message.TakeoverRoutingMsg
		json.Unmarshal(content, &info)
		r.pbftNode.setTakeoverRouting(info.TargetShardID, info.RouteIPs)
		if len(info.RouteIPs) == 0 {
			fmt.Printf("[Node S%dN%d] Cleared takeover routing override for Shard %d\n", r.pbftNode.ShardID, r.pbftNode.NodeID, info.TargetShardID)
		} else {
			fmt.Printf("[Node S%dN%d] Updated takeover routing for Shard %d (mode=%s, routes=%v)\n",
				r.pbftNode.ShardID, r.pbftNode.NodeID, info.TargetShardID, info.Mode, info.RouteIPs)
		}

	case message.CBlockInfo:
		r.handleBlockHeaderSupervision(content)

	case message.CConfirmTB:
		r.handleTBConfirmation(content)
		fmt.Println("[Node] TB Confirmed by L1/Supervisor.")

	case message.CTakeover:
		r.handleTakeoverCommand(content)
		fmt.Println("[CS_l] Received Takeover Authorization from L1.")
		// 在这里切换本地交易池逻辑，开始接收目标分片的交易

	case message.CReconfig:
		var info message.ReconfigInfo
		json.Unmarshal(content, &info)

		oldShardID := r.pbftNode.ShardID
		oldNodeID := r.pbftNode.NodeID
		phaseLabel := "PROVISIONAL"
		if info.Activate {
			phaseLabel = "FINAL"
		}

		fmt.Printf("[Node %d] %s RECONFIG START (Epoch %d)...\n", r.pbftNode.NodeID, phaseLabel, info.Epoch)
		fmt.Printf("  >> Old Identity: Shard %d, Node %d\n", oldShardID, oldNodeID)
		fmt.Printf("  >> New Identity: Shard %d, Node %d\n", info.NewShardID, info.NewNodeID)

		// 1. 更新身份信息 (跨分片重配置核心)
		r.pbftNode.ShardID = info.NewShardID
		r.pbftNode.NodeID = info.NewNodeID
		r.pbftNode.RunningNode.ShardID = info.NewShardID
		r.pbftNode.RunningNode.NodeID = info.NewNodeID

		// 同步更新链配置中的 ShardID，否则交易路由/执行仍用旧值
		r.pbftNode.pbftChainConfig.ShardID = info.NewShardID

		// 2. 更新监督任务
		r.pbftNode.RunningNode.SuperviseTargetShardID = info.MainTarget
		r.pbftNode.RunningNode.BackupTargetShardIDs = info.BackupTargets
		r.pbftNode.RunningNode.SupervisedByShardID = info.SupervisedBy

		// 3. 更新本地的网络映射表 (IP Table)
		params.IPmap_nodeTable = info.NewIPTable
		r.pbftNode.ip_nodeTable = info.NewIPTable
		r.pbftNode.clearAllTakeoverRouting()

		// 更新节点数 (可能因淘汰而变化)
		if nodes, ok := info.NewIPTable[info.NewShardID]; ok {
			r.pbftNode.node_nums = uint64(len(nodes))
			r.pbftNode.malicious_nums = (r.pbftNode.node_nums - 1) / 3
			if info.Activate && r.pbftNode.node_nums > 0 && r.pbftNode.node_nums < 4 {
				fmt.Printf("  >> WARNING: undersized committee (%d nodes); PBFT quorum now requires unanimous votes\n", r.pbftNode.node_nums)
			}
		}
		r.pbftNode.RunningNode.NodeType = params.GetNodeTypeForReputation(r.pbftNode.ShardID, r.pbftNode.pbftChainConfig.ShardNums)

		r.mapLock.Lock()
		r.fullBSGMap = info.FullBSGMap
		r.mapLock.Unlock()

		fmt.Printf("  >> Updated Full BSG Map. Total targets tracked: %d\n", len(r.fullBSGMap))

		// 4. Always reset consensus-chain state after reconfiguration.
		// Even if shardID does not change, shard membership may still change,
		// and mixed historical chains inside one shard will trigger
		// "not a valid block" (parent hash mismatch) and endless view-change.
		// Preserve the TxPool so that transactions accumulated before the epoch
		// boundary are not discarded — nodes need them to produce blocks in the
		// new epoch, especially when no new injections occur after tx injection
		// has completed.
		if params.IsConsensusShardForReputation(info.NewShardID, r.pbftNode.pbftChainConfig.ShardNums) {
			r.pbftNode.CurChain.ResetToGenesisPreserveTxPool()
			if oldShardID != info.NewShardID {
				fmt.Printf("  >> Blockchain reset to genesis (TxPool preserved) for cross-shard migration\n")
			} else {
				fmt.Printf("  >> Blockchain reset to genesis (TxPool preserved) for same-shard membership refresh\n")
			}
		}

		// 5. 重置共识状态。provisional 只做同步，不真正激活；final activate 才开放出块。
		r.pbftNode.stopSignal.Store(false)
		r.pbftNode.currentEpoch.Store(int32(info.Epoch))
		r.pbftNode.view.Store(0)
		r.pbftNode.sequenceID = 1
		if info.Activate {
			r.pbftNode.pbftStage.Store(1)
		} else {
			r.pbftNode.pbftStage.Store(0)
		}
		// Force-release sequenceLock to unblock any stale proposal goroutine.
		// After CReconfig clears cntCommitConfirm, old-round handleCommit goroutines
		// are stuck in conditionalVar.Wait() and can never reach the 2f+1 threshold
		// needed to call sequenceLock.Unlock(). This causes deadlock: the new-epoch
		// proposal goroutine blocks forever at sequenceLock.Lock().
		// TryLock+Unlock guarantees the mutex is in the unlocked state regardless of
		// whether it was previously held (by the stale old round) or not.
		r.pbftNode.sequenceLock.TryLock()
		r.pbftNode.sequenceLock.Unlock()
		r.pbftNode.requestPool = make(map[string]*message.Request)
		r.pbftNode.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
		r.pbftNode.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
		r.pbftNode.isCommitBordcast = make(map[string]bool)
		r.pbftNode.isReply = make(map[string]bool)
		r.pbftNode.height2Digest = make(map[uint64]string)
		r.pbftNode.vcLock.Lock()
		r.pbftNode.viewChangeMap = make(map[ViewChangeData]map[uint64]bool)
		r.pbftNode.newViewMap = make(map[ViewChangeData]map[uint64]bool)
		r.pbftNode.vcLock.Unlock()
		pendingHold := 2 * time.Duration(params.ReconfigTimeGap) * time.Second
		if pendingHold < 60*time.Second {
			pendingHold = 60 * time.Second
		}
		if info.Activate {
			r.pbftNode.lastCommitTime.Store(time.Now().UnixMilli())
		} else {
			r.pbftNode.lastCommitTime.Store(time.Now().Add(pendingHold).UnixMilli())
		}

		// 6. 重置监督计时器 + 设置重配置宽限期。
		r.superviseLock.Lock()
		if r.monitorTimer != nil {
			r.monitorTimer.Stop()
			r.monitorTimer = nil
		}
		if r.tbMonitorTimer != nil {
			r.tbMonitorTimer.Stop()
			r.tbMonitorTimer = nil
		}
		r.isTakingOver = false
		for _, timer := range r.bsgMonitorTimers {
			timer.Stop()
		}
		r.bsgMonitorTimers = make(map[uint64]*time.Timer)
		r.emergencySupervisingTargets = make(map[uint64]bool)
		r.voteBox = nil
		r.decisionMade = nil

		graceDuration := 30 * time.Second
		if !info.Activate && pendingHold > graceDuration {
			graceDuration = pendingHold
		}
		r.reconfigGraceUntil = time.Now().Add(graceDuration).UnixMilli()

		if info.Activate && r.IsPrimary() && info.MainTarget != params.NoShardID {
			initialTimeout := r.initialSupervisionTimeout(graceDuration)
			targetShardID := info.MainTarget
			r.monitorTimer = time.AfterFunc(initialTimeout, func() {
				r.reportFailure("Initial Block Generation Timeout after reconfig", targetShardID)
			})
		}
		r.superviseLock.Unlock()
		fmt.Printf("  >> Supervision grace period: monitoring paused for %v after reconfig\n", graceDuration)

		if info.Activate {
			if r.pbftNode.NodeID == 0 {
				fmt.Println("  >> I am the LEADER of this Epoch.")
			}
			if r.pbftNode.RunningNode.NodeType == shard.NodeType_Storage {
				fmt.Println("  >> Role in this epoch: STORAGE")
			} else {
				fmt.Println("  >> Role in this epoch: CONSENSUS")
			}
		} else {
			fmt.Println("  >> Provisional reconfig only: waiting for final activation before starting consensus.")
		}

		if oldShardID != info.NewShardID {
			fmt.Printf("  >> CROSS-SHARD MIGRATION: Shard %d -> Shard %d\n", oldShardID, info.NewShardID)
		}
		if info.Activate {
			fmt.Printf("FINAL RECONFIG COMPLETE. (Epoch %d, new node_nums=%d, seq reset=%d)\n", info.Epoch, r.pbftNode.node_nums, r.pbftNode.sequenceID)
		} else {
			fmt.Printf("PROVISIONAL RECONFIG ACKED. (Epoch %d, target shard=%d, target node=%d)\n", info.Epoch, info.NewShardID, info.NewNodeID)
		}

		ack := message.ReconfigAckMsg{
			Epoch:      info.Epoch,
			NodeIP:     r.pbftNode.RunningNode.IPaddr,
			NewShardID: info.NewShardID,
			NewNodeID:  info.NewNodeID,
		}
		ackBytes, _ := json.Marshal(ack)
		networks.TcpDial(message.MergeMessage(message.CReconfigAck, ackBytes), params.SupervisorAddr)

	case message.CEliminateNotice:
		var info message.EliminateNoticeMsg
		json.Unmarshal(content, &info)
		fmt.Printf("[Node S%dN%d] ⚠ ELIMINATED at Epoch %d! Reason: %s, Reputation: %.4f\n",
			r.pbftNode.ShardID, r.pbftNode.NodeID, info.Epoch, info.Reason, info.Reputation)
		fmt.Println("  >> This node will stop participating in consensus.")
		// 标记为已停止（不参与后续共识）
		r.pbftNode.stopSignal.Store(true)

	case message.CVoteBSG:
		r.handleBSGVote(content)

	case message.CSupervisionAssign:
		var info message.SupervisionAssignMsg
		json.Unmarshal(content, &info)

		if r.pbftNode.RunningNode.NodeType == shard.NodeType_Storage {
			r.pbftNode.RunningNode.SuperviseTargetShardID = params.NoShardID
			r.pbftNode.RunningNode.BackupTargetShardIDs = nil
			r.pbftNode.RunningNode.SupervisedByShardID = params.NoShardID
			return true
		}

		r.pbftNode.RunningNode.SuperviseTargetShardID = info.MainTarget
		r.pbftNode.RunningNode.BackupTargetShardIDs = info.BackupTargets
		r.pbftNode.RunningNode.SupervisedByShardID = info.SupervisedBy

		r.mapLock.Lock()
		r.fullBSGMap = info.FullBSGMap
		r.mapLock.Unlock()

		fmt.Printf("[Node S%dN%d] Supervision assigned: Monitor Shard %d, Monitored by Shard %d\n",
			r.pbftNode.ShardID, r.pbftNode.NodeID, info.MainTarget, info.SupervisedBy)

		// 主动启动初始监督定时器：如果被监督分片从一开始就没有出块（Leader 未提案），
		// 则不会发送 CGetState，checkSupervisionTrigger 永远不会被调用。
		// 因此在收到监督任务分配时，Leader 主动启动一个定时器来检测这种"完全沉默"的情况。
		if r.IsPrimary() && info.MainTarget != params.NoShardID {
			r.superviseLock.Lock()
			// 首次启动定时器，给被监督分片一个较宽松的启动窗口
			// (节点启动有 8 秒等待 + 出块间隔)
			initialTimeout := r.initialSupervisionTimeout(0)
			if r.monitorTimer != nil {
				r.monitorTimer.Stop()
			}
			targetShardID := info.MainTarget
			r.monitorTimer = time.AfterFunc(initialTimeout, func() {
				r.reportFailure("Initial Block Generation Timeout (shard never started)", targetShardID)
			})
			r.superviseLock.Unlock()
			fmt.Printf("[Node S%dN%d] Started initial supervision timer for Shard %d (timeout=%v)\n",
				r.pbftNode.ShardID, r.pbftNode.NodeID, info.MainTarget, initialTimeout)
		}

	case message.CStopMonitoring:
		// L1 通知：某分片已被接管，停止监督该分片
		type StopMonitoringInfo struct {
			TargetShardID uint64
		}
		var info StopMonitoringInfo
		json.Unmarshal(content, &info)

		r.superviseLock.Lock()
		// 如果该分片是我的主监督目标，停止定时器
		if r.pbftNode.RunningNode.SuperviseTargetShardID == info.TargetShardID {
			if r.monitorTimer != nil {
				r.monitorTimer.Stop()
				r.monitorTimer = nil
			}
			if r.tbMonitorTimer != nil {
				r.tbMonitorTimer.Stop()
				r.tbMonitorTimer = nil
			}
			r.pbftNode.RunningNode.SuperviseTargetShardID = params.NoShardID
			fmt.Printf("[Node S%dN%d] Stopped main supervision for taken-over Shard %d\n",
				r.pbftNode.ShardID, r.pbftNode.NodeID, info.TargetShardID)
		}
		// 如果该分片在 BSG 紧急监督列表中，也清除
		if r.emergencySupervisingTargets[info.TargetShardID] {
			delete(r.emergencySupervisingTargets, info.TargetShardID)
			if timer, ok := r.bsgMonitorTimers[info.TargetShardID]; ok {
				timer.Stop()
				delete(r.bsgMonitorTimers, info.TargetShardID)
			}
			fmt.Printf("[Node S%dN%d] Stopped BSG supervision for taken-over Shard %d\n",
				r.pbftNode.ShardID, r.pbftNode.NodeID, info.TargetShardID)
		}
		r.superviseLock.Unlock()
	}
	return true
}

// 核心逻辑: 判断是否有资格签名 (Top 10% AND VRF Selected)
func (r *ReputationSupervisionMod) checkSigningEligibility(height uint64) bool {
	// 设定选中概率阈值
	// 例如 0.3 表示大约 30% 的节点会被选中
	// 这个值可以根据分片大小动态调整，确保至少有 k 个节点被选中
	const SelectionThreshold = 0.3

	// 直接调用 VRF 进行随机验证
	// 输入: 我的 NodeID, 当前高度 (作为随机种子), 阈值
	// 只要 VRF 输出 < Threshold，即视为中签
	isSelected := utils.VRFVerify(r.pbftNode.NodeID, int64(height), SelectionThreshold)

	if isSelected {
		fmt.Printf("[Node %d]  VRF Selected for Height %d\n", r.pbftNode.NodeID, height)
	}

	return isSelected
}

// 监督逻辑: 检查是否触发计时器
func (r *ReputationSupervisionMod) checkSupervisionTrigger(content []byte) {
	// 0. 重配置宽限期内不启动监督计时器
	if time.Now().UnixMilli() < r.reconfigGraceUntil {
		return
	}

	// 1. 解析
	var req message.GetStateMsg
	json.Unmarshal(content, &req)

	// 2a. 检查是否是主监督目标
	isMainTarget := r.pbftNode.RunningNode.SuperviseTargetShardID != params.NoShardID &&
		req.SenderShardID == r.pbftNode.RunningNode.SuperviseTargetShardID
	// 2b. 检查是否是 BSG 紧急监督目标
	isEmergencyTarget := r.emergencySupervisingTargets[req.SenderShardID]

	if !isMainTarget && !isEmergencyTarget {
		return
	}
	// 3. 只有 Leader 负责计时
	if !r.IsPrimary() {
		return
	}

	fmt.Printf("[Supervisor %d] Monitoring Shard %d start consensus.\n", r.pbftNode.ShardID, req.SenderShardID)
	monitorTimeoutMs := params.SupervisionTimeout
	blockIntervalMs := int(r.pbftNode.pbftChainConfig.BlockInterval)
	minRequired := blockIntervalMs*2 + 1000
	if monitorTimeoutMs < minRequired {
		monitorTimeoutMs = minRequired
	}

	if isMainTarget {
		// 主监督: 超时直接向 L1 上报
		r.superviseLock.Lock()
		if r.monitorTimer != nil {
			r.monitorTimer.Stop()
		}
		r.monitorTimer = time.AfterFunc(time.Duration(monitorTimeoutMs)*time.Millisecond, func() {
			r.reportFailure("Block Generation Timeout", req.SenderShardID)
		})
		r.superviseLock.Unlock()
	} else if isEmergencyTarget {
		// BSG 紧急监督: 超时通过声誉加权投票
		r.startBSGMonitorTimer(req.SenderShardID)
	}
}

func (r *ReputationSupervisionMod) startBlockTimer(targetID uint64) {
	// 实现 map 类型的 timer 管理...
	fmt.Printf("[Supervisor] Timer started for Target %d\n", targetID)
	// time.AfterFunc(..., func() { r.reportFailure(..., targetID) })
}

// 上报失效并请求接管
func (r *ReputationSupervisionMod) reportFailure(reason string, targetShardID uint64) {
	// 重配置宽限期内不上报失效（避免过渡期误判）
	if time.Now().UnixMilli() < r.reconfigGraceUntil {
		fmt.Printf("[Supervisor %d] Skipping failure report for Shard %d (within reconfig grace period)\n", r.pbftNode.ShardID, targetShardID)
		return
	}
	fmt.Printf("[Supervisor %d] Timeout! Reporting failure of Shard %d. Reason: %s\n", r.pbftNode.ShardID, targetShardID, reason)

	currentPoolSize := 0
	if r.pbftNode.CurChain != nil && r.pbftNode.CurChain.Txpool != nil {
		currentPoolSize = r.pbftNode.CurChain.Txpool.GetTxQueueLen()
	}

	report := message.SupervisionResultMsg{
		ReporterShardID: r.pbftNode.ShardID,
		TargetShardID:   targetShardID,
		Epoch:           int(r.pbftNode.currentEpoch.Load()),
		SeqID:           r.pbftNode.sequenceID,
		IsFaulty:        true,
		Reason:          reason, // 使用传入的 reason
		CurrentPoolSize: currentPoolSize,
		TargetPoolSize:  0, // supervisor will use its own estimate
		ReporterNode:    r.pbftNode.RunningNode,
	}
	bytes, _ := json.Marshal(report)
	msg := message.MergeMessage(message.CSupervisionRes, bytes)
	networks.TcpDial(msg, params.SupervisorAddr)
}

// startBSGMonitorTimer 启动 BSG 紧急监督定时器（加锁版本，外部调用）
func (r *ReputationSupervisionMod) startBSGMonitorTimer(targetShardID uint64) {
	r.superviseLock.Lock()
	r.startBSGMonitorTimerLocked(targetShardID)
	r.superviseLock.Unlock()
}

// startBSGMonitorTimerLocked 启动 BSG 紧急监督定时器（调用方已持有 superviseLock）
// 如果目标分片在超时期间没有出块，BSG 成员将通过声誉加权投票进行决策。
func (r *ReputationSupervisionMod) startBSGMonitorTimerLocked(targetShardID uint64) {
	if existing, ok := r.bsgMonitorTimers[targetShardID]; ok {
		existing.Stop()
	}

	timeout := time.Duration(params.SupervisionTimeout) * time.Millisecond
	r.bsgMonitorTimers[targetShardID] = time.AfterFunc(timeout, func() {
		// 重配置宽限期内不投票
		if time.Now().UnixMilli() < r.reconfigGraceUntil {
			return
		}
		fmt.Printf("[BSG Node S%dN%d] Timeout for supervised Shard %d! Casting BSG vote (FAULTY).\n",
			r.pbftNode.ShardID, r.pbftNode.NodeID, targetShardID)
		r.castBSGVote(targetShardID, true)
	})
}

func (r *ReputationSupervisionMod) handleBlockHeaderSupervision(content []byte) {
	var bim message.BlockInfoMsg
	json.Unmarshal(content, &bim)

	// 1. 检查是否是我监督的分片 (主目标或紧急监督目标)
	isMainTarget := r.pbftNode.RunningNode.SuperviseTargetShardID != params.NoShardID && bim.SenderShardID == r.pbftNode.RunningNode.SuperviseTargetShardID
	isEmergencyTarget := r.emergencySupervisingTargets[bim.SenderShardID]
	if !isMainTarget && !isEmergencyTarget {
		return
	}
	if !r.IsPrimary() {
		return
	}

	// 2. 收到区块头，说明出块正常
	r.superviseLock.Lock()
	if isMainTarget {
		if r.monitorTimer != nil {
			r.monitorTimer.Stop()
		}
		if r.tbMonitorTimer != nil {
			r.tbMonitorTimer.Stop()
			r.tbMonitorTimer = nil
		}
		// 重新启动定时器等待下一个区块。如果被监督分片在两次共识之间崩溃
		// （提交区块后、开始下一轮前），不会有新的 CGetState 来触发
		// checkSupervisionTrigger，因此必须在此处主动重启定时器。
		monitorTimeoutMs := params.SupervisionTimeout
		blockIntervalMs := int(r.pbftNode.pbftChainConfig.BlockInterval)
		minRequired := blockIntervalMs*2 + 1000
		if monitorTimeoutMs < minRequired {
			monitorTimeoutMs = minRequired
		}
		targetShardID := bim.SenderShardID
		r.monitorTimer = time.AfterFunc(time.Duration(monitorTimeoutMs)*time.Millisecond, func() {
			r.reportFailure("Block Generation Timeout", targetShardID)
		})
	}

	// 3. BSG 紧急监督目标: 重置该目标的 BSG 定时器
	if isEmergencyTarget {
		if timer, ok := r.bsgMonitorTimers[bim.SenderShardID]; ok {
			timer.Stop()
		}
		// 重新启动定时器等待下一次出块
		r.startBSGMonitorTimerLocked(bim.SenderShardID)
	}
	r.superviseLock.Unlock()

	fmt.Printf("[Supervisor %d] Block generated by Shard %d. Supervision heartbeat OK.\n", r.pbftNode.ShardID, bim.SenderShardID)
}

// 处理 TB 确认消息 (停止 TB 计时)
func (r *ReputationSupervisionMod) handleTBConfirmation(content []byte) {
	// 解析消息找到来源分片
	// 这里简单处理，假设收到的广播都是相关的

	r.superviseLock.Lock()
	if r.tbMonitorTimer != nil {
		r.tbMonitorTimer.Stop()
		fmt.Printf("[Supervisor %d] TB Confirmed. Supervision Cycle Completed.\n", r.pbftNode.ShardID)
	}
	r.superviseLock.Unlock()
}

func (r *ReputationSupervisionMod) handleTakeoverCommand(content []byte) {
	// 解码接管消息，获取实际被接管的分片 ID
	var tkMsg message.TakeoverMsg
	if err := json.Unmarshal(content, &tkMsg); err != nil {
		// 兼容旧格式: 使用自身监督目标
		tkMsg.TargetShardID = r.pbftNode.RunningNode.SuperviseTargetShardID
	}
	if tkMsg.TargetShardID == params.NoShardID {
		fmt.Printf("[CS %d] Ignoring takeover command without a valid target\n", r.pbftNode.ShardID)
		return
	}
	fmt.Printf("[CS %d] ACTIVATING TAKEOVER MODE for Shard %d (mode: %s)\n", r.pbftNode.ShardID, tkMsg.TargetShardID, tkMsg.Mode)

	r.isTakingOver = true
	r.takeoverTarget = tkMsg.TargetShardID

	// Leader sends a takeover-completion TB to the supervisor so that
	// TestTakeoverTime can record the end timestamp.
	if r.IsPrimary() {
		takeoverTB := message.TimeBeacon{
			ShardID:   r.takeoverTarget,
			Timestamp: time.Now().Unix(),
		}
		tbBytes, _ := json.Marshal(takeoverTB)
		sig := r.signData(tbBytes)
		uploadMsg := message.UploadTBMsg{
			ShardID:              r.takeoverTarget,
			TB:                   takeoverTB,
			IsTakeoverCompletion: true,
			TakeoverMode:         tkMsg.Mode,
			Signatures:           map[uint64][]byte{r.pbftNode.NodeID: sig},
		}
		bytes, _ := json.Marshal(uploadMsg)
		sendMsg := message.MergeMessage(message.CUploadTB, bytes)
		networks.TcpDial(sendMsg, params.SupervisorAddr)
		fmt.Printf("[CS %d] Sent takeover-completion TB for Shard %d to Supervisor\n", r.pbftNode.ShardID, r.takeoverTarget)
	}
}
func (r *ReputationSupervisionMod) initVoting() {
	r.voteBox = make(map[uint64][]*message.BSGVoteMsg)
	r.decisionMade = make(map[uint64]bool)
}

func (r *ReputationSupervisionMod) castBSGVote(targetShardID uint64, isFaulty bool) {
	// 1. 获取 BSG Leader (声誉最高的分片)

	// 构造投票消息
	vote := message.BSGVoteMsg{
		VoterShardID:    r.pbftNode.ShardID,
		TargetShardID:   targetShardID,
		IsFaulty:        isFaulty,
		VoterReputation: r.pbftNode.RunningNode.Reputation, // 使用当前声誉
		Timestamp:       time.Now().Unix(),
	}
	voteBytes, _ := json.Marshal(vote)
	msg := message.MergeMessage(message.CVoteBSG, voteBytes)

	// 2. 发送给 BSG Leader (或广播给 BSG 全员)
	// 假设我们有一个函数 getBSGMembers(targetID) 返回成员列表
	// 为了演示，这里假设广播给 BSG 所有成员
	bsgMembers := r.getBSGMembers(targetShardID)
	leaderShardID := r.findHighestReputationShard(bsgMembers)
	if r.pbftNode.ShardID == leaderShardID {
		// 如果我自己就是 Leader，直接内部处理
		r.handleBSGVote(voteBytes)
	} else {
		// 如果我是普通成员，只发送给 Leader (O(1) 的跨分片通信)
		fmt.Printf("[BSG Node %d] Sending vote to BSG Leader %d.\n", r.pbftNode.ShardID, leaderShardID)
		r.broadcastToShard(leaderShardID, msg) // 实际上是发送给那个分片的节点

		// 3. 启动超时防“装死”机制 (借鉴 PBFT View Change)
		r.startLeaderWatchdog(targetShardID, voteBytes)
	}
}
func (r *ReputationSupervisionMod) getReputation(shardID uint64) float64 {
	r.superviseLock.Lock()
	defer r.superviseLock.Unlock()

	// 尝试从本地信誉表中获取该分片的信誉值
	if rep, ok := r.shardRepTable[shardID]; ok {
		return rep
	}

	// 如果没有查到（例如系统刚启动，或者模拟器环境下未完全初始化）
	// 返回一个默认的基础信誉值
	return 1.0
}
func (r *ReputationSupervisionMod) findHighestReputationShard(members []uint64) uint64 {
	var maxRep float64 = -1.0
	var leader uint64 = members[0]
	for _, m := range members {
		rep := r.getReputation(m) // 假设有获取声誉的辅助方法
		if rep > maxRep {
			maxRep = rep
			leader = m
		}
	}
	return leader
}

func (r *ReputationSupervisionMod) startLeaderWatchdog(targetShardID uint64, originalVote []byte) {
	// 设置超时时间 (例如 3 倍的正常网络延迟)
	timeout := time.Duration(params.SupervisionTimeout*3) * time.Millisecond

	time.AfterFunc(timeout, func() {
		// 检查是否已经接到了 L1 的接管通知
		if !r.decisionMade[targetShardID] {
			fmt.Printf("[BSG Node %d] BSG Leader Timeout! Sending vote DIRECTLY to L1.\n", r.pbftNode.ShardID)

			// 把本应发给 Leader 的选票，打包直接发给 L1 (Supervisor)
			// L1 在 handleMessage 中可以额外增加一个直接计票的逻辑
			msg := message.MergeMessage(message.CVoteBSG, originalVote)
			networks.TcpDial(msg, params.SupervisorAddr)
		}
	})
}
func (r *ReputationSupervisionMod) handleBSGVote(content []byte) {
	var vote message.BSGVoteMsg
	json.Unmarshal(content, &vote)

	r.voteLock.Lock()
	defer r.voteLock.Unlock()

	targetID := vote.TargetShardID

	// 防止重复处理已决议的目标
	if r.decisionMade[targetID] {
		return
	}

	// 存入投票箱
	if _, ok := r.voteBox[targetID]; !ok {
		r.voteBox[targetID] = make([]*message.BSGVoteMsg, 0)
	}

	// 检查是否重复投票 (实际应检查 VoterShardID)
	for _, v := range r.voteBox[targetID] {
		if v.VoterShardID == vote.VoterShardID {
			return // 已投过
		}
	}

	r.voteBox[targetID] = append(r.voteBox[targetID], &vote)

	// 尝试计票
	r.tryTallyVotes(targetID)
}

func (r *ReputationSupervisionMod) tryTallyVotes(targetID uint64) {
	votes := r.voteBox[targetID]

	// 1. 计算 BSG 成员的总声誉 (固定的分母)
	var sumTotalBSGRep float64 = 0.0
	bsgMembers := r.getBSGMembers(targetID)

	// 无论成员是否投票、是否装死，分母必须是 BSG 全员的声誉总和
	for _, memberID := range bsgMembers {
		sumTotalBSGRep += r.getReputation(memberID)
	}

	// 2. 统计当前已收到的加权票数 (分子)
	var sumFaultyRep float64 = 0.0
	var sumNormalRep float64 = 0.0

	for _, v := range votes {
		if v.IsFaulty {
			sumFaultyRep += v.VoterReputation
		} else {
			sumNormalRep += v.VoterReputation
		}
	}

	// 设定 PBFT 式的加权法定人数阈值 (例如 2/3)
	const DecisionThreshold = 0.66

	fmt.Printf("[BSG Leader %d] Tallying Target %d: FaultyRep=%.2f, NormalRep=%.2f, TotalBSGRep=%.2f\n",
		r.pbftNode.ShardID, targetID, sumFaultyRep, sumNormalRep, sumTotalBSGRep)

	// -------------------------------------------------------------------------
	// 3. 核心判断：信誉加权达标即通过，无需等待其余选票 (完美解决"装死"问题)
	// -------------------------------------------------------------------------
	if sumFaultyRep >= DecisionThreshold*sumTotalBSGRep {
		fmt.Printf("[BSG Leader] ⚖️ QUORUM REACHED: Shard %d is FAULTY. Action triggered!\n", targetID)

		r.decisionMade[targetID] = true

		// 向 L1 汇报最终结果
		r.reportSupervisorFailure("BSG Quorum Reached: Faulty", targetID)

		// 清理投票箱
		delete(r.voteBox, targetID)

	} else if sumNormalRep >= DecisionThreshold*sumTotalBSGRep {
		fmt.Printf("[BSG Leader] ⚖️ QUORUM REACHED: Shard %d is NORMAL. False alarm cleared.\n", targetID)

		r.decisionMade[targetID] = true
		// 目标正常，无需向 L1 上报接管，直接清理内存即可
		delete(r.voteBox, targetID)
	}
}
func (r *ReputationSupervisionMod) getBSGMembers(targetID uint64) []uint64 {

	return r.fullBSGMap[targetID]
}
func (r *ReputationSupervisionMod) reportSupervisorFailure(reason string, targetID uint64) {
	report := message.SupervisionResultMsg{
		ReporterShardID: r.pbftNode.ShardID,
		TargetShardID:   targetID,
		Epoch:           int(r.pbftNode.currentEpoch.Load()),
		SeqID:           r.pbftNode.sequenceID,
		IsFaulty:        true,
		Reason:          reason,
		ReporterNode:    r.pbftNode.RunningNode,
	}
	bytes, _ := json.Marshal(report)
	msg := message.MergeMessage(message.CSupervisionRes, bytes)
	networks.TcpDial(msg, params.SupervisorAddr)
}
