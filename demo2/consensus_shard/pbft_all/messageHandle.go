package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// this func is only invoked by main node
func (p *PbftConsensusNode) Propose() {
	if p.RunningNode.NodeType == shard.NodeType_Storage {
		p.pl.Plog.Printf("S%dN%d : storage/standby role, waiting for potential activation by reconfiguration\n", p.ShardID, p.NodeID)
		fmt.Printf("[S%d N%d] Storage/standby node started. Waiting for activation by CReconfig.\n", p.ShardID, p.NodeID)
		for {
			if p.stopSignal.Load() {
				return
			}
			if p.RunningNode.NodeType != shard.NodeType_Storage {
				p.pl.Plog.Printf("S%dN%d : role switched to consensus, starting proposer loop\n", p.ShardID, p.NodeID)
				break
			}
			time.Sleep(time.Second)
		}
	}

	// wait other nodes to start TCPlistening, sleep 8 sec.
	time.Sleep(8 * time.Second)

	nextRoundBeginSignal := make(chan bool)

	go func() {
		// go into the next round
		for {
			if p.stopSignal.Load() {
				return
			}
			time.Sleep(time.Duration(int64(p.pbftChainConfig.BlockInterval)) * time.Millisecond)
			if p.stopSignal.Load() {
				return
			}
			// send a signal to another GO-Routine. It will block until a GO-Routine try to fetch data from this channel.
			for p.pbftStage.Load() != 1 {
				if p.stopSignal.Load() {
					return
				}
				time.Sleep(time.Millisecond * 100)
			}
			// 非阻塞发送：如果主循环已退出（没有人读 channel），避免永远阻塞
			select {
			case nextRoundBeginSignal <- true:
			default:
				// 主循环已不再接收信号，检查是否应退出
				if p.stopSignal.Load() {
					return
				}
			}
		}
	}()

	go func() {
		// check whether to view change
		for {
			if p.stopSignal.Load() {
				return
			}
			time.Sleep(time.Second)
			if p.stopSignal.Load() {
				return
			}
			if time.Now().UnixMilli()-p.lastCommitTime.Load() > int64(params.PbftViewChangeTimeOut) {
				p.lastCommitTime.Store(time.Now().UnixMilli())
				go p.viewChangePropose()
			}
		}
	}()

	for {
		select {
		case <-nextRoundBeginSignal:
			go func() {
				if p.node_nums == 0 {
					return
				}
				leaderID := uint64(p.view.Load()) % p.node_nums
				// if this node is not leader, do not propose.
				if leaderID != p.NodeID {
					return
				}

				p.sequenceLock.Lock()
				p.pl.Plog.Printf("S%dN%d get sequenceLock locked, now trying to propose...\n", p.ShardID, p.NodeID)
				// propose
				// implement interface to generate propose
				_, r := p.ihm.HandleinPropose()

				digest := getDigest(r)
				p.requestPool[string(digest)] = r
				p.pl.Plog.Printf("S%dN%d put the request into the pool ...\n", p.ShardID, p.NodeID)

				p.height2Digest[p.sequenceID] = string(digest)

				// Plan A: Malicious leader tampering
				isMalicious := p.ShardID == params.MaliciousShardID && p.NodeID == params.MaliciousNodeID
				shouldTamper := isMalicious && rand.Float64() < params.MaliciousProbability

				if shouldTamper {
					// Tamper with digest and broadcast to other nodes
					tamperedDigest := make([]byte, len(digest))
					copy(tamperedDigest, digest)
					tamperedDigest[0] ^= 0xFF
					fmt.Printf("[MALICIOUS] S%dN%d: Tampering block digest at seq %d\n", p.ShardID, p.NodeID, p.sequenceID)

					tamperedPP := message.PrePrepare{
						RequestMsg: r,
						Digest:     tamperedDigest,
						SeqID:      p.sequenceID,
					}
					ppbyte, err := json.Marshal(tamperedPP)
					if err != nil {
						log.Panic()
					}
					msg_send := message.MergeMessage(message.CPrePrepare, ppbyte)
					networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)

					// Send correct version to self (local state stays consistent)
					correctPP := message.PrePrepare{
						RequestMsg: r,
						Digest:     digest,
						SeqID:      p.sequenceID,
					}
					correctBytes, err := json.Marshal(correctPP)
					if err != nil {
						log.Panic()
					}
					correctMsg := message.MergeMessage(message.CPrePrepare, correctBytes)
					networks.TcpDial(correctMsg, p.RunningNode.IPaddr)
				} else {
					ppmsg := message.PrePrepare{
						RequestMsg: r,
						Digest:     digest,
						SeqID:      p.sequenceID,
					}
					ppbyte, err := json.Marshal(ppmsg)
					if err != nil {
						log.Panic()
					}
					msg_send := message.MergeMessage(message.CPrePrepare, ppbyte)
					networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
					networks.TcpDial(msg_send, p.RunningNode.IPaddr)
				}
				p.pbftStage.Store(2)
			}()

		case <-p.pStop:
			p.pl.Plog.Printf("S%dN%d get stopSignal in Propose Routine, now stop...\n", p.ShardID, p.NodeID)
			return
		}
	}
}

// Handle pre-prepare messages here.
// If you want to do more operations in the pre-prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrePrepare**
func (p *PbftConsensusNode) handlePrePrepare(content []byte) {
	p.RunningNode.PrintNode()
	fmt.Println("received the PrePrepare ...")
	// decode the message
	ppmsg := new(message.PrePrepare)
	err := json.Unmarshal(content, ppmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 1 && ppmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if ppmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	flag := false
	if digest := getDigest(ppmsg.RequestMsg); string(digest) != string(ppmsg.Digest) {
		p.pl.Plog.Printf("S%dN%d : the digest is not consistent, so refuse to prepare. \n", p.ShardID, p.NodeID)
		// Report malicious behavior: only the next-in-line honest node reports to avoid duplication
		leaderID := uint64(p.view.Load()) % p.node_nums
		reporterID := (leaderID + 1) % p.node_nums
		if p.NodeID == reporterID {
			leaderIP := ""
			if ip, ok := p.ip_nodeTable[p.ShardID][leaderID]; ok {
				leaderIP = ip
			}
			faultyNodeID := leaderID
			report := message.SupervisionResultMsg{
				ReporterShardID: p.ShardID,
				TargetShardID:   p.ShardID,
				Epoch:           int(p.currentEpoch.Load()),
				SeqID:           ppmsg.SeqID,
				IsFaulty:        true,
				Reason:          fmt.Sprintf("Consensus Violation: Invalid Block Digest from Leader %d", leaderID),
				FaultyNodeID:    &faultyNodeID,
				FaultyNodeIP:    leaderIP,
				ReporterNode:    p.RunningNode,
			}
			reportBytes, _ := json.Marshal(report)
			reportMsg := message.MergeMessage(message.CSupervisionRes, reportBytes)
			networks.TcpDial(reportMsg, params.SupervisorAddr)
			fmt.Printf("[HONEST] S%dN%d: Reported malicious leader %d to Supervisor\n", p.ShardID, p.NodeID, leaderID)
		}
	} else if p.sequenceID < ppmsg.SeqID {
		p.requestPool[string(getDigest(ppmsg.RequestMsg))] = ppmsg.RequestMsg
		p.height2Digest[ppmsg.SeqID] = string(getDigest(ppmsg.RequestMsg))
		p.pl.Plog.Printf("S%dN%d : the Sequence id is not consistent, so refuse to prepare. \n", p.ShardID, p.NodeID)
	} else {
		// do your operation in this interface
		flag = p.ihm.HandleinPrePrepare(ppmsg)
		if flag {
			p.requestPool[string(getDigest(ppmsg.RequestMsg))] = ppmsg.RequestMsg
			p.height2Digest[ppmsg.SeqID] = string(getDigest(ppmsg.RequestMsg))
		}
	}
	// if the message is true, broadcast the prepare message
	if flag {
		digest := ppmsg.Digest

		// Plan B: Malicious voter — any CS non-leader node may randomly send wrong digest
		leaderID := uint64(0)
		if p.node_nums > 0 {
			leaderID = uint64(p.view.Load()) % p.node_nums
		}
		isLeader := p.NodeID == leaderID
		isMaliciousVoter := !isLeader &&
			params.MaliciousVoterProb > 0 &&
			p.RunningNode.NodeType != shard.NodeType_Storage &&
			rand.Float64() < params.MaliciousVoterProb
		if isMaliciousVoter {
			tamperedDigest := make([]byte, len(digest))
			copy(tamperedDigest, digest)
			tamperedDigest[0] ^= 0xFF
			digest = tamperedDigest
			fmt.Printf("[WRONG-VOTE] S%dN%d: Casting wrong vote (tampered Prepare digest) at seq %d\n",
				p.ShardID, p.NodeID, ppmsg.SeqID)
		}

		pre := message.Prepare{
			Digest:     digest,
			SeqID:      ppmsg.SeqID,
			SenderNode: p.RunningNode,
		}
		prepareByte, err := json.Marshal(pre)
		if err != nil {
			log.Panic()
		}
		// broadcast
		msg_send := message.MergeMessage(message.CPrepare, prepareByte)
		networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
		networks.TcpDial(msg_send, p.RunningNode.IPaddr)
		p.pl.Plog.Printf("S%dN%d : has broadcast the prepare message \n", p.ShardID, p.NodeID)

		// Pbft stage add 1. It means that this round of pbft goes into the next stage, i.e., Prepare stage.
		p.pbftStage.Add(1)
	}
}

// Handle prepare messages here.
// If you want to do more operations in the prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrepare**
func (p *PbftConsensusNode) handlePrepare(content []byte) {
	p.pl.Plog.Printf("S%dN%d : received the Prepare ...\n", p.ShardID, p.NodeID)
	// decode the message
	pmsg := new(message.Prepare)
	err := json.Unmarshal(content, pmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 2 && pmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if pmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	if _, ok := p.requestPool[string(pmsg.Digest)]; !ok {
		p.pl.Plog.Printf("S%dN%d : doesn't have the digest in the requst pool, refuse to commit\n", p.ShardID, p.NodeID)

		// Plan B detection: report the wrong voter to Supervisor
		leaderID := uint64(0)
		if p.node_nums > 0 {
			leaderID = uint64(p.view.Load()) % p.node_nums
		}
		if pmsg.SenderNode.ShardID == p.ShardID && pmsg.SenderNode.NodeID != leaderID {
			// Only one honest node reports (the first non-leader, non-sender node)
			reporterID := (pmsg.SenderNode.NodeID + 1) % p.node_nums
			if reporterID == leaderID {
				reporterID = (reporterID + 1) % p.node_nums
			}
			if p.NodeID == reporterID {
				faultyIP := ""
				if ip, ok := p.ip_nodeTable[p.ShardID][pmsg.SenderNode.NodeID]; ok {
					faultyIP = ip
				}
				faultyNodeID := pmsg.SenderNode.NodeID
				report := message.SupervisionResultMsg{
					ReporterShardID: p.ShardID,
					TargetShardID:   p.ShardID,
					Epoch:           int(p.currentEpoch.Load()),
					SeqID:           pmsg.SeqID,
					IsFaulty:        true,
					Reason:          fmt.Sprintf("Wrong Vote: Node %d sent invalid Prepare digest", pmsg.SenderNode.NodeID),
					FaultyNodeID:    &faultyNodeID,
					FaultyNodeIP:    faultyIP,
					ReporterNode:    p.RunningNode,
				}
				reportBytes, _ := json.Marshal(report)
				reportMsg := message.MergeMessage(message.CSupervisionRes, reportBytes)
				networks.TcpDial(reportMsg, params.SupervisorAddr)
				fmt.Printf("[DETECT-WRONG-VOTE] S%dN%d: Reported Node %d wrong vote to Supervisor\n",
					p.ShardID, p.NodeID, pmsg.SenderNode.NodeID)
			}
		}
	} else if p.sequenceID < pmsg.SeqID {
		p.pl.Plog.Printf("S%dN%d : inconsistent sequence ID, refuse to commit\n", p.ShardID, p.NodeID)
	} else {
		// if needed more operations, implement interfaces
		p.ihm.HandleinPrepare(pmsg)

		p.set2DMap(true, string(pmsg.Digest), pmsg.SenderNode)
		cnt := len(p.cntPrepareConfirm[string(pmsg.Digest)])

		// if the node has received 2f messages (itself included), and it haven't committed, then it commit
		p.lock.Lock()
		defer p.lock.Unlock()
		if uint64(cnt) >= p.quorumThreshold() && !p.isCommitBordcast[string(pmsg.Digest)] {
			p.pl.Plog.Printf("S%dN%d : is going to commit\n", p.ShardID, p.NodeID)
			// generate commit and broadcast
			c := message.Commit{
				Digest:     pmsg.Digest,
				SeqID:      pmsg.SeqID,
				SenderNode: p.RunningNode,
			}
			commitByte, err := json.Marshal(c)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CCommit, commitByte)
			networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
			networks.TcpDial(msg_send, p.RunningNode.IPaddr)
			p.isCommitBordcast[string(pmsg.Digest)] = true
			p.pl.Plog.Printf("S%dN%d : commit is broadcast\n", p.ShardID, p.NodeID)

			p.pbftStage.Add(1)
		}
	}
}

// Handle commit messages here.
// If you want to do more operations in the commit stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinCommit**
func (p *PbftConsensusNode) handleCommit(content []byte) {
	// decode the message
	cmsg := new(message.Commit)
	err := json.Unmarshal(content, cmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 3 && cmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	if cmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	p.pl.Plog.Printf("S%dN%d received the Commit from ...%d\n", p.ShardID, p.NodeID, cmsg.SenderNode.NodeID)
	p.set2DMap(false, string(cmsg.Digest), cmsg.SenderNode)
	cnt := len(p.cntCommitConfirm[string(cmsg.Digest)])

	p.lock.Lock()
	defer p.lock.Unlock()

	if uint64(cnt) >= p.quorumThreshold() && !p.isReply[string(cmsg.Digest)] {
		p.pl.Plog.Printf("S%dN%d : has received 2f + 1 commits ... \n", p.ShardID, p.NodeID)
		leaderID := uint64(0)
		if p.node_nums > 0 {
			leaderID = uint64(p.view.Load()) % p.node_nums
		}
		// if this node is left behind, so it need to requst blocks
		if _, ok := p.requestPool[string(cmsg.Digest)]; !ok {
			p.isReply[string(cmsg.Digest)] = true
			p.askForLock.Lock()
			// request the block
			sn := &shard.Node{
				NodeID:  leaderID,
				ShardID: p.ShardID,
				IPaddr:  p.ip_nodeTable[p.ShardID][leaderID],
			}
			orequest := message.RequestOldMessage{
				SeqStartHeight: p.sequenceID + 1,
				SeqEndHeight:   cmsg.SeqID,
				ServerNode:     sn,
				SenderNode:     p.RunningNode,
			}
			bromyte, err := json.Marshal(orequest)
			if err != nil {
				log.Panic()
			}

			p.pl.Plog.Printf("S%dN%d : is now requesting message (seq %d to %d) ... \n", p.ShardID, p.NodeID, orequest.SeqStartHeight, orequest.SeqEndHeight)
			msg_send := message.MergeMessage(message.CRequestOldrequest, bromyte)
			networks.TcpDial(msg_send, orequest.ServerNode.IPaddr)
		} else {
			// implement interface
			p.ihm.HandleinCommit(cmsg)
			p.isReply[string(cmsg.Digest)] = true
			p.pl.Plog.Printf("S%dN%d: this round of pbft %d is end \n", p.ShardID, p.NodeID, p.sequenceID)
			if p.NodeID == leaderID {
				// 使用 map 去重，防止 Leader 被重复计入
				voterSet := make(map[string]bool)

				// 1. 遍历 PBFT 底层真实的投票池
				commitMap := p.cntCommitConfirm[string(cmsg.Digest)]
				for nodePtr, hasVoted := range commitMap {
					if hasVoted {
						if ip, ok := p.ip_nodeTable[p.ShardID][nodePtr.NodeID]; ok {
							voterSet[ip] = true
						}
					}
				}

				// 2. 把 Leader 自己也加进去（Leader 肯定干活了）
				if myIP, ok := p.ip_nodeTable[p.ShardID][p.NodeID]; ok {
					voterSet[myIP] = true
				}

				activeVoters := make([]string, 0, len(voterSet))
				for ip := range voterSet {
					activeVoters = append(activeVoters, ip)
				}

				// 3. 构造专门用于汇报选票的 BlockInfoMsg
				voterBim := message.BlockInfoMsg{
					BlockBodyLength: -1, // 【关键暗号】用 -1 作为特殊标记，与正常区块区分
					BlockHeight:     cmsg.SeqID,
					Epoch:           int(p.currentEpoch.Load()),
					SenderShardID:   p.ShardID, // 如果您在 message.go 里改成了 ShardID，这里就用 ShardID: p.ShardID
					Voters:          activeVoters,
				}
				voterBytes, _ := json.Marshal(voterBim)
				sendMsg := message.MergeMessage(message.CBlockInfo, voterBytes)
				networks.TcpDial(sendMsg, params.SupervisorAddr)

				// Notify the supervising shard about this block commit so it can
				// stop the monitorTimer and avoid false-positive supervision reports.
				supervisedBy := p.RunningNode.SupervisedByShardID
				if supervisedBy != params.NoShardID {
					// SupervisedByShardID has been set by supervision assignment
					if supervisedBy != p.ShardID {
						if nodes, ok := p.ip_nodeTable[supervisedBy]; ok {
							for _, ip := range nodes {
								go networks.TcpDial(sendMsg, ip)
							}
						}
					}
				}
			}
			p.sequenceID += 1
		}

		p.pbftStage.Store(1)
		p.lastCommitTime.Store(time.Now().UnixMilli())

		// if this node is a main node, then unlock the sequencelock
		if p.NodeID == leaderID {
			p.sequenceLock.Unlock()
			p.pl.Plog.Printf("S%dN%d get sequenceLock unlocked...\n", p.ShardID, p.NodeID)
		}
	}
}

// this func is only invoked by the main node,
// if the request is correct, the main node will send
// block back to the message sender.
// now this function can send both block and partition
func (p *PbftConsensusNode) handleRequestOldSeq(content []byte) {
	if p.node_nums == 0 {
		return
	}
	leaderID := uint64(p.view.Load()) % p.node_nums
	if leaderID != p.NodeID {
		return
	}

	rom := new(message.RequestOldMessage)
	err := json.Unmarshal(content, rom)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : received the old message requst from ...", p.ShardID, p.NodeID)
	rom.SenderNode.PrintNode()

	oldR := make([]*message.Request, 0)
	for height := rom.SeqStartHeight; height <= rom.SeqEndHeight; height++ {
		if _, ok := p.height2Digest[height]; !ok {
			p.pl.Plog.Printf("S%dN%d : has no this digest to this height %d\n", p.ShardID, p.NodeID, height)
			break
		}
		if r, ok := p.requestPool[p.height2Digest[height]]; !ok {
			p.pl.Plog.Printf("S%dN%d : has no this message to this digest %d\n", p.ShardID, p.NodeID, height)
			break
		} else {
			oldR = append(oldR, r)
		}
	}
	p.pl.Plog.Printf("S%dN%d : has generated the message to be sent\n", p.ShardID, p.NodeID)

	p.ihm.HandleReqestforOldSeq(rom)

	// send the block back
	sb := message.SendOldMessage{
		SeqStartHeight: rom.SeqStartHeight,
		SeqEndHeight:   rom.SeqEndHeight,
		OldRequest:     oldR,
		SenderNode:     p.RunningNode,
	}
	sbByte, err := json.Marshal(sb)
	if err != nil {
		log.Panic()
	}
	msg_send := message.MergeMessage(message.CSendOldrequest, sbByte)
	networks.TcpDial(msg_send, rom.SenderNode.IPaddr)
	p.pl.Plog.Printf("S%dN%d : send blocks\n", p.ShardID, p.NodeID)
}

// node requst blocks and receive blocks from the main node
func (p *PbftConsensusNode) handleSendOldSeq(content []byte) {
	som := new(message.SendOldMessage)
	err := json.Unmarshal(content, som)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : has received the SendOldMessage message\n", p.ShardID, p.NodeID)

	// implement interface for new consensus
	p.ihm.HandleforSequentialRequest(som)
	beginSeq := som.SeqStartHeight
	for idx, r := range som.OldRequest {
		p.requestPool[string(getDigest(r))] = r
		p.height2Digest[uint64(idx)+beginSeq] = string(getDigest(r))
		p.isReply[string(getDigest(r))] = true
		p.pl.Plog.Printf("this round of pbft %d is end \n", uint64(idx)+beginSeq)
	}
	p.sequenceID = som.SeqEndHeight + 1
	if rDigest, ok1 := p.height2Digest[p.sequenceID]; ok1 {
		if r, ok2 := p.requestPool[rDigest]; ok2 {
			ppmsg := &message.PrePrepare{
				RequestMsg: r,
				SeqID:      p.sequenceID,
				Digest:     getDigest(r),
			}
			flag := false
			flag = p.ihm.HandleinPrePrepare(ppmsg)
			if flag {
				pre := message.Prepare{
					Digest:     ppmsg.Digest,
					SeqID:      ppmsg.SeqID,
					SenderNode: p.RunningNode,
				}
				prepareByte, err := json.Marshal(pre)
				if err != nil {
					log.Panic()
				}
				// broadcast
				msg_send := message.MergeMessage(message.CPrepare, prepareByte)
				networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
				p.pl.Plog.Printf("S%dN%d : has broadcast the prepare message \n", p.ShardID, p.NodeID)
			}
		}
	}

	p.askForLock.Unlock()
}
