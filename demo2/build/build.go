package build

import (
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor"
	"log"
	"time"
)

func initConfig(nid, nnm, sid, snm uint64) *params.ChainConfig {
	// Read the contents of ipTable.json
	ipMap := readIpTable("./ipTable.json")
	params.IPmap_nodeTable = ipMap
	params.SupervisorAddr = params.IPmap_nodeTable[params.SupervisorShard][0]

	// check the correctness of params
	if len(ipMap)-1 < int(snm) {
		log.Panicf("Input ShardNumber = %d, but only %d shards in ipTable.json.\n", snm, len(ipMap)-1)
	}
	for shardID := 0; shardID < len(ipMap)-1; shardID++ {
		if len(ipMap[uint64(shardID)]) < int(nnm) {
			log.Panicf("Input NodeNumber = %d, but only %d nodes in Shard %d.\n", nnm, len(ipMap[uint64(shardID)]), shardID)
		}
	}

	params.NodesInShard = int(nnm)
	params.ShardNum = int(snm)

	// init the network layer
	networks.InitNetworkTools()

	pcc := &params.ChainConfig{
		ChainID:        sid,
		NodeID:         nid,
		ShardID:        sid,
		Nodes_perShard: uint64(params.NodesInShard),
		ShardNums:      snm,
		BlockSize:      uint64(params.MaxBlockSize_global),
		BlockInterval:  uint64(params.Block_Interval),
		InjectSpeed:    uint64(params.InjectSpeed),
	}
	return pcc
}

func BuildSupervisor(nnm, snm uint64) {
	methodID := params.ConsensusMethod
	var measureMod []string
	if methodID == 0 || methodID == 2 {
		measureMod = params.MeasureBrokerMod
	} else {
		measureMod = params.MeasureRelayMod
	}
	measureMod = append(measureMod, "Tx_Details")
	measureMod = append(measureMod, "Takeover_Time")

	lsn := new(supervisor.Supervisor)
	lsn.NewSupervisor(params.SupervisorAddr, initConfig(123, nnm, 123, snm), params.CommitteeMethod[methodID], measureMod...)
	go lsn.TcpListen()
	time.Sleep(8000 * time.Millisecond)
	lsn.SupervisorTxHandling()
}

func BuildNewPbftNode(nid, nnm, sid, snm uint64) {
	methodID := params.ConsensusMethod
	worker := pbft_all.NewPbftNode(sid, nid, initConfig(nid, nnm, sid, snm), params.CommitteeMethod[methodID])
	if params.CommitteeMethod[methodID] == "Reputation" {
		numCS := uint64(params.ConsensusShardCount())
		if sid < numCS {
			log.Printf("[BUILD] Shard %d Node %d -> CONSENSUS shard (CS count=%d, total=%d)\n", sid, nid, numCS, snm)
		} else {
			log.Printf("[BUILD] Shard %d Node %d -> STORAGE shard (CS count=%d, total=%d)\n", sid, nid, numCS, snm)
		}
	}
	worker.PerformPoWAndJoin()
	go worker.TcpListen()
	worker.Propose()
}

// BuildStandbyPbftNode starts a candidate node process on an independent port.
// It is not part of CS/SS at boot and waits to be activated by CReconfig.
func BuildStandbyPbftNode(standbyID uint64, bindAddr string, nnm, snm uint64) {
	methodID := params.ConsensusMethod
	pcc := initConfig(standbyID, nnm, 0, snm)
	standbyShardID := snm + 1000 + standbyID
	pcc.ChainID = standbyShardID
	pcc.ShardID = standbyShardID
	pcc.NodeID = standbyID

	if _, ok := params.IPmap_nodeTable[standbyShardID]; !ok {
		params.IPmap_nodeTable[standbyShardID] = make(map[uint64]string)
	}
	params.IPmap_nodeTable[standbyShardID][standbyID] = bindAddr

	worker := pbft_all.NewPbftNode(standbyShardID, standbyID, pcc, params.CommitteeMethod[methodID])
	worker.RunningNode.IPaddr = bindAddr
	worker.RunningNode.NodeType = params.NodeType_Storage

	log.Printf("[BUILD] Standby candidate started: id=%d, bind=%s, pseudoShard=%d\n", standbyID, bindAddr, standbyShardID)
	go worker.TcpListen()
	worker.Propose()
}
