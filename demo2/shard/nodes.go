// definition of node and shard

package shard

import (
	"blockEmulator/params"
	"fmt"
)

type Node struct {
	NodeID  uint64
	ShardID uint64
	IPaddr  string

	NodeType               int
	Reputation             float64
	SuperviseTargetShardID uint64
	SupervisedByShardID    uint64
	BackupTargetShardIDs   []uint64
}

func (n *Node) PrintNode() {
	v := []interface{}{
		n.NodeID,
		n.ShardID,
		n.IPaddr,
	}
	fmt.Printf("%v\n", v)
}

const (
	NodeType_Consensus = 0
	NodeType_Storage   = 1
)

func (n *Node) UpdateReputation(P, S, E, C_total float64) {
	rho := params.ReputationRho
	w_id := 1.0 // 身份权重 (假设同质)

	// 权重配置
	w_p := 1.0  // 正常
	w_s := -0.5 // 不响应
	w_e := -2.0 // 错误

	score := (w_p*P + w_s*S + w_e*E) / C_total
	if C_total == 0 {
		score = 0
	}

	// 更新
	n.Reputation = rho*n.Reputation + (1-rho)*w_id*score

	// 边界限制
	if n.Reputation > 1.0 {
		n.Reputation = 1.0
	}
	if n.Reputation < 0.0 {
		n.Reputation = 0.0
	}
}
