package utils

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// VRFVerify 模拟 VRF 计算，判断节点是否被选中
// 输入: 私钥(此处用NodeID模拟), 种子(BlockHeight + Round), 阈值
// 输出: 是否选中
func VRFVerify(nodeID uint64, seed int64, threshold float64) bool {
	ratio := VRFHash(nodeID, seed)
	return ratio < threshold
}

// VRFHash 计算节点的 VRF 输出值 [0, 1)
// 输入: nodeID + seed → HMAC-SHA256 → 归一化到 [0,1)
func VRFHash(nodeID uint64, seed int64) float64 {
	input := make([]byte, 16)
	binary.BigEndian.PutUint64(input[0:8], nodeID)
	binary.BigEndian.PutUint64(input[8:16], uint64(seed))

	h := hmac.New(sha256.New, input)
	h.Write(input)
	hash := h.Sum(nil)

	hashVal := binary.BigEndian.Uint64(hash[:8])
	maxVal := ^uint64(0)
	return float64(hashVal) / float64(maxVal)
}

// VRFHashStr 使用字符串标识(如IP地址)计算 VRF 输出值 [0, 1)
func VRFHashStr(identifier string, seed int64) float64 {
	seedBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seedBytes, uint64(seed))

	h := hmac.New(sha256.New, []byte(identifier))
	h.Write(seedBytes)
	hash := h.Sum(nil)

	hashVal := binary.BigEndian.Uint64(hash[:8])
	maxVal := ^uint64(0)
	return float64(hashVal) / float64(maxVal)
}

// NodeVRFResult 用于VRF分片分配的结果
type NodeVRFResult struct {
	IP       string
	VRFValue float64
}

// VRFShardAssignment 基于 VRF 将一组节点分配到分片
// 方案: 每个节点使用 epoch randomness + 节点标识 → VRF → 随机数
//
//	将随机数排序后均匀分配到各分片
//	每个分片内 VRF 值最小的节点为初始 leader (NodeID=0)
//
// 返回: newIPTable[shardID][nodeID] = IP
func VRFShardAssignment(activeNodeIPs []string, epochSeed int64, shardNum int) map[uint64]map[uint64]string {
	if len(activeNodeIPs) == 0 || shardNum == 0 {
		return make(map[uint64]map[uint64]string)
	}

	// 1. 计算每个节点的 VRF 值
	results := make([]NodeVRFResult, len(activeNodeIPs))
	for i, ip := range activeNodeIPs {
		results[i] = NodeVRFResult{
			IP:       ip,
			VRFValue: VRFHashStr(ip, epochSeed),
		}
	}

	// 2. 按 VRF 值排序 (确定性排列)
	sort.Slice(results, func(i, j int) bool {
		return results[i].VRFValue < results[j].VRFValue
	})

	// 3. 均匀分配到各分片 (每个分片至少 1 个节点)
	newIPTable := make(map[uint64]map[uint64]string)
	for s := 0; s < shardNum; s++ {
		newIPTable[uint64(s)] = make(map[uint64]string)
	}

	for i, r := range results {
		shardID := uint64(i % shardNum)
		nodeID := uint64(i / shardNum) // 分片内的序号
		newIPTable[shardID][nodeID] = r.IP
	}

	return newIPTable
}
