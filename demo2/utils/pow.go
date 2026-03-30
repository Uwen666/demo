package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// 定义 POW 难度 (前导零的位数，或目标阈值)
// 为了演示，这里设置较低难度，比如前 4 位是 0
const Difficulty = 4

// 计算 Hash
// 数据源: RandomSeed + NodeID (模拟公钥) + Nonce
func CalculateHash(seed int64, nodeID uint64, nonce int) string {
	record := strconv.FormatInt(seed, 10) + strconv.FormatUint(nodeID, 10) + strconv.Itoa(nonce)
	h := sha256.New()
	h.Write([]byte(record))
	hashed := h.Sum(nil)
	return hex.EncodeToString(hashed)
}

// 验证是否满足难度要求
func IsValidHash(hash string, difficulty int) bool {
	prefix := strings.Repeat("0", difficulty)
	return strings.HasPrefix(hash, prefix)
}

// 挖矿 (Solve Puzzle)
func SolvePuzzle(seed int64, nodeID uint64) int {
	var nonce int
	// 简单的暴力穷举
	for {
		hash := CalculateHash(seed, nodeID, nonce)
		if IsValidHash(hash, Difficulty) {
			// fmt.Printf("PoW Solved! Node: %d, Nonce: %d, Hash: %s\n", nodeID, nonce, hash)
			return nonce
		}
		nonce++
		// 防止死循环 (实际场景不需要)
		if nonce > 100000000 {
			nonce = 0 // 重置或报错
		}
	}
}

// 验证 (Verify Puzzle)
func VerifyPoW(seed int64, nodeID uint64, nonce int) bool {
	hash := CalculateHash(seed, nodeID, nonce)
	return IsValidHash(hash, Difficulty)
}
