package params

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

var (
	// The following parameters can be set in main.go.
	// default values:
	NodesInShard = 4  // \# of Nodes in a shard.
	ShardNum     = 20 // \# of shards.
)

// consensus layer & output file path
var (
	ConsensusMethod = 0 // ConsensusMethod an Integer, which indicates the choice ID of methods / consensuses. Value range: [0, 4), representing [CLPA_Broker, CLPA, Broker, Relay]"

	PbftViewChangeTimeOut = 10000 // The view change threshold of pbft. If the process of PBFT is too slow, the view change mechanism will be triggered.

	Block_Interval = 4000 // The time interval for generating a new block

	MaxBlockSize_global = 1000  // The maximum number of transactions a block contains
	BlocksizeInBytes    = 20000 // The maximum size (in bytes) of block body
	UseBlocksizeInBytes = 0     // Use blocksizeInBytes as the blocksize measurement if '1'.

	InjectSpeed   = 2000   // The speed of transaction injection
	TotalDataSize = 300000 // The total number of txs to be injected
	TxBatchSize   = 16000  // The supervisor read a batch of txs then send them. The size of a batch is 'TxBatchSize'

	BrokerNum            = 10 // The # of Broker accounts used in Broker / CLPA_Broker.
	RelayWithMerkleProof = 0  // When using a consensus about "Relay", nodes will send Tx Relay with proof if "RelayWithMerkleProof" = 1

	ExpDataRootDir     = "expTest"                     // The root dir where the experimental data should locate.
	DataWrite_path     = ExpDataRootDir + "/result/"   // Measurement data result output path
	LogWrite_path      = ExpDataRootDir + "/log"       // Log output path
	DatabaseWrite_path = ExpDataRootDir + "/database/" // database write path

	SupervisorAddr = "127.0.0.1:18800"        // Supervisor ip address
	DatasetFile    = `./selectedTxs_300K.csv` // The raw BlockTransaction data path

	ReconfigTimeGap = 50 // The time gap between epochs. This variable is only used in CLPA / CLPA_Broker now.
)

// network layer
var (
	Delay       int // The delay of network (ms) when sending. 0 if delay < 0
	JitterRange int // The jitter range of delay (ms). Jitter follows a uniform distribution. 0 if JitterRange < 0.
	Bandwidth   int // The bandwidth limit (Bytes). +inf if bandwidth < 0
)

// read from file
type globalConfig struct {
	ConsensusMethod int `json:"ConsensusMethod"`

	PbftViewChangeTimeOut int `json:"PbftViewChangeTimeOut"`

	ExpDataRootDir string `json:"ExpDataRootDir"`

	BlockInterval int `json:"Block_Interval"`

	BlocksizeInBytes    int `json:"BlocksizeInBytes"`
	MaxBlockSizeGlobal  int `json:"BlockSize"`
	UseBlocksizeInBytes int `json:"UseBlocksizeInBytes"`

	InjectSpeed   int `json:"InjectSpeed"`
	TotalDataSize int `json:"TotalDataSize"`

	TxBatchSize          int    `json:"TxBatchSize"`
	BrokerNum            int    `json:"BrokerNum"`
	RelayWithMerkleProof int    `json:"RelayWithMerkleProof"`
	DatasetFile          string `json:"DatasetFile"`
	ReconfigTimeGap      int    `json:"ReconfigTimeGap"`

	Delay       int `json:"Delay"`
	JitterRange int `json:"JitterRange"`
	Bandwidth   int `json:"Bandwidth"`
}

func ReadConfigFile() {
	// read configurations from paramsConfig.json
	data, err := os.ReadFile("paramsConfig.json")
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	var config globalConfig
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	// output configurations
	fmt.Printf("Config: %+v\n", config)

	// set configurations to params
	// consensus params
	ConsensusMethod = config.ConsensusMethod

	PbftViewChangeTimeOut = config.PbftViewChangeTimeOut

	// data file params
	ExpDataRootDir = config.ExpDataRootDir
	DataWrite_path = ExpDataRootDir + "/result/"
	LogWrite_path = ExpDataRootDir + "/log"
	DatabaseWrite_path = ExpDataRootDir + "/database/"

	Block_Interval = config.BlockInterval

	MaxBlockSize_global = config.MaxBlockSizeGlobal
	BlocksizeInBytes = config.BlocksizeInBytes
	UseBlocksizeInBytes = config.UseBlocksizeInBytes

	InjectSpeed = config.InjectSpeed
	TotalDataSize = config.TotalDataSize
	TxBatchSize = config.TxBatchSize

	BrokerNum = config.BrokerNum
	RelayWithMerkleProof = config.RelayWithMerkleProof
	DatasetFile = config.DatasetFile

	ReconfigTimeGap = config.ReconfigTimeGap

	// network params
	Delay = config.Delay
	JitterRange = config.JitterRange
	Bandwidth = config.Bandwidth

}

var (
	ConsensusMethod_Reputation = 4

	ReputationRho       = 0.5
	HealthFactorBase    = 3000 // C_target: 理想队列长度，约为 MaxBlockSize 的 3 倍
	SupervisionTimeout  = 15000
	ReputationThreshold = 0.5 // 驱逐阈值：声誉低于此值的节点被驱逐
	TakeoverThreshold   = 1.5

	NodeType_Consensus = 0
	NodeType_Storage   = 1

	// Plan A: Malicious node simulation
	MaliciousShardID     = uint64(2) // Which shard has the malicious node
	MaliciousNodeID      = uint64(0) // Which node is malicious (leader at view 0)
	MaliciousProbability = 0.0       // Probability of malicious behavior per block (0.0 = disabled)

	// Plan B: Malicious voter simulation (wrong vote in Prepare/Commit phase)
	// 任意共识节点都可能随机投错票，概率由 MaliciousVoterProb 控制。
	// 重新打开恶意投票注入，便于在当前实验中观察错误投票对监督/接管路径的影响。
	MaliciousVoterProb = 0.01

	// TB Failure Injection: simulate TB validation failure to trigger takeover
	TBFailureProbability = 0.0 // Disabled: was 0.05. Re-enable to test TB failure → takeover path.
	NoShardID            = ^uint64(0)
)

// ConsensusShardCount returns the number of consensus shards (first half).
func ConsensusShardCount() int {
	if CommitteeMethod[ConsensusMethod] == "Reputation" && ShardNum >= 2 {
		return ShardNum / 2
	}
	return ShardNum
}

// IsConsensusShardForReputation returns whether a shard belongs to the
// consensus-shard set. The first half shards are CS, and the second half are SS.
func IsConsensusShardForReputation(shardID, shardNums uint64) bool {
	if shardNums < 2 {
		return true
	}
	half := shardNums / 2
	if half == 0 {
		return true
	}
	return shardID < half
}

// GetPairedStorageShardForReputation maps CS_m -> SS_m.
func GetPairedStorageShardForReputation(consensusShardID, shardNums uint64) uint64 {
	if shardNums < 2 {
		return consensusShardID
	}
	half := shardNums / 2
	if half == 0 {
		return consensusShardID
	}
	if consensusShardID < half {
		return consensusShardID + half
	}
	return consensusShardID
}

// GetNodeTypeForReputation assigns node role by shard role. All nodes in CS
// shards are consensus nodes; all nodes in SS shards are storage nodes.
func GetNodeTypeForReputation(shardID, shardNums uint64) int {
	if IsConsensusShardForReputation(shardID, shardNums) {
		return NodeType_Consensus
	}
	return NodeType_Storage
}
