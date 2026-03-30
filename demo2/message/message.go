package message

import (
	"blockEmulator/core"
	"blockEmulator/shard"
	"time"
)

var prefixMSGtypeLen = 30

type MessageType string
type RequestType string

const (
	CPrePrepare        MessageType = "preprepare"
	CPrepare           MessageType = "prepare"
	CCommit            MessageType = "commit"
	CRequestOldrequest MessageType = "requestOldrequest"
	CSendOldrequest    MessageType = "sendOldrequest"
	CStop              MessageType = "stop"

	CRelay          MessageType = "relay"
	CRelayWithProof MessageType = "CRelay&Proof"
	CInject         MessageType = "inject"

	CBlockInfo MessageType = "BlockInfo"
	CSeqIDinfo MessageType = "SequenceID"

	CGetState            MessageType = "GetState"
	CReturnState         MessageType = "ReturnState"
	CUploadTB            MessageType = "UploadTB"
	CConfirmTB           MessageType = "ConfirmTB"
	CSupervisionRes      MessageType = "SupervisionRe"
	CTakeover            MessageType = "Takeover"
	CReconfig            MessageType = "Reconfig"
	CReconfigAck         MessageType = "ReconfigAck"
	CJoinRequest         MessageType = "JoinRequest"  // Node -> Supervisor: 提交 POW 答案
	CJoinResponse        MessageType = "JoinResponse" // Supervisor -> Node: 准入结果
	CTakeoverSupervision MessageType = "TakeoverSupervision"
	CTakeoverRouting     MessageType = "TakeoverRouting"   // Supervisor -> Node: takeover route override
	CVoteBSG             MessageType = "VoteBSG"           // BSG Member -> BSG Leader
	CTBSign              MessageType = "TBSign"            // Node -> Leader
	CSupervisionAssign   MessageType = "SupervisionAssign" // Supervisor -> Node: epoch 0 supervision assignment
	CEliminateNotice     MessageType = "EliminateNotice"   // Supervisor -> Node: 声誉不足被淘汰
	CStopMonitoring      MessageType = "StopMonitoring"    // Supervisor -> Node: 分片已被接管，停止监督
)

var (
	BlockRequest RequestType = "Block"
	// add more types
	// ...
)

type TimeBeacon struct {
	ShardID     uint64
	BlockHeight uint64
	BlockHash   []byte // 区块哈希
	StateRoot   []byte // 状态树根
	TxRoot      []byte // 交易树根
	ReceiptRoot []byte // 收据树根
	Timestamp   int64
}

type RawMessage struct {
	Content []byte // the content of raw message, txs and blocks (most cases) included
}

type Request struct {
	RequestType RequestType
	Msg         RawMessage // request message
	ReqTime     time.Time  // request time
}

type JoinRequestMsg struct {
	NodeID  uint64
	ShardID uint64
	Nonce   int // POW 答案
}

// 注册响应消息
type JoinResponseMsg struct {
	Success bool
	Message string
}

type PrePrepare struct {
	RequestMsg *Request // the request message should be pre-prepared
	Digest     []byte   // the digest of this request, which is the only identifier
	SeqID      uint64
}

type Prepare struct {
	Digest     []byte // To identify which request is prepared by this node
	SeqID      uint64
	SenderNode *shard.Node // To identify who send this message
}

type Commit struct {
	Digest     []byte // To identify which request is prepared by this node
	SeqID      uint64
	SenderNode *shard.Node // To identify who send this message
}

type Reply struct {
	MessageID  uint64
	SenderNode *shard.Node
	Result     bool
}

type RequestOldMessage struct {
	SeqStartHeight uint64
	SeqEndHeight   uint64
	ServerNode     *shard.Node // send this request to the server node
	SenderNode     *shard.Node
}

type SendOldMessage struct {
	SeqStartHeight uint64
	SeqEndHeight   uint64
	OldRequest     []*Request
	SenderNode     *shard.Node
}

type InjectTxs struct {
	Txs       []*core.Transaction
	ToShardID uint64
}

// data sent to the supervisor
type BlockInfoMsg struct {
	BlockBodyLength int
	BlockHeight     uint64
	InnerShardTxs   []*core.Transaction // txs which are innerShard
	Epoch           int

	ProposeTime   time.Time // record the propose time of this block (txs)
	CommitTime    time.Time // record the commit time of this block (txs)
	SenderShardID uint64
	TxPoolSize    int
	Voters        []string

	// for transaction relay
	Relay1Txs []*core.Transaction // relay1 transactions in chain first time
	Relay2Txs []*core.Transaction // relay2 transactions in chain second time

	// for broker
	Broker1Txs []*core.Transaction // cross transactions at first time by broker
	Broker2Txs []*core.Transaction // cross transactions at second time by broker
}

type SeqIDinfo struct {
	SenderShardID uint64
	SenderSeq     uint64
}

func MergeMessage(msgType MessageType, content []byte) []byte {
	b := make([]byte, prefixMSGtypeLen)
	for i, v := range []byte(msgType) {
		b[i] = v
	}
	merge := append(b, content...)
	return merge
}

func SplitMessage(message []byte) (MessageType, []byte) {
	msgTypeBytes := message[:prefixMSGtypeLen]
	msgType_pruned := make([]byte, 0)
	for _, v := range msgTypeBytes {
		if v != byte(0) {
			msgType_pruned = append(msgType_pruned, v)
		}
	}
	msgType := string(msgType_pruned)
	content := message[prefixMSGtypeLen:]
	return MessageType(msgType), content
}

type GetStateMsg struct {
	BlockHeight   uint64
	AccountList   []string
	SenderNode    uint64
	SenderShardID uint64
}

type ReturnStateMsg struct {
	BlockHeight uint64
	AccountList []*core.AccountState
	StateRoot   []byte
}

type UploadTBMsg struct {
	ShardID              uint64
	BlockHeight          uint64
	Timestamp            int64
	Signature            []byte
	TB                   TimeBeacon
	IsTakeoverCompletion bool   // 标记为接管完成 TB，跳过失败注入
	TakeoverMode         string // 接管模式: "Granted"(CS_l直接接管) 或 "BSG_Activated"(BSG组接管)
	// 聚合签名: Map[NodeID]Signature
	Signatures map[uint64][]byte
}

// TakeoverMsg 接管指令消息，包含目标分片 ID
type TakeoverMsg struct {
	TargetShardID uint64 // 被接管的失效分片 ID
	Mode          string // "Granted" 或 "BSG_Activated"
}

type TakeoverRoutingMsg struct {
	TargetShardID uint64
	Mode          string
	RouteIPs      []string
}

type SupervisionResultMsg struct {
	ReporterShardID uint64      // 报告者 (CS_l)
	TargetShardID   uint64      // 被举报者 (CS_m)
	Epoch           int         // 事件发生时 epoch
	SeqID           uint64      // 事件关联的共识轮次
	IsFaulty        bool        // 是否失效
	Reason          string      // 原因
	FaultyNodeID    *uint64     // 具体作恶节点 ID (可选)
	FaultyNodeIP    string      // 具体作恶节点 IP (可选, 空=全分片)
	CurrentPoolSize int         // CS_l 当前交易池长度 (用于负载计算)
	TargetPoolSize  int         // CS_m 积压交易数量
	ReporterNode    *shard.Node // 报告节点信息
}

type ReconfigInfo struct {
	Epoch      int
	NewShardID uint64
	NewNodeID  uint64
	Activate   bool

	// 新的全网节点表 (用于节点更新通信录)
	// Key: ShardID, Val: map[NodeID]IP
	NewIPTable map[uint64]map[uint64]string

	// 监督任务 (顺便下发，减少消息次数)
	MainTarget    uint64
	BackupTargets []uint64
	FullBSGMap    map[uint64][]uint64
	SupervisedBy  uint64
}

type ReconfigAckMsg struct {
	Epoch      int
	NodeIP     string
	NewShardID uint64
	NewNodeID  uint64
}

type BSGVoteMsg struct {
	VoterShardID    uint64  // 投票者分片 ID
	TargetShardID   uint64  // 被监督的目标分片 ID (CS_m)
	IsFaulty        bool    // 投票观点：是否认为目标失效
	VoterReputation float64 // 投票者的声誉值 (用于加权)
	Timestamp       int64
	// Signature []byte // 实际场景需签名防止伪造
}

type TBSignMsg struct {
	TB        TimeBeacon
	NodeID    uint64
	Signature []byte
	// VRF 证明 (用于 Leader 验证该节点是否有资格签名)
	VRFProof []byte
}

// Epoch 0 supervision assignment message
type SupervisionAssignMsg struct {
	MainTarget    uint64              // 主监督目标分片
	BackupTargets []uint64            // 备用监督目标
	FullBSGMap    map[uint64][]uint64 // BSG 拓扑
	SupervisedBy  uint64              // 谁监督我
}

// EliminateNoticeMsg 通知节点因声誉低于阈值被淘汰
type EliminateNoticeMsg struct {
	Epoch      int
	Reason     string
	Reputation float64
}
