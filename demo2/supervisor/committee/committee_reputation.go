package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// ReputationCommitteeModule combines Relay-style transaction sending with
// periodic epoch reconfiguration for the reputation-based consensus mode.
type ReputationCommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int
	IpNodeTable  map[uint64]map[uint64]string
	sl           *supervisor_log.SupervisorLog
	Ss           *signal.StopSignal

	// Epoch management
	epochInterval int // seconds between epoch reconfiguration
	curEpoch      int32

	// Callback to trigger epoch reconfiguration on the supervisor.
	// Returns the updated IP node table after reconfiguration.
	epochReconfig func(epoch int) map[uint64]map[uint64]string
}

func NewReputationCommitteeModule(
	ipNodeTable map[uint64]map[uint64]string,
	ss *signal.StopSignal,
	slog *supervisor_log.SupervisorLog,
	csvFilePath string,
	dataNum, batchNum int,
	epochInterval int,
	epochReconfig func(epoch int) map[uint64]map[uint64]string,
) *ReputationCommitteeModule {
	return &ReputationCommitteeModule{
		csvPath:       csvFilePath,
		dataTotalNum:  dataNum,
		batchDataNum:  batchNum,
		nowDataNum:    0,
		IpNodeTable:   ipNodeTable,
		Ss:            ss,
		sl:            slog,
		epochInterval: epochInterval,
		curEpoch:      0,
		epochReconfig: epochReconfig,
	}
}

func (rcm *ReputationCommitteeModule) HandleOtherMessage([]byte) {}

func (rcm *ReputationCommitteeModule) txSending(txlist []*core.Transaction) {
	sendToShard := make(map[uint64][]*core.Transaction)
	numCS := uint64(params.ConsensusShardCount())

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			for sid := uint64(0); sid < numCS; sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, rcm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := uint64(utils.Addr2Shard(tx.Sender))
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

func (rcm *ReputationCommitteeModule) MsgSendingControl() {
	log.Println("[Reputation] MsgSendingControl: start reading and injecting transactions...")

	epochTimer := time.NewTimer(time.Duration(rcm.epochInterval) * time.Second)
	defer epochTimer.Stop()

	txfile, err := os.Open(rcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0)
	batchCount := 0

	for {
		// Check if epoch reconfiguration is due
		select {
		case <-epochTimer.C:
			newEpoch := atomic.AddInt32(&rcm.curEpoch, 1)
			log.Printf("[Reputation] Epoch %d: triggering reconfiguration...\n", newEpoch)
			newTable := rcm.epochReconfig(int(newEpoch))
			if newTable != nil {
				rcm.IpNodeTable = newTable
			}
			log.Printf("[Reputation] Epoch %d: reconfiguration complete.\n", newEpoch)
			epochTimer.Reset(time.Duration(rcm.epochInterval) * time.Second)
		default:
		}

		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := data2tx(data, uint64(rcm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			rcm.nowDataNum++
		}

		if len(txlist) == int(rcm.batchDataNum) || rcm.nowDataNum == rcm.dataTotalNum {
			batchCount++
			log.Printf("[Reputation] Sending batch %d, txs in batch: %d, total sent: %d / %d\n",
				batchCount, len(txlist), rcm.nowDataNum, rcm.dataTotalNum)
			rcm.txSending(txlist)
			txlist = make([]*core.Transaction, 0)
			rcm.Ss.StopGap_Reset()
		}

		if rcm.nowDataNum == rcm.dataTotalNum {
			break
		}
	}
	log.Printf("[Reputation] MsgSendingControl: all %d transactions sent.\n", rcm.nowDataNum)
}

func (rcm *ReputationCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	rcm.sl.Slog.Printf("[Reputation] received block from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
}
