package measure

import (
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

const stalePendingTakeoverTTL = 90 * time.Second

type pendingTakeover struct {
	start    time.Time
	sequence int
	epoch    int
}

type completedTakeover struct {
	targetShardID uint64
	sequence      int
	epoch         int
	duration      time.Duration
	mode          string
	startedAt     time.Time
	completedAt   time.Time
}

// 接管时长测试模块
type TestTakeoverTime struct {
	mu                 sync.RWMutex
	pendingTakeovers   map[uint64]pendingTakeover // 每个目标分片同时只允许一个进行中的接管事件
	completedTakeovers []completedTakeover        // 保留所有完成事件，避免同一分片多次接管被覆盖
	nextSequence       map[uint64]int             // 为同一分片的多次接管生成递增编号
}

func NewTestTakeoverTime() *TestTakeoverTime {
	return &TestTakeoverTime{
		pendingTakeovers:   make(map[uint64]pendingTakeover),
		completedTakeovers: make([]completedTakeover, 0),
		nextSequence:       make(map[uint64]int),
	}
}

func (tt *TestTakeoverTime) OutputMetricName() string {
	return "Takeover_Duration_Module_ms"
}

// 供 Supervisor 关闭时输出最终结果
func (tt *TestTakeoverTime) OutputRecord() ([]float64, float64) {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	tt.writeToCSV()

	fmt.Println("=========== Takeover Duration Results ===========")
	var sum float64
	var count float64
	var records []float64

	// 分类统计: CS_l 直接接管 vs BSG 组接管
	var directSum, bsgSum float64
	var directCount, bsgCount float64

	totalStarted := 0
	for _, sequence := range tt.nextSequence {
		totalStarted += sequence
	}

	if totalStarted == 0 {
		fmt.Println("No takeover event was triggered (no CSupervisionRes with IsFaulty=true).")
	}
	for _, event := range tt.completedTakeovers {
		ms := float64(event.duration.Microseconds()) / 1000.0
		mode := event.mode
		if mode == "" {
			mode = "Unknown"
		}
		if mode == "BSG_Activated" {
			fmt.Printf("Epoch %d Shard %d Event #%d [BSG Takeover] Duration: %.2f ms\n", event.epoch, event.targetShardID, event.sequence, ms)
			bsgSum += ms
			bsgCount++
		} else {
			fmt.Printf("Epoch %d Shard %d Event #%d [Direct Takeover] Duration: %.2f ms\n", event.epoch, event.targetShardID, event.sequence, ms)
			directSum += ms
			directCount++
		}
		records = append(records, ms)
		sum += ms
		count++
	}
	if totalStarted > 0 && len(tt.completedTakeovers) == 0 {
		fmt.Printf("Takeover started for %d event(s) but not completed before shutdown.\n", totalStarted)
	}
	if len(tt.pendingTakeovers) > 0 {
		fmt.Printf("%d takeover event(s) were still pending at shutdown.\n", len(tt.pendingTakeovers))
	}
	if directCount > 0 {
		fmt.Printf("--- Direct Takeover (CS_l): %.0f event(s), Average: %.2f ms\n", directCount, directSum/directCount)
	}
	if bsgCount > 0 {
		fmt.Printf("--- BSG Takeover: %.0f event(s), Average: %.2f ms\n", bsgCount, bsgSum/bsgCount)
	}
	avg := 0.0
	if count > 0 {
		avg = sum / count
		fmt.Printf("Overall Average Takeover Time: %.2f ms\n", avg)
	}
	fmt.Println("=================================================")

	// 返回值用于画图 (X轴：次数或分片, Y轴：耗时)
	return records, avg
}

func (tt *TestTakeoverTime) writeToCSV() {
	fileName := tt.OutputMetricName()
	detailHeader := []string{"EpochID", "TargetShardID", "EventSequence", "TakeoverMode", "Duration_ms", "StartedAt", "CompletedAt"}
	detailRows := make([][]string, 0, len(tt.completedTakeovers))

	epochCount := make(map[int]int)
	epochDuration := make(map[int]float64)

	for _, event := range tt.completedTakeovers {
		mode := event.mode
		if mode == "" {
			mode = "Unknown"
		}
		durationMS := float64(event.duration.Microseconds()) / 1000.0
		detailRows = append(detailRows, []string{
			strconv.Itoa(event.epoch),
			strconv.FormatUint(event.targetShardID, 10),
			strconv.Itoa(event.sequence),
			mode,
			strconv.FormatFloat(durationMS, 'f', 3, 64),
			event.startedAt.Format("2006-01-02 15:04:05.000"),
			event.completedAt.Format("2006-01-02 15:04:05.000"),
		})
		epochCount[event.epoch]++
		epochDuration[event.epoch] += durationMS
	}
	WriteMetricsToCSV(fileName, detailHeader, detailRows)

	summaryName := fileName + "_ByEpoch"
	summaryHeader := []string{"EpochID", "TakeoverCount", "TotalDuration_ms", "AverageDuration_ms"}
	epochs := make([]int, 0, len(epochCount))
	for epoch := range epochCount {
		epochs = append(epochs, epoch)
	}
	sort.Ints(epochs)

	summaryRows := make([][]string, 0, len(epochs))
	for _, epoch := range epochs {
		count := epochCount[epoch]
		total := epochDuration[epoch]
		avg := 0.0
		if count > 0 {
			avg = total / float64(count)
		}
		summaryRows = append(summaryRows, []string{
			strconv.Itoa(epoch),
			strconv.Itoa(count),
			strconv.FormatFloat(total, 'f', 3, 64),
			strconv.FormatFloat(avg, 'f', 3, 64),
		})
	}
	WriteMetricsToCSV(summaryName, summaryHeader, summaryRows)
}

// 处理与区块生成无关的额外消息
func (tt *TestTakeoverTime) HandleExtraMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	tt.mu.Lock()
	defer tt.mu.Unlock()

	switch msgType {
	// 1. 拦截 L1 收到的举报结果（作为接管起点）
	case message.CSupervisionRes:
		var smsg message.SupervisionResultMsg
		json.Unmarshal(content, &smsg)

		// 只统计会触发接管流程的跨分片故障。
		// reporter==target 是分片内作恶上报（E 惩罚），不应计入接管耗时。
		if smsg.IsFaulty && smsg.ReporterShardID != smsg.TargetShardID {
			targetID := smsg.TargetShardID
			// 同一目标分片在接管完成前通常忽略重复上报。
			// 但若跨 epoch 或 pending 已过久，说明旧事件可能已失效，替换为新事件。
			if pending, exists := tt.pendingTakeovers[targetID]; exists {
				if pending.epoch == smsg.Epoch && time.Since(pending.start) <= stalePendingTakeoverTTL {
					return
				}
				delete(tt.pendingTakeovers, targetID)
			}
			tt.nextSequence[targetID]++
			tt.pendingTakeovers[targetID] = pendingTakeover{
				start:    time.Now(),
				sequence: tt.nextSequence[targetID],
				epoch:    smsg.Epoch,
			}
		}

	// 2. 拦截 L1 收到的 TB 上传（作为接管终点）
	case message.CUploadTB:
		var umsg message.UploadTBMsg
		json.Unmarshal(content, &umsg)

		// 只将接管完成 TB 作为结束信号，普通 TB 上报不参与接管计时。
		if !umsg.IsTakeoverCompletion {
			return
		}

		targetID := umsg.ShardID
		if targetID == 0 && umsg.TB.ShardID != 0 {
			targetID = umsg.TB.ShardID
		}

		if pending, ok := tt.pendingTakeovers[targetID]; ok {
			completedAt := time.Now()
			duration := completedAt.Sub(pending.start)
			mode := umsg.TakeoverMode
			if mode == "" {
				mode = "Unknown"
			}
			tt.completedTakeovers = append(tt.completedTakeovers, completedTakeover{
				targetShardID: targetID,
				sequence:      pending.sequence,
				epoch:         pending.epoch,
				duration:      duration,
				mode:          mode,
				startedAt:     pending.start,
				completedAt:   completedAt,
			})
			delete(tt.pendingTakeovers, targetID)

			modeLabel := "Direct"
			if mode == "BSG_Activated" {
				modeLabel = "BSG"
			} else if mode == "Unknown" {
				modeLabel = "Unknown"
			}
			fmt.Printf("⏱️ [Metric] Takeover COMPLETED for Shard %d Event #%d [%s]. Duration: %v\n", targetID, pending.sequence, modeLabel, duration)
		}
	}
}

func (tt *TestTakeoverTime) UpdateMeasureRecord(bim *message.BlockInfoMsg) {
	// 本模块主要通过 HandleExtraMessage 监听状态，不强依赖 BlockInfo
}
