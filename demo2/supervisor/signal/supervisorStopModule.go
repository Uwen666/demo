package signal

import (
	"sync"
)

// to judge when the listener to send the stop message to the leaders
type StopSignal struct {
	stoplock sync.Mutex // check the stopGap will not be modified by other processes

	stopGap       int // record how many empty txLists from leaders in a row
	stopThreshold int // the threshold
}

func NewStopSignal(stop_Threshold int) *StopSignal {
	return &StopSignal{
		stopGap:       0,
		stopThreshold: stop_Threshold,
	}
}

// when receiving a message with an empty txList, then call this function to increase stopGap
func (ss *StopSignal) StopGap_Inc() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.stopGap++
}

// when receiving a message with txs excuted, then call this function to reset stopGap
func (ss *StopSignal) StopGap_Reset() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.stopGap = 0
}

// Check the stopGap is enough or not
// if StopGap is not less than stopThreshold, then the stop message should be sent to leaders.
func (ss *StopSignal) GapEnough() bool {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	return ss.stopGap >= ss.stopThreshold
}

func (ss *StopSignal) GetGap() int {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	return ss.stopGap
}

func (ss *StopSignal) GetThreshold() int {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	return ss.stopThreshold
}

// ReduceThreshold decreases the threshold by delta (e.g. when a shard is taken over
// and will no longer send empty-block reports). Threshold is clamped to >= 1.
func (ss *StopSignal) ReduceThreshold(delta int) {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.stopThreshold -= delta
	if ss.stopThreshold < 1 {
		ss.stopThreshold = 1
	}
}

// ResetAll resets stopGap and sets a new threshold atomically.
// Useful after epoch reconfiguration when takeover state is rebuilt.
func (ss *StopSignal) ResetAll(stopThreshold int) {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	if stopThreshold < 1 {
		stopThreshold = 1
	}
	ss.stopThreshold = stopThreshold
	ss.stopGap = 0
}
