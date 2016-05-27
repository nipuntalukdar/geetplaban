package geetplaban

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DEFAULT_NUM_SLOTS     = 8
	MAX_NUM_SLOTS         = 256
	MAX_NUM_SLOTS_MINUS_1 = 255
	MIN_INTERVAL          = 20
	MAX_INTERVAL          = 3600
)

func getCurrentInterval(width int64) int64 {
	t := time.Now().Unix()
	if width < MIN_INTERVAL {
		width = MIN_INTERVAL
	}
	if width > MAX_INTERVAL {
		width = MAX_INTERVAL
	}
	t = t - (t % width)
	return t
}

type StageStats struct {
	stage_id  uint
	emitted   *uint64
	failed    *uint64
	timespent *uint64
}

func NewStageStats(stage uint) *StageStats {
	return &StageStats{stage, new(uint64), new(uint64), new(uint64)}
}

func (ss *StageStats) String() string {
	return fmt.Sprintf("Sid:%d EM:%d F:%d T:%d",
		ss.stage_id, *ss.emitted, *ss.failed, *ss.timespent)
}

func (ss *StageStats) addEmitted(val uint64) {
	atomic.AddUint64(ss.emitted, val)
}

func (ss *StageStats) addFailed(val uint64) {
	atomic.AddUint64(ss.failed, val)
}

func (ss *StageStats) addTimespent(val uint64) {
	atomic.AddUint64(ss.timespent, val)
}

type AllStageStats struct {
	rws         []*sync.RWMutex
	stage_stats []map[uint]*StageStats
	num_slots   uint
	interval    int64
}

func NewAllStageStats(num_slots uint, interval int64) *AllStageStats {
	var rws []*sync.RWMutex
	if num_slots > MAX_NUM_SLOTS {
		num_slots = MAX_NUM_SLOTS
	}
	if num_slots == 0 {
		num_slots = DEFAULT_NUM_SLOTS
	}

	i := uint(0)
	for i < num_slots {
		rws = append(rws, &sync.RWMutex{})
		i++
	}

	var stage_stats []map[uint]*StageStats

	i = 0
	for i < num_slots {
		stage_stats = append(stage_stats, make(map[uint]*StageStats))
		i++
	}
	return &AllStageStats{rws, stage_stats, num_slots, interval}
}

func (allstgs *AllStageStats) getStats(stage_id uint) *StageStats {
	slot := (stage_id & MAX_NUM_SLOTS_MINUS_1) % allstgs.num_slots
	allstgs.rws[slot].RLock()
	s, ok := allstgs.stage_stats[slot][stage_id]
	allstgs.rws[slot].RUnlock()
	if !ok {
		allstgs.rws[slot].Lock()
		s, ok = allstgs.stage_stats[slot][stage_id]
		if !ok {
			s = NewStageStats(stage_id)
			allstgs.stage_stats[slot][stage_id] = s
		}
		allstgs.rws[slot].Unlock()
	}
	return s
}

func (allstgs *AllStageStats) addFailed(stage_id uint, delta uint64) {
	s := allstgs.getStats(stage_id)
	s.addFailed(delta)
}

func (allstgs *AllStageStats) addTimespent(stage_id uint, delta uint64) {
	s := allstgs.getStats(stage_id)
	s.addTimespent(delta)
}

func (allstgs *AllStageStats) addEmitted(stage_id uint, delta uint64) {
	s := allstgs.getStats(stage_id)
	s.addEmitted(delta)
}

func (allstgs *AllStageStats) dumpJson() {
	for i, l := range allstgs.rws {
		l.Lock()
		for _, v := range allstgs.stage_stats[i] {
			LOG.Infof("STAT: %s", v)
		}
		l.Unlock()
	}
}

type AllStats struct {
	t                   *time.Ticker
	t2                  *time.Ticker
	flush               *list.List
	flushLock           *sync.Mutex
	current_stage_stats *AllStageStats
}

func NewAllStats() *AllStats {
	t := time.NewTicker(1)
	t2 := time.NewTicker(2)
	flush := list.New()
	flushl := &sync.Mutex{}
	current_stage_stats := NewAllStageStats(8, getCurrentInterval(MIN_INTERVAL))
	ret := &AllStats{t, t2, flush, flushl, current_stage_stats}
	go ret.statsMonitor()
	go ret.statsFlusher()
	return ret
}

func (asts *AllStats) setCurrentStageStats(stgsts *AllStageStats) {
	asts.current_stage_stats = stgsts
}

func (asts *AllStats) Stop() {
	asts.t.Stop()
	asts.t2.Stop()
}

func (asts *AllStats) addFailed(stage_id uint, delta uint64) {
	asts.current_stage_stats.addFailed(stage_id, delta)
}

func (asts *AllStats) addEmitted(stage_id uint, delta uint64) {
	asts.current_stage_stats.addEmitted(stage_id, delta)
}

func (asts *AllStats) addTimespent(stage_id uint, delta uint64) {
	asts.current_stage_stats.addTimespent(stage_id, delta)
}

func (asts *AllStats) statsFlusher() {
	for range asts.t2.C {
		asts.flushLock.Lock()
		e := asts.flush.Front()
		asts.flushLock.Unlock()
		if e == nil {
			continue
		}
		asts.flushLock.Lock()
		stgsts := asts.flush.Remove(e)
		asts.flushLock.Unlock()
		stgsts.(*AllStageStats).dumpJson()
	}
}

func (asts *AllStats) statsMonitor() {
	lastCheck := time.Now().Unix()
	for range asts.t.C {
		if time.Now().Unix()-lastCheck < 20 {
			continue
		}
		lastCheck = time.Now().Unix()
		temp := asts.current_stage_stats
		asts.current_stage_stats = NewAllStageStats(8, getCurrentInterval(MIN_INTERVAL))
		time.Sleep(2 * time.Second)
		asts.flushLock.Lock()
		asts.flush.PushBack(temp)
		asts.flushLock.Unlock()
	}
}

var (
	allsts *AllStats = nil
	stonce sync.Once
)

func init_stats() {
	stonce.Do(func() {
		allsts = NewAllStats()
	})
}

func Stats() *AllStats {
	return allsts
}
