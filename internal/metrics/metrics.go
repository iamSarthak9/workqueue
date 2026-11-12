package metrics

import (
	"sync/atomic"
	"time"
)

type Tracker struct {
	startTime    time.Time
	received     atomic.Uint64
	completed    atomic.Uint64
	failed       atomic.Uint64
	retried      atomic.Uint64
	deadLettered atomic.Uint64
	activeWorkers atomic.Int64
	queueDepth   atomic.Int64
	dlqDepth     atomic.Int64
}

type Snapshot struct {
	TotalJobsInQueue int64  `json:"total_jobs_in_queue"`
	JobsDone         uint64 `json:"jobs_done"`
	JobsFailed       uint64 `json:"jobs_failed"`
	JobsReceived     uint64 `json:"jobs_received"`
	JobsRetried      uint64 `json:"jobs_retried"`
	JobsDeadLettered uint64 `json:"jobs_dead_lettered"`
	ActiveWorkers    int64  `json:"active_workers"`
	DLQDepth         int64  `json:"dlq_depth"`
	UptimeSeconds    int64  `json:"uptime_seconds"`
}

func NewTracker() *Tracker {
	return &Tracker{
		startTime: time.Now().UTC(),
	}
}

func (m *Tracker) IncReceived()       { m.received.Add(1) }
func (m *Tracker) IncCompleted()      { m.completed.Add(1) }
func (m *Tracker) IncFailed()         { m.failed.Add(1) }
func (m *Tracker) IncRetried()        { m.retried.Add(1) }
func (m *Tracker) IncDeadLettered()   { m.deadLettered.Add(1) }
func (m *Tracker) WorkerStarted()     { m.activeWorkers.Add(1) }
func (m *Tracker) WorkerStopped()     { m.activeWorkers.Add(-1) }
func (m *Tracker) SetQueueDepth(d int64) { m.queueDepth.Store(d) }
func (m *Tracker) SetDLQDepth(d int64)   { m.dlqDepth.Store(d) }

func (m *Tracker) Snapshot() Snapshot {
	return Snapshot{
		TotalJobsInQueue: m.queueDepth.Load(),
		JobsDone:         m.completed.Load(),
		JobsFailed:       m.failed.Load(),
		JobsReceived:     m.received.Load(),
		JobsRetried:      m.retried.Load(),
		JobsDeadLettered: m.deadLettered.Load(),
		ActiveWorkers:    m.activeWorkers.Load(),
		DLQDepth:         m.dlqDepth.Load(),
		UptimeSeconds:    int64(time.Since(m.startTime).Seconds()),
	}
}
