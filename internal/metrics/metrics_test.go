package metrics

import (
	"sync"
	"testing"
)

func TestMetricsConcurrency(t *testing.T) {
	tracker := NewTracker()
	const workers = 20
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				tracker.IncReceived()
				tracker.IncCompleted()
				tracker.IncFailed()
				tracker.IncRetried()
				tracker.IncDeadLettered()
			}
		}()
	}

	wg.Wait()

	snap := tracker.Snapshot()
	expected := uint64(workers * iterations)

	if snap.JobsReceived != expected {
		t.Errorf("expected received %d, got %d", expected, snap.JobsReceived)
	}
	if snap.JobsDone != expected {
		t.Errorf("expected completed %d, got %d", expected, snap.JobsDone)
	}
	if snap.JobsFailed != expected {
		t.Errorf("expected failed %d, got %d", expected, snap.JobsFailed)
	}
	if snap.JobsRetried != expected {
		t.Errorf("expected retried %d, got %d", expected, snap.JobsRetried)
	}
	if snap.JobsDeadLettered != expected {
		t.Errorf("expected dead lettered %d, got %d", expected, snap.JobsDeadLettered)
	}
}

func TestWorkerCounters(t *testing.T) {
	tracker := NewTracker()

	tracker.WorkerStarted()
	tracker.WorkerStarted()
	if snap := tracker.Snapshot(); snap.ActiveWorkers != 2 {
		t.Errorf("expected 2 active workers, got %d", snap.ActiveWorkers)
	}

	tracker.WorkerStopped()
	if snap := tracker.Snapshot(); snap.ActiveWorkers != 1 {
		t.Errorf("expected 1 active worker, got %d", snap.ActiveWorkers)
	}
}
