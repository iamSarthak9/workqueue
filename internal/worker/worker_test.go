package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iamSarthak9/workqueue/internal/logger"
	"github.com/iamSarthak9/workqueue/internal/metrics"
	"github.com/iamSarthak9/workqueue/internal/queue"
	"github.com/iamSarthak9/workqueue/internal/task"
)

func TestRegistry(t *testing.T) {
	reg := NewRegistry()

	called := false
	reg.RegisterFunc("custom_job", func(ctx context.Context, tsk *task.Task) error {
		called = true
		return nil
	})

	handler, exists := reg.Get("custom_job")
	if !exists {
		t.Fatal("expected handler to exist")
	}

	tsk, _ := task.NewTask("custom_job", map[string]any{}, 1)
	err := handler.Process(context.Background(), tsk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected handler function to be invoked")
	}

	_, existsUnknown := reg.Get("non_existent")
	if existsUnknown {
		t.Fatal("expected unknown handler to not exist")
	}
}

func TestWorkerPool_SuccessExecution(t *testing.T) {
	q := queue.NewInMemoryQueue()
	defer q.Close()

	reg := NewRegistry()
	doneCh := make(chan string, 1)

	reg.RegisterFunc("test_task", func(ctx context.Context, tsk *task.Task) error {
		doneCh <- tsk.ID
		return nil
	})

	m := metrics.NewTracker()
	log := logger.Get()

	pool := NewPool(q, reg, m, log, 2)
	pool.Start()
	defer pool.Stop(2 * time.Second)

	tsk, _ := task.NewTask("test_task", map[string]any{"data": 123}, 1)
	_ = q.Enqueue(context.Background(), tsk)

	select {
	case id := <-doneCh:
		if id != tsk.ID {
			t.Errorf("expected task id %s, got %s", tsk.ID, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for task completion")
	}

	snap := m.Snapshot()
	if snap.JobsDone != 1 {
		t.Errorf("expected 1 job done, got %d", snap.JobsDone)
	}
}

func TestWorkerPool_RetryAndDLQ(t *testing.T) {
	q := queue.NewInMemoryQueue()
	defer q.Close()

	reg := NewRegistry()

	reg.RegisterFunc("fail_task", func(ctx context.Context, tsk *task.Task) error {
		return errors.New("permanent failure")
	})

	m := metrics.NewTracker()
	log := logger.Get()

	pool := NewPool(q, reg, m, log, 1)
	pool.Start()
	defer pool.Stop(2 * time.Second)

	tsk, _ := task.NewTask("fail_task", map[string]any{}, 0)
	_ = q.Enqueue(context.Background(), tsk)

	time.Sleep(300 * time.Millisecond)

	dlqLen, _ := q.GetDLQLength(context.Background())
	if dlqLen != 1 {
		t.Errorf("expected 1 task in DLQ, got %d", dlqLen)
	}

	snap := m.Snapshot()
	if snap.JobsDeadLettered != 1 {
		t.Errorf("expected 1 dead lettered, got %d", snap.JobsDeadLettered)
	}
}
