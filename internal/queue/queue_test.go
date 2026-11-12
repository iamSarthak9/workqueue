package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iamSarthak9/workqueue/internal/task"
)

func TestInMemoryQueue_EnqueueDequeue(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	tsk, _ := task.NewTask("send_notification", map[string]any{"to": "user@domain.com"}, 2)
	ctx := context.Background()

	if err := q.Enqueue(ctx, tsk); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	length, err := q.GetQueueLength(ctx)
	if err != nil || length != 1 {
		t.Fatalf("expected queue length 1, got %d (err: %v)", length, err)
	}

	dequeued, err := q.Dequeue(ctx, 1*time.Second)
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}

	if dequeued.ID != tsk.ID {
		t.Errorf("expected task id %s, got %s", tsk.ID, dequeued.ID)
	}

	length, _ = q.GetQueueLength(ctx)
	if length != 0 {
		t.Errorf("expected queue length 0, got %d", length)
	}
}

func TestInMemoryQueue_DLQ(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	tsk, _ := task.NewTask("failing_job", map[string]any{}, 0)

	failureErr := errors.New("network timeout")
	if err := q.SendToDLQ(ctx, tsk, failureErr); err != nil {
		t.Fatalf("failed to send to DLQ: %v", err)
	}

	dlqLen, err := q.GetDLQLength(ctx)
	if err != nil || dlqLen != 1 {
		t.Fatalf("expected dlq length 1, got %d", dlqLen)
	}

	if tsk.Status != task.StatusDeadLetter {
		t.Errorf("expected dead letter status, got %s", tsk.Status)
	}
	if tsk.LastError != "network timeout" {
		t.Errorf("expected last error network timeout, got %s", tsk.LastError)
	}
}

func TestInMemoryQueue_Timeout(t *testing.T) {
	q := NewInMemoryQueue()
	defer q.Close()

	ctx := context.Background()
	_, err := q.Dequeue(ctx, 50*time.Millisecond)
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}
