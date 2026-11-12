package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/iamSarthak9/workqueue/internal/task"
	"github.com/redis/go-redis/v9"
)

var (
	ErrNilTask     = errors.New("cannot enqueue nil task")
	ErrQueueClosed = errors.New("queue is closed")
	ErrTimeout     = errors.New("dequeue timed out")
)

type Queue interface {
	Enqueue(ctx context.Context, t *task.Task) error
	Dequeue(ctx context.Context, timeout time.Duration) (*task.Task, error)
	Requeue(ctx context.Context, t *task.Task) error
	SendToDLQ(ctx context.Context, t *task.Task, reason error) error
	GetQueueLength(ctx context.Context) (int64, error)
	GetDLQLength(ctx context.Context) (int64, error)
	Ping(ctx context.Context) error
	Close() error
}

type RedisQueue struct {
	client    *redis.Client
	queueName string
	dlqName   string
}

func NewRedisQueue(redisURL, queueName, dlqName string) (*RedisQueue, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url %q: %w", redisURL, err)
	}

	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	if queueName == "" {
		queueName = "workqueue:tasks"
	}
	if dlqName == "" {
		dlqName = "workqueue:dlq"
	}

	return &RedisQueue{
		client:    client,
		queueName: queueName,
		dlqName:   dlqName,
	}, nil
}

func (rq *RedisQueue) Enqueue(ctx context.Context, t *task.Task) error {
	if t == nil {
		return ErrNilTask
	}
	if err := t.Validate(); err != nil {
		return err
	}

	t.Status = task.StatusPending
	t.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("failed to serialize task: %w", err)
	}

	return rq.client.RPush(ctx, rq.queueName, data).Err()
}

func (rq *RedisQueue) Dequeue(ctx context.Context, timeout time.Duration) (*task.Task, error) {
	res, err := rq.client.BLPop(ctx, timeout, rq.queueName).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrTimeout
		}
		return nil, err
	}

	if len(res) < 2 {
		return nil, fmt.Errorf("malformed redis response: %v", res)
	}

	var t task.Task
	if err := json.Unmarshal([]byte(res[1]), &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task payload: %w", err)
	}

	return &t, nil
}

func (rq *RedisQueue) Requeue(ctx context.Context, t *task.Task) error {
	if t == nil {
		return ErrNilTask
	}
	t.Status = task.StatusPending
	t.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("failed to marshal task for retry: %w", err)
	}

	return rq.client.RPush(ctx, rq.queueName, data).Err()
}

func (rq *RedisQueue) SendToDLQ(ctx context.Context, t *task.Task, reason error) error {
	if t == nil {
		return ErrNilTask
	}
	t.Status = task.StatusDeadLetter
	if reason != nil {
		t.LastError = reason.Error()
	}
	t.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("failed to marshal dead letter task: %w", err)
	}

	return rq.client.RPush(ctx, rq.dlqName, data).Err()
}

func (rq *RedisQueue) GetQueueLength(ctx context.Context) (int64, error) {
	return rq.client.LLen(ctx, rq.queueName).Result()
}

func (rq *RedisQueue) GetDLQLength(ctx context.Context) (int64, error) {
	return rq.client.LLen(ctx, rq.dlqName).Result()
}

func (rq *RedisQueue) Ping(ctx context.Context) error {
	return rq.client.Ping(ctx).Err()
}

func (rq *RedisQueue) Close() error {
	return rq.client.Close()
}

type InMemoryQueue struct {
	mu     sync.Mutex
	tasks  []*task.Task
	dlq    []*task.Task
	closed bool
	notify chan struct{}
}

func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		tasks:  make([]*task.Task, 0),
		dlq:    make([]*task.Task, 0),
		notify: make(chan struct{}, 1),
	}
}

func (iq *InMemoryQueue) Enqueue(ctx context.Context, t *task.Task) error {
	if t == nil {
		return ErrNilTask
	}
	if err := t.Validate(); err != nil {
		return err
	}

	iq.mu.Lock()
	defer iq.mu.Unlock()

	if iq.closed {
		return ErrQueueClosed
	}

	t.Status = task.StatusPending
	t.UpdatedAt = time.Now().UTC()
	iq.tasks = append(iq.tasks, t)

	select {
	case iq.notify <- struct{}{}:
	default:
	}
	return nil
}

func (iq *InMemoryQueue) Dequeue(ctx context.Context, timeout time.Duration) (*task.Task, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		iq.mu.Lock()
		if len(iq.tasks) > 0 {
			t := iq.tasks[0]
			iq.tasks = iq.tasks[1:]
			iq.mu.Unlock()
			return t, nil
		}
		if iq.closed {
			iq.mu.Unlock()
			return nil, ErrQueueClosed
		}
		iq.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, ErrTimeout
		case <-iq.notify:
		}
	}
}

func (iq *InMemoryQueue) Requeue(ctx context.Context, t *task.Task) error {
	return iq.Enqueue(ctx, t)
}

func (iq *InMemoryQueue) SendToDLQ(ctx context.Context, t *task.Task, reason error) error {
	if t == nil {
		return ErrNilTask
	}
	iq.mu.Lock()
	defer iq.mu.Unlock()

	t.Status = task.StatusDeadLetter
	if reason != nil {
		t.LastError = reason.Error()
	}
	t.UpdatedAt = time.Now().UTC()
	iq.dlq = append(iq.dlq, t)
	return nil
}

func (iq *InMemoryQueue) GetQueueLength(ctx context.Context) (int64, error) {
	iq.mu.Lock()
	defer iq.mu.Unlock()
	return int64(len(iq.tasks)), nil
}

func (iq *InMemoryQueue) GetDLQLength(ctx context.Context) (int64, error) {
	iq.mu.Lock()
	defer iq.mu.Unlock()
	return int64(len(iq.dlq)), nil
}

func (iq *InMemoryQueue) Ping(ctx context.Context) error {
	iq.mu.Lock()
	defer iq.mu.Unlock()
	if iq.closed {
		return ErrQueueClosed
	}
	return nil
}

func (iq *InMemoryQueue) Close() error {
	iq.mu.Lock()
	defer iq.mu.Unlock()
	iq.closed = true
	close(iq.notify)
	return nil
}
