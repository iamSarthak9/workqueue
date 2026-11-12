package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/iamSarthak9/workqueue/internal/logger"
	"github.com/iamSarthak9/workqueue/internal/metrics"
	"github.com/iamSarthak9/workqueue/internal/queue"
	"github.com/iamSarthak9/workqueue/internal/task"
)

type Pool struct {
	queue       queue.Queue
	registry    *Registry
	metrics     *metrics.Tracker
	logger      *logger.Logger
	concurrency int
	taskTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewPool(q queue.Queue, reg *Registry, m *metrics.Tracker, log *logger.Logger, concurrency int) *Pool {
	if reg == nil {
		reg = DefaultRegistry()
	}
	if m == nil {
		m = metrics.NewTracker()
	}
	if log == nil {
		log = logger.Get()
	}
	if concurrency <= 0 {
		concurrency = 4
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Pool{
		queue:       q,
		registry:    reg,
		metrics:     m,
		logger:      log,
		concurrency: concurrency,
		taskTimeout: 60 * time.Second,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (p *Pool) Start() {
	p.logger.Info("starting worker pool", "concurrency", p.concurrency)

	for i := 1; i <= p.concurrency; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

func (p *Pool) Stop(timeout time.Duration) error {
	p.logger.Info("shutting down worker pool...")
	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("all workers stopped cleanly")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("worker shutdown timed out after %v", timeout)
	}
}

func (p *Pool) workerLoop(workerID int) {
	defer p.wg.Done()
	p.metrics.WorkerStarted()
	defer p.metrics.WorkerStopped()

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		t, err := p.queue.Dequeue(p.ctx, 2*time.Second)
		if err != nil {
			if errors.Is(err, queue.ErrTimeout) || errors.Is(err, context.Canceled) {
				continue
			}
			p.logger.Error("error dequeuing task", "worker_id", workerID, "error", err.Error())
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if t == nil {
			continue
		}

		if depth, errLen := p.queue.GetQueueLength(p.ctx); errLen == nil {
			p.metrics.SetQueueDepth(depth)
		}
		if dlqDepth, errDlq := p.queue.GetDLQLength(p.ctx); errDlq == nil {
			p.metrics.SetDLQDepth(dlqDepth)
		}

		p.executeTask(t)
	}
}

func (p *Pool) executeTask(t *task.Task) {
	start := time.Now()
	t.Status = task.StatusProcessing
	t.UpdatedAt = start

	handler, exists := p.registry.Get(t.Type)
	if !exists {
		err := fmt.Errorf("%w: %s", ErrUnknownTaskType, t.Type)
		p.handleFailure(t, err)
		return
	}

	execCtx, cancel := context.WithTimeout(p.ctx, p.taskTimeout)
	err := handler.Process(execCtx, t)
	cancel()
	duration := time.Since(start)

	if err != nil {
		p.handleFailure(t, err)
		return
	}

	t.Status = task.StatusCompleted
	t.UpdatedAt = time.Now().UTC()
	p.metrics.IncCompleted()
	p.logger.LogTaskSuccess(t, duration)
}

func (p *Pool) handleFailure(t *task.Task, err error) {
	p.metrics.IncFailed()
	t.CurrentRetry++
	t.LastError = err.Error()
	t.UpdatedAt = time.Now().UTC()

	if t.CurrentRetry <= t.MaxRetries {
		p.metrics.IncRetried()
		p.logger.LogTaskFailure(t, err, true)

		go func(taskCopy *task.Task, attempt int) {
			backoff := taskCopy.CalculateBackoff(attempt)
			select {
			case <-time.After(backoff):
				if reqErr := p.queue.Requeue(context.Background(), taskCopy); reqErr != nil {
					p.logger.Error("failed to requeue task after backoff",
						"task_id", taskCopy.ID,
						"error", reqErr.Error(),
					)
				}
			case <-p.ctx.Done():
				_ = p.queue.Requeue(context.Background(), taskCopy)
			}
		}(t, t.CurrentRetry)
	} else {
		p.metrics.IncDeadLettered()
		p.logger.LogTaskDeadLetter(t, err)

		if dlqErr := p.queue.SendToDLQ(context.Background(), t, err); dlqErr != nil {
			p.logger.Error("failed to push task to DLQ",
				"task_id", t.ID,
				"error", dlqErr.Error(),
			)
		}
	}
}
