package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/iamSarthak9/workqueue/internal/task"
)

var (
	ErrUnknownTaskType = errors.New("unsupported task type: no registered handler")
	ErrHandlerTimeout  = errors.New("task handler execution timed out")
)

type Handler interface {
	Process(ctx context.Context, t *task.Task) error
}

type HandlerFunc func(ctx context.Context, t *task.Task) error

func (f HandlerFunc) Process(ctx context.Context, t *task.Task) error {
	return f(ctx, t)
}

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

var defaultRegistry = NewRegistry()

func NewRegistry() *Registry {
	r := &Registry{
		handlers: make(map[string]Handler),
	}
	r.registerDefaultHandlers()
	return r
}

func (r *Registry) Register(taskType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[taskType] = h
}

func (r *Registry) RegisterFunc(taskType string, fn HandlerFunc) {
	r.Register(taskType, fn)
}

func (r *Registry) Get(taskType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, exists := r.handlers[taskType]
	return h, exists
}

func DefaultRegistry() *Registry {
	return defaultRegistry
}

func (r *Registry) registerDefaultHandlers() {
	r.RegisterFunc("send_email", func(ctx context.Context, t *task.Task) error {
		to, ok := t.Payload["to"].(string)
		if !ok || to == "" {
			return errors.New("missing or invalid 'to' field in payload")
		}
		subject, ok := t.Payload["subject"].(string)
		if !ok || subject == "" {
			return errors.New("missing or invalid 'subject' field in payload")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		return nil
	})

	r.RegisterFunc("resize_image", func(ctx context.Context, t *task.Task) error {
		if _, ok := t.Payload["new_x"]; !ok {
			if _, okW := t.Payload["width"]; !okW {
				return errors.New("missing width or new_x in payload")
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Millisecond):
		}
		return nil
	})

	r.RegisterFunc("generate_pdf", func(ctx context.Context, t *task.Task) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(40 * time.Millisecond):
		}
		return nil
	})

	r.RegisterFunc("webhook_dispatch", func(ctx context.Context, t *task.Task) error {
		url, ok := t.Payload["url"].(string)
		if !ok || url == "" {
			return errors.New("missing webhook url in payload")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		return nil
	})
}

func Process_Task(task_to_execute task.Task) error {
	if task_to_execute.Payload == nil {
		return fmt.Errorf("payload is empty")
	}
	if task_to_execute.Type == "" {
		return fmt.Errorf("task type is empty")
	}

	handler, exists := defaultRegistry.Get(task_to_execute.Type)
	if !exists {
		return ErrUnknownTaskType
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return handler.Process(ctx, &task_to_execute)
}
