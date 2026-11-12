package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/iamSarthak9/workqueue/internal/task"
)

type Logger struct {
	slog *slog.Logger
	mu   sync.Mutex
	file *os.File
}

var defaultLogger *Logger
var once sync.Once

func Init(levelStr string, logPath string) (*Logger, error) {
	var level slog.Level
	switch levelStr {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	writers := []io.Writer{os.Stdout}
	var f *os.File

	if logPath != "" {
		dir := filepath.Dir(logPath)
		if dir != "." && dir != "/" {
			_ = os.MkdirAll(dir, 0755)
		}
		var err error
		f, err = os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			writers = append(writers, f)
		}
	}

	multiWriter := io.MultiWriter(writers...)
	handler := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: level,
	})

	l := &Logger{
		slog: slog.New(handler),
		file: f,
	}

	once.Do(func() {
		defaultLogger = l
	})

	return l, nil
}

func Get() *Logger {
	if defaultLogger == nil {
		handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
		defaultLogger = &Logger{slog: slog.New(handler)}
	}
	return defaultLogger
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

func (l *Logger) Info(msg string, args ...any)  { l.slog.Info(msg, args...) }
func (l *Logger) Warn(msg string, args ...any)  { l.slog.Warn(msg, args...) }
func (l *Logger) Error(msg string, args ...any) { l.slog.Error(msg, args...) }
func (l *Logger) Debug(msg string, args ...any) { l.slog.Debug(msg, args...) }

func (l *Logger) LogTaskSuccess(t *task.Task, duration time.Duration) {
	l.slog.Info("task completed successfully",
		slog.String("task_id", t.ID),
		slog.String("task_type", t.Type),
		slog.Int("current_retry", t.CurrentRetry),
		slog.Duration("duration_ms", duration),
	)
}

func (l *Logger) LogTaskFailure(t *task.Task, err error, retrying bool) {
	l.slog.Warn("task execution failed",
		slog.String("task_id", t.ID),
		slog.String("task_type", t.Type),
		slog.String("error", err.Error()),
		slog.Int("current_retry", t.CurrentRetry),
		slog.Int("max_retries", t.MaxRetries),
		slog.Bool("will_retry", retrying),
	)
}

func (l *Logger) LogTaskDeadLetter(t *task.Task, err error) {
	l.slog.Error("task moved to dead letter queue",
		slog.String("task_id", t.ID),
		slog.String("task_type", t.Type),
		slog.String("error", err.Error()),
		slog.Int("total_attempts", t.CurrentRetry),
	)
}
