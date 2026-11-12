package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iamSarthak9/workqueue/internal/config"
	"github.com/iamSarthak9/workqueue/internal/logger"
	"github.com/iamSarthak9/workqueue/internal/queue"
	"github.com/iamSarthak9/workqueue/internal/task"
)

type Server struct {
	cfg    *config.Config
	queue  queue.Queue
	logger *logger.Logger
	server *http.Server
}

func NewServer(cfg *config.Config, q queue.Queue, log *logger.Logger) *Server {
	s := &Server{
		cfg:    cfg,
		queue:  q,
		logger: log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", s.handleEnqueue)
	mux.HandleFunc("/healthz", s.handleHealthz)

	s.server = &http.Server{
		Addr:         cfg.ProducerPort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

type Response struct {
	Status  string     `json:"status"`
	Message string     `json:"message,omitempty"`
	Error   string     `json:"error,omitempty"`
	Task    *task.Task `json:"task,omitempty"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, Response{
			Status: "error",
			Error:  "method not allowed: only POST requests are accepted",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1048576)

	var input task.Task
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, Response{
			Status: "error",
			Error:  fmt.Sprintf("invalid json payload: %v", err),
		})
		return
	}

	if err := input.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, Response{
			Status: "error",
			Error:  err.Error(),
		})
		return
	}

	if input.ID == "" {
		input.ID = task.GenerateUUIDv4()
	}
	if input.CreatedAt.IsZero() {
		input.CreatedAt = time.Now().UTC()
	}
	input.UpdatedAt = input.CreatedAt
	input.Status = task.StatusPending

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := s.queue.Enqueue(ctx, &input); err != nil {
		s.logger.Error("failed to enqueue task", "task_id", input.ID, "error", err.Error())
		writeJSON(w, http.StatusInternalServerError, Response{
			Status: "error",
			Error:  "internal server error: failed to enqueue task",
		})
		return
	}

	s.logger.Info("task enqueued successfully", "task_id", input.ID, "task_type", input.Type)

	writeJSON(w, http.StatusAccepted, Response{
		Status:  "success",
		Message: fmt.Sprintf("Task of type '%s' enqueued successfully", input.Type),
		Task:    &input,
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	redisStatus := "connected"
	if err := s.queue.Ping(ctx); err != nil {
		redisStatus = "unreachable"
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"service":   "workqueue-producer",
		"status":    "healthy",
		"redis":     redisStatus,
		"timestamp": time.Now().UTC(),
	})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func main() {
	cfg := config.Load()
	log, _ := logger.Init(cfg.LogLevel, cfg.LogFilePath)
	defer log.Close()

	log.Info("starting workqueue producer service", "port", cfg.ProducerPort, "redis_url", cfg.RedisURL)

	q, err := queue.NewRedisQueue(cfg.RedisURL, cfg.QueueName, cfg.DLQName)
	if err != nil {
		log.Error("failed to connect to redis", "error", err.Error())
		os.Exit(1)
	}
	defer q.Close()

	srv := NewServer(cfg, q, log)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := srv.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("producer server fatal error", "error", err.Error())
			os.Exit(1)
		}
	}()

	log.Info("workqueue producer is listening", "address", cfg.ProducerPort)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	_ = srv.server.Shutdown(shutdownCtx)
	log.Info("producer service exited cleanly")
}
