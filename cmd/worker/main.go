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
	"github.com/iamSarthak9/workqueue/internal/metrics"
	"github.com/iamSarthak9/workqueue/internal/queue"
	"github.com/iamSarthak9/workqueue/internal/worker"
)

type WorkerService struct {
	cfg     *config.Config
	queue   queue.Queue
	pool    *worker.Pool
	metrics *metrics.Tracker
	logger  *logger.Logger
	server  *http.Server
}

func NewWorkerService(cfg *config.Config, q queue.Queue, m *metrics.Tracker, log *logger.Logger) *WorkerService {
	pool := worker.NewPool(q, worker.DefaultRegistry(), m, log, cfg.WorkerConcurrency)

	svc := &WorkerService{
		cfg:     cfg,
		queue:   q,
		pool:    pool,
		metrics: m,
		logger:  log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", svc.handleMetrics)
	mux.HandleFunc("/healthz", svc.handleHealthz)

	svc.server = &http.Server{
		Addr:         cfg.WorkerPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	return svc
}

func (s *WorkerService) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed: only GET allowed"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if qLen, err := s.queue.GetQueueLength(ctx); err == nil {
		s.metrics.SetQueueDepth(qLen)
	}
	if dlqLen, err := s.queue.GetDLQLength(ctx); err == nil {
		s.metrics.SetDLQDepth(dlqLen)
	}

	snap := s.metrics.Snapshot()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(snap)
}

func (s *WorkerService) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	redisStatus := "connected"
	if err := s.queue.Ping(ctx); err != nil {
		redisStatus = "unreachable"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"service":        "workqueue-worker",
		"status":         "healthy",
		"redis":          redisStatus,
		"active_workers": s.cfg.WorkerConcurrency,
		"timestamp":      time.Now().UTC(),
	})
}

func main() {
	cfg := config.Load()
	log, _ := logger.Init(cfg.LogLevel, cfg.LogFilePath)
	defer log.Close()

	log.Info("initializing workqueue worker daemon", "concurrency", cfg.WorkerConcurrency, "port", cfg.WorkerPort, "redis_url", cfg.RedisURL)

	q, err := queue.NewRedisQueue(cfg.RedisURL, cfg.QueueName, cfg.DLQName)
	if err != nil {
		log.Error("failed to connect to redis", "error", err.Error())
		os.Exit(1)
	}
	defer q.Close()

	m := metrics.NewTracker()
	svc := NewWorkerService(cfg, q, m, log)

	svc.pool.Start()

	go func() {
		log.Info("metrics endpoint listening", "address", cfg.WorkerPort)
		if err := svc.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server fatal error", "error", err.Error())
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Info("shutdown signal received; commencing graceful teardown...")

	httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpCancel()
	_ = svc.server.Shutdown(httpCtx)

	_ = svc.pool.Stop(cfg.ShutdownTimeout)
	log.Info("workqueue worker daemon shut down cleanly")
}
