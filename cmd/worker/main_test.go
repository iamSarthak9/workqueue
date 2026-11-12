package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/iamSarthak9/workqueue/internal/config"
	"github.com/iamSarthak9/workqueue/internal/logger"
	"github.com/iamSarthak9/workqueue/internal/metrics"
	"github.com/iamSarthak9/workqueue/internal/queue"
)

func TestHandleMetrics(t *testing.T) {
	cfg := &config.Config{
		WorkerPort:        ":8081",
		WorkerConcurrency: 2,
	}
	memQueue := queue.NewInMemoryQueue()
	defer memQueue.Close()

	m := metrics.NewTracker()
	m.IncReceived()
	m.IncCompleted()

	log := logger.Get()
	svc := NewWorkerService(cfg, memQueue, m, log)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	svc.handleMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var snap metrics.Snapshot
	if err := json.NewDecoder(w.Body).Decode(&snap); err != nil {
		t.Fatalf("failed to decode metrics JSON: %v", err)
	}

	if snap.JobsDone != 1 {
		t.Errorf("expected 1 job done, got %d", snap.JobsDone)
	}
}

func TestHandleMetrics_MethodNotAllowed(t *testing.T) {
	cfg := &config.Config{
		WorkerPort: ":8081",
	}
	memQueue := queue.NewInMemoryQueue()
	defer memQueue.Close()

	m := metrics.NewTracker()
	log := logger.Get()
	svc := NewWorkerService(cfg, memQueue, m, log)

	req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	w := httptest.NewRecorder()

	svc.handleMetrics(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestHandleHealthz(t *testing.T) {
	cfg := &config.Config{
		WorkerPort: ":8081",
	}
	memQueue := queue.NewInMemoryQueue()
	defer memQueue.Close()

	m := metrics.NewTracker()
	log := logger.Get()
	svc := NewWorkerService(cfg, memQueue, m, log)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	svc.handleHealthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}
