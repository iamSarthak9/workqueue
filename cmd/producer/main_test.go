package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/iamSarthak9/workqueue/internal/config"
	"github.com/iamSarthak9/workqueue/internal/logger"
	"github.com/iamSarthak9/workqueue/internal/queue"
)

func setupTestServer() (*Server, *queue.InMemoryQueue) {
	cfg := &config.Config{
		ProducerPort: ":8080",
		LogLevel:     "DEBUG",
	}
	memQueue := queue.NewInMemoryQueue()
	log := logger.Get()
	return NewServer(cfg, memQueue, log), memQueue
}

func TestHandleEnqueue_Success(t *testing.T) {
	srv, memQueue := setupTestServer()
	defer memQueue.Close()

	payload := map[string]any{
		"type":    "send_email",
		"retries": 3,
		"payload": map[string]any{
			"to":      "user@example.com",
			"subject": "Welcome",
		},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(body))
	w := httptest.NewRecorder()

	srv.handleEnqueue(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected status 202 Accepted, got %d: %s", w.Code, w.Body.String())
	}

	var resp Response
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to parse response json: %v", err)
	}

	if resp.Status != "success" {
		t.Errorf("expected status success, got %s", resp.Status)
	}
	if resp.Task == nil || resp.Task.ID == "" {
		t.Fatal("expected task with valid ID in response")
	}
}

func TestHandleEnqueue_InvalidMethod(t *testing.T) {
	srv, memQueue := setupTestServer()
	defer memQueue.Close()

	req := httptest.NewRequest(http.MethodGet, "/enqueue", nil)
	w := httptest.NewRecorder()

	srv.handleEnqueue(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405 Method Not Allowed, got %d", w.Code)
	}
}

func TestHandleEnqueue_EmptyType(t *testing.T) {
	srv, memQueue := setupTestServer()
	defer memQueue.Close()

	payload := map[string]any{
		"type":    "",
		"payload": map[string]any{"key": "value"},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(body))
	w := httptest.NewRecorder()

	srv.handleEnqueue(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request, got %d", w.Code)
	}
}

func TestHandleHealthz(t *testing.T) {
	srv, memQueue := setupTestServer()
	defer memQueue.Close()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	srv.handleHealthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}
