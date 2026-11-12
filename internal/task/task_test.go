package task

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewTask(t *testing.T) {
	payload := map[string]any{"user_id": 42, "action": "send_welcome"}
	tsk, err := NewTask("user_notification", payload, 3)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if tsk.ID == "" {
		t.Fatal("expected non-empty task ID")
	}
	if tsk.Type != "user_notification" {
		t.Errorf("expected type user_notification, got %s", tsk.Type)
	}
	if tsk.MaxRetries != 3 {
		t.Errorf("expected max retries 3, got %d", tsk.MaxRetries)
	}
	if tsk.Status != StatusPending {
		t.Errorf("expected status pending, got %s", tsk.Status)
	}
	if tsk.CreatedAt.IsZero() {
		t.Error("expected non-zero created_at")
	}
}

func TestNewTask_EmptyType(t *testing.T) {
	_, err := NewTask("", map[string]any{"foo": "bar"}, 1)
	if err != ErrEmptyTaskType {
		t.Errorf("expected ErrEmptyTaskType, got %v", err)
	}
}

func TestTask_Validate(t *testing.T) {
	tsk := &Task{
		Type:    "",
		Payload: map[string]any{"test": 1},
	}
	if err := tsk.Validate(); err != ErrEmptyTaskType {
		t.Errorf("expected ErrEmptyTaskType, got %v", err)
	}

	tsk.Type = "valid_type"
	tsk.Payload = nil
	if err := tsk.Validate(); err != ErrNilPayload {
		t.Errorf("expected ErrNilPayload, got %v", err)
	}

	tsk.Payload = map[string]any{}
	if err := tsk.Validate(); err != nil {
		t.Errorf("expected valid task, got %v", err)
	}
}

func TestTask_LegacyJSONCompatibility(t *testing.T) {
	legacyJSON := `{
		"type": "send_email",
		"retries": 5,
		"payload": {
			"to": "dev@workqueue.local",
			"subject": "System Alert"
		}
	}`

	var tsk Task
	if err := json.Unmarshal([]byte(legacyJSON), &tsk); err != nil {
		t.Fatalf("failed to unmarshal legacy json: %v", err)
	}

	if tsk.Type != "send_email" {
		t.Errorf("expected send_email, got %s", tsk.Type)
	}
	if tsk.MaxRetries != 5 {
		t.Errorf("expected max retries 5, got %d", tsk.MaxRetries)
	}
	if tsk.ID == "" {
		t.Error("expected auto-generated ID for task without one")
	}
}

func TestTask_CalculateBackoff(t *testing.T) {
	tsk := &Task{}
	b1 := tsk.CalculateBackoff(1)
	b2 := tsk.CalculateBackoff(2)
	b3 := tsk.CalculateBackoff(3)

	if b1 <= 0 || b2 <= 0 || b3 <= 0 {
		t.Error("backoff should always be positive")
	}
	if b3 > 35*time.Second {
		t.Errorf("backoff exceeded capped limit: %v", b3)
	}
}

func TestGenerateUUIDv4(t *testing.T) {
	id1 := GenerateUUIDv4()
	id2 := GenerateUUIDv4()

	if len(id1) != 36 {
		t.Errorf("expected 36-char uuid, got %d chars: %s", len(id1), id1)
	}
	if id1 == id2 {
		t.Error("expected distinct UUIDs")
	}
}
