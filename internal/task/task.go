package task

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand"
	"strings"
	"time"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusCompleted  TaskStatus = "completed"
	StatusFailed     TaskStatus = "failed"
	StatusDeadLetter TaskStatus = "dead_letter"
)

var (
	ErrEmptyTaskType = errors.New("task type cannot be empty")
	ErrNilPayload    = errors.New("task payload cannot be nil")
)

type Task struct {
	ID           string         `json:"id"`
	Type         string         `json:"type"`
	Payload      map[string]any `json:"payload"`
	MaxRetries   int            `json:"max_retries"`
	CurrentRetry int            `json:"current_retry"`
	Status       TaskStatus     `json:"status"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
	LastError    string         `json:"last_error,omitempty"`
}

func NewTask(taskType string, payload map[string]any, maxRetries int) (*Task, error) {
	cleanType := strings.TrimSpace(taskType)
	if cleanType == "" {
		return nil, ErrEmptyTaskType
	}
	if payload == nil {
		payload = make(map[string]any)
	}
	if maxRetries < 0 {
		maxRetries = 0
	}

	now := time.Now().UTC()
	return &Task{
		ID:           GenerateUUIDv4(),
		Type:         cleanType,
		Payload:      payload,
		MaxRetries:   maxRetries,
		CurrentRetry: 0,
		Status:       StatusPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

func (t *Task) Validate() error {
	if strings.TrimSpace(t.Type) == "" {
		return ErrEmptyTaskType
	}
	if t.Payload == nil {
		return ErrNilPayload
	}
	return nil
}

func (t *Task) UnmarshalJSON(data []byte) error {
	type Alias Task
	aux := struct {
		LegacyRetries *int `json:"retries"`
		*Alias
	}{
		Alias: (*Alias)(t),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.LegacyRetries != nil && t.MaxRetries == 0 {
		t.MaxRetries = *aux.LegacyRetries
	}
	if t.ID == "" {
		t.ID = GenerateUUIDv4()
	}
	if t.Status == "" {
		t.Status = StatusPending
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now().UTC()
	}
	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = t.CreatedAt
	}
	return nil
}

func (t *Task) CalculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 100 * time.Millisecond
	}
	base := 500 * time.Millisecond
	maxBackoff := 30 * time.Second

	factor := math.Pow(2, float64(attempt-1))
	backoff := time.Duration(float64(base) * factor)
	if backoff > maxBackoff || backoff < 0 {
		backoff = maxBackoff
	}

	jittered := backoff/2 + time.Duration(mathrand.Int63n(int64(backoff/2)+1))
	return jittered
}

func GenerateUUIDv4() string {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(uuid[0:4]),
		hex.EncodeToString(uuid[4:6]),
		hex.EncodeToString(uuid[6:8]),
		hex.EncodeToString(uuid[8:10]),
		hex.EncodeToString(uuid[10:16]))
}
