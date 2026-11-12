# WorkQueue

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Redis](https://img.shields.io/badge/Redis-7.x-DC382D?style=flat&logo=redis)](https://redis.io/)
[![Docker Ready](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat&logo=docker)](https://www.docker.com/)
[![CI](https://img.shields.io/badge/Build-Passing-brightgreen?style=flat&logo=githubactions)](https://github.com/iamSarthak9/workqueue/actions)

A high-performance, distributed, asynchronous background task processing engine written in **Go**, powered by **Redis** as a distributed queue broker. Built with clean architecture, concurrency safety, exponential backoff retries, dead-letter queueing (DLQ), and structured telemetry.

---

## 🏛️ System Architecture

```text
  [ HTTP Clients / Services ]
              │
              ▼  POST /enqueue
     ┌──────────────────┐
     │ Producer Service │  (Input Validation, RFC 4122 UUID v4, JSON API)
     └────────┬─────────┘
              │  RPUSH
              ▼
  ┌────────────────────────────────────────────────────────┐
  │                 Redis Queue Broker                     │
  │  • workqueue:tasks (Main Queue List)                   │
  │  • workqueue:dlq   (Dead Letter Queue)                 │
  └────────┬───────────────────────────────────────────────┘
           │  BLPOP (Context-aware polling)
           ▼
  ┌────────────────────────────────────────────────────────┐
  │                 Worker Pool Daemon                     │
  │  ┌──────────────┐   ┌──────────────┐   ┌────────────┐  │
  │  │ Worker Goroutine 1 │   │ Worker Goroutine 2 │   │ Worker N   │  │
  │  └──────┬───────┘   └──────┬───────┘   └─────┬──────┘  │
  │         │                  │                 │         │
  │         ▼                  ▼                 ▼         │
  │  ┌──────────────────────────────────────────────────┐  │
  │  │       Pluggable Task Handler Registry            │  │
  │  │  • Handler interface   • Per-task timeout ctx    │  │
  │  │  • send_email          • resize_image            │  │
  │  │  • generate_pdf        • webhook_dispatch        │  │
  │  └─────────────────────────┬────────────────────────┘  │
  │                            │                           │
  │           ┌────────────────┴────────────────┐          │
  │           ▼                                 ▼          │
  │     [ Success ]                     [ Failure ]        │
  │   - Atomic Stats                  - Exponential Backoff│
  │   - Structured Log                - Exhausted -> DLQ   │
  └────────────────────────────┬───────────────────────────┘
                               │
                               ▼  GET /metrics | GET /healthz
                 [ Atomic Telemetry & Health ]
```

---

## ✨ Key Features & Engineering Highlights

- **Decoupled Asynchronous Processing**: Offload compute-intensive or IO-bound operations (emails, media encoding, report generation, webhooks) from critical user request-response paths.
- **Pluggable Task Registry**: Extensible `Handler` interface enabling registration of custom business logic with per-task context cancellation and timeout controls.
- **Concurrency & Lock-Free Thread Safety**: High-throughput worker pool utilizing goroutines and standard library `sync/atomic` counters, preventing race conditions under high concurrency.
- **Intelligent Fault Tolerance**: Exponential backoff with randomized jitter to prevent thundering herd problems against downstream dependencies.
- **Dead Letter Queue (DLQ)**: Tasks that exceed their configured max retries are automatically routed to `workqueue:dlq` with complete failure reasons and stack traces for inspection.
- **Graceful Shutdown**: Traps `SIGINT` and `SIGTERM` OS signals, halts consumer loops, and drains in-flight tasks within a configurable timeout window before process exit.
- **Structured JSON Observability**: Standard library `log/slog` structured logging with concurrent-safe file and stdout sinks.
- **Production DevOps Tooling**: Multi-stage Docker containerization, Docker Compose orchestration, Makefile automation, and automated GitHub Actions CI pipeline.

---

## 🚀 Quick Start (Local Setup)

### Option 1: Run with Docker Compose (Recommended)

Start the entire distributed stack (Redis + Producer + Worker) with one command:

```bash
docker compose up --build -d
```

To stop all services:
```bash
docker compose down
```

### Option 2: Run with Makefile
```bash
# Start Docker stack
make up

# Run test suite with race detector
make test

# Build local binaries
make build
```

### Option 3: Run Directly with Go
```bash
# Terminal 1: Start Producer Service (port :8080)
go run ./cmd/producer

# Terminal 2: Start Worker Daemon (port :8081)
go run ./cmd/worker
```

---

## 📡 API Reference

### 1. Enqueue Task (`POST /enqueue`)
Submits a background job to the queue.

- **URL**: `http://localhost:8080/enqueue`
- **Method**: `POST`
- **Header**: `Content-Type: application/json`

#### Request Payload:
```json
{
  "type": "send_email",
  "max_retries": 3,
  "payload": {
    "to": "sarthaksharma991145@gmail.com",
    "subject": "Distributed Worker Notification"
  }
}
```

#### Response (`202 Accepted`):
```json
{
  "status": "success",
  "message": "Task of type 'send_email' enqueued successfully",
  "task": {
    "id": "e9b28a25-c995-46f3-9fc0-fae910bd3c50",
    "type": "send_email",
    "payload": {
      "to": "sarthaksharma991145@gmail.com",
      "subject": "Distributed Worker Notification"
    },
    "max_retries": 3,
    "current_retry": 0,
    "status": "pending",
    "created_at": "2026-11-12T17:00:00Z",
    "updated_at": "2026-11-12T17:00:00Z"
  }
}
```

---

### 2. Worker Telemetry & Metrics (`GET /metrics`)
Exposes live operational metrics across queues and active worker pools.

- **URL**: `http://localhost:8081/metrics`
- **Method**: `GET`

#### Response (`200 OK`):
```json
{
  "total_jobs_in_queue": 0,
  "jobs_done": 142,
  "jobs_failed": 3,
  "jobs_received": 145,
  "jobs_retried": 5,
  "jobs_dead_lettered": 1,
  "active_workers": 5,
  "dlq_depth": 1,
  "uptime_seconds": 3600
}
```

---

### 3. Health Probes (`GET /healthz`)
Available on both Producer (`:8080/healthz`) and Worker (`:8081/healthz`) for container health checks.

```json
{
  "service": "workqueue-producer",
  "status": "healthy",
  "redis": "connected",
  "timestamp": "2026-11-12T17:00:00Z"
}
```

---

## 🛠️ Registering Custom Tasks

Adding custom tasks is modular and clean. Register a handler with `worker.DefaultRegistry()`:

```go
package main

import (
    "context"
    "fmt"
    "github.com/iamSarthak9/workqueue/internal/task"
    "github.com/iamSarthak9/workqueue/internal/worker"
)

func main() {
    worker.DefaultRegistry().RegisterFunc("generate_report", func(ctx context.Context, t *task.Task) error {
        userID := t.Payload["user_id"]
        fmt.Printf("Generating monthly report for user %v\n", userID)
        return nil
    })
}
```

---

## 🧪 Testing & Verification

Run the full automated test suite with Go's race condition detector:

```bash
go test -v -race ./...
```

---

## ⚙️ Configuration Reference

| Environment Variable | Default Value | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | Redis broker endpoint |
| `PORT_PRODUCER` | `8080` | Enqueue API port |
| `PORT_WORKER` | `8081` | Metrics & telemetry port |
| `QUEUE_NAME` | `workqueue:tasks` | Main FIFO queue key |
| `DLQ_NAME` | `workqueue:dlq` | Dead Letter Queue key |
| `WORKER_CONCURRENCY` | `5` | Number of concurrent worker goroutines |
| `SHUTDOWN_TIMEOUT_SECONDS` | `10` | Timeout window for draining in-flight tasks |
| `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARN`, `ERROR`) |
| `LOG_FILE_PATH` | `logs/workqueue.log` | Persistent structured log destination |

---

## 👤 Author

**Sarthak Sharma**
- GitHub: [@iamSarthak9](https://github.com/iamSarthak9)
- Email: sarthaksharma991145@gmail.com
