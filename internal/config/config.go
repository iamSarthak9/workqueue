package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	RedisURL          string
	ProducerPort      string
	WorkerPort        string
	QueueName         string
	DLQName           string
	WorkerConcurrency int
	ShutdownTimeout   time.Duration
	LogLevel          string
	LogFilePath       string
}

func Load() *Config {
	_ = godotenv.Load()

	producerPort := getEnv("PORT_PRODUCER", getEnv("PORT", "8080"))
	workerPort := getEnv("PORT_WORKER", "8081")

	concurrency := 5
	if cStr := os.Getenv("WORKER_CONCURRENCY"); cStr != "" {
		if c, err := strconv.Atoi(cStr); err == nil && c > 0 {
			concurrency = c
		}
	}

	timeoutSec := 10
	if tStr := os.Getenv("SHUTDOWN_TIMEOUT_SECONDS"); tStr != "" {
		if t, err := strconv.Atoi(tStr); err == nil && t > 0 {
			timeoutSec = t
		}
	}

	return &Config{
		RedisURL:          getEnv("REDIS_URL", "redis://localhost:6379"),
		ProducerPort:      formatPort(producerPort),
		WorkerPort:        formatPort(workerPort),
		QueueName:         getEnv("QUEUE_NAME", "workqueue:tasks"),
		DLQName:           getEnv("DLQ_NAME", "workqueue:dlq"),
		WorkerConcurrency: concurrency,
		ShutdownTimeout:   time.Duration(timeoutSec) * time.Second,
		LogLevel:          strings.ToUpper(getEnv("LOG_LEVEL", "INFO")),
		LogFilePath:       getEnv("LOG_FILE_PATH", "logs/workqueue.log"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func formatPort(port string) string {
	if !strings.HasPrefix(port, ":") {
		return ":" + port
	}
	return port
}
