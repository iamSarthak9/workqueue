.PHONY: all build test up down logs clean help

all: build

## build: Compiles producer and worker binaries into bin/
build:
	@mkdir -p bin
	go build -o bin/producer ./cmd/producer
	go build -o bin/worker ./cmd/worker

## test: Runs all unit and integration tests with data race detection
test:
	go test -v -race ./...

## up: Spins up Redis broker, Producer, and Worker services via Docker Compose
up:
	docker compose up --build -d

## down: Stops and removes all Docker Compose containers
down:
	docker compose down

## logs: Streams live logs from all services
logs:
	docker compose logs -f

## clean: Removes compiled binaries and temporary test artifacts
clean:
	rm -rf bin/
	rm -f coverage.out
