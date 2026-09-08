DATABASE_URL ?= postgres://queued:queued@localhost:5432/queued?sslmode=disable
export DATABASE_URL

.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-10s %s\n", $$1, $$2}'

.PHONY: run
run: ## run the API server + dashboard
	go run ./cmd/queued

.PHONY: worker
worker: ## run a worker process
	go run ./cmd/worker

.PHONY: migrate
migrate: ## apply database migrations
	go run ./cmd/queued -migrate

.PHONY: test
test: ## run the test suite
	go test ./...

.PHONY: race
race: ## run the test suite under the race detector
	go test -race ./...

.PHONY: lint
lint: ## run golangci-lint
	golangci-lint run ./...

.PHONY: bench
bench: ## run benchmarks
	go test -bench=. -benchmem -run '^$$' ./...

.PHONY: up
up: ## start the stack
	docker compose up -d --build

.PHONY: down
down: ## stop the stack and drop its volumes
	docker compose down -v

.PHONY: logs
logs: ## tail the stack logs
	docker compose logs -f

.PHONY: psql
psql: ## open a psql shell against the compose database
	docker compose exec postgres psql -U queued -d queued
