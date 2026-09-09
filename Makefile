DATABASE_URL ?= postgres://queued:queued@localhost:5432/queued?sslmode=disable
export DATABASE_URL

# The benchmarks run against a real server rather than a throwaway container,
# because the container the correctness tests use has fsync off and would
# roughly double the numbers.
TEST_DATABASE_URL ?= $(DATABASE_URL)

.DEFAULT_GOAL := help

.PHONY: help
help: ## list targets
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
race: ## the gate: run the suite under the race detector
	go test -race ./...

.PHONY: lint
lint: ## run golangci-lint
	golangci-lint run ./...

# -count=1 is not optional. Without it the test cache happily returns a previous
# run's timings for an unchanged package, and a benchmark that reports numbers
# it did not measure is worse than no benchmark.
.PHONY: bench
bench: ## run the benchmarks against the compose database
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -tags bench -count=1 -v -timeout 60m \
		-run 'Test(Throughput|BatchedVsSingleClaim|ClaimAtDepth|NotifyVsPolling)' ./internal/bench/

.PHONY: loadgen
loadgen: ## enqueue continuously so the dashboard has something to show
	go run ./cmd/enqueue -kind succeed -rate 200 -for 10m

.PHONY: seed
seed: ## a mixed backlog: successes, failures, panics, and a schedule
	go run ./cmd/enqueue -kind succeed -n 2000
	go run ./cmd/enqueue -kind flaky -n 50
	go run ./cmd/enqueue -kind always-fails -n 20 -max-attempts 3
	go run ./cmd/enqueue -kind panics -n 5 -max-attempts 2
	go run ./cmd/enqueue -kind invalid-payload -n 5
	go run ./cmd/enqueue -kind succeed -delay 1h -n 10
	go run ./cmd/enqueue -kind succeed -cron '*/1 * * * *' -name every-minute

.PHONY: up
up: ## start the stack (postgres, queued, 3 workers)
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

.PHONY: image-size
image-size: ## build the image and report its size
	docker build -q -t queued:local . >/dev/null
	@docker image inspect queued:local --format 'queued:local {{.Size}} bytes' | \
		awk '{printf "%s  %.1f MB\n", $$1, $$2/1024/1024}'
