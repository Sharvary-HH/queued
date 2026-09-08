# queued

A durable background job queue backed by Postgres alone. No Redis, no broker.
Jobs are enqueued into a table, claimed by a pool of workers with
`SELECT ... FOR UPDATE SKIP LOCKED`, retried with exponential backoff, and
parked in a dead-letter queue when they will never succeed.

## Correctness properties

These are the reasons the project exists. Each one gets a test in
`internal/...` once the matching phase lands.

1. **No double execution under concurrency.** Two workers never claim the same
   job.
2. **No lost jobs on worker death.** A worker that dies mid-job has the job
   reclaimed after its visibility timeout.
3. **No lost jobs on graceful shutdown.** On SIGTERM the claimer stops, in-flight
   jobs drain, anything still running at the deadline is released back to
   `pending`.
4. **Bounded retries.** A job that keeps failing lands in the DLQ after exactly
   `max_attempts`.
5. **At-least-once, not exactly-once.** Handlers must be idempotent.

## Status

Phase 0 (scaffold) is done: layout, build, lint config, and a compose stack with
Postgres. Everything else is marked in the source with the phase that fills it
in.

## Running it

```
docker compose up -d --build   # or: make up
curl localhost:8080/healthz
curl localhost:8080/readyz
```

`make help` lists the rest.

## Configuration

Everything is environment driven; see `.env.example`.
