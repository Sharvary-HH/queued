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

Phases 0 and 1 are done: layout, build, lint config, a compose stack with
Postgres, and the schema. Everything else is marked in the source with the phase
that fills it in.

## Schema

Three tables in `migrations/0001_init.sql`:

| table | what it is |
| --- | --- |
| `jobs` | the queue itself, one row per job |
| `job_attempts` | append-only history, one row per try |
| `recurring_jobs` | cron definitions |

`state` is a real Postgres enum (`pending`, `claimed`, `succeeded`, `failed`,
`dead`) rather than a text column, so a typo in a query is an error instead of a
job that quietly never runs again.

Two check constraints are worth calling out. `jobs_claim_fields` says a job in
`claimed` must have both `claimed_at` and `claimed_by` set — without that, the
reaper has no way to decide whether a claim has expired, and a job could be lost
exactly the way property 2 forbids. `jobs_max_attempts_positive` keeps
`max_attempts >= 1`, since zero would mean a job that goes straight to the DLQ
without ever running.

`job_attempts` is deliberately **not** unique on `(job_id, attempt)`. That
constraint looks like it would enforce property 1, but it would not: if two
workers really did claim the same row they would each get a *different* attempt
number (attempt increments at claim time), so the insert would succeed and the
bug would be invisible. Worse, the constraint would make the double-execution
test pass by construction instead of by observation. The test counts executions
instead.

### Migrations

Forward-only, applied by a ~200 line runner in `internal/migrate`. It takes a
`pg_advisory_lock` first, so `docker compose up` starting the API and three
workers simultaneously does not have four processes racing to create the same
table — the first one migrates, the rest block and then find nothing to do. Each
migration runs in its own transaction (Postgres has transactional DDL, so a
failure halfway leaves nothing behind), and the file's SHA-256 is recorded so
editing a migration that has already been applied is a hard error rather than a
silent schema drift.

```
make migrate        # or: go run ./cmd/queued -migrate
```

There are no down migrations. Rolling a migration back in production is mostly a
lie — you cannot un-drop a column's data — and locally `make down` throws the
volume away, which is faster and more honest than a rollback path nobody tests.

### Why the claim index is partial

```sql
CREATE INDEX jobs_claim_idx ON jobs (queue, priority, run_at)
    WHERE state = 'pending';
```

Only `pending` rows can ever be claimed, and a healthy queue is mostly history —
the backlog is small, the pile of finished jobs is not. Measured on a table
seeded with 500k `succeeded` and 100k `pending` rows:

| index | size |
| --- | --- |
| partial, `WHERE state = 'pending'` | **912 kB** |
| same columns, no `WHERE` | 20 MB |

The whole table is 124 MB. The partial index tracks the size of the *backlog*
rather than the size of the *table*, which is what keeps it in `shared_buffers`
when the table is tens of gigabytes.

The write side matters just as much. A job's life is
`pending -> claimed -> succeeded`. With a full index every one of those
transitions rewrites an index entry. With the partial index, leaving `pending`
deletes the entry and it never comes back — finished jobs stop costing anything
on write, forever.

Column order follows the query: equality on `queue` first, then the two
`ORDER BY` columns in order, so the planner walks the index in order and stops
at `LIMIT` instead of sorting.

### EXPLAIN ANALYZE on the claim query

600k rows, 100k of them pending, `LIMIT 10`, run inside a transaction that is
rolled back:

```
 Update on jobs (actual time=3.380..3.906 rows=10 loops=1)
   Buffers: shared hit=264 read=3 dirtied=2 written=1
   ->  Nested Loop (actual time=0.300..0.673 rows=10 loops=1)
         ->  HashAggregate (actual time=0.241..0.244 rows=10 loops=1)
               Group Key: "ANY_subquery".id
               ->  Subquery Scan on "ANY_subquery" (actual time=0.083..0.236 rows=10 loops=1)
                     ->  Limit (actual time=0.078..0.227 rows=10 loops=1)
                           ->  LockRows (actual time=0.077..0.225 rows=10 loops=1)
                                 ->  Index Scan using jobs_claim_idx on jobs jobs_1 (actual time=0.042..0.148 rows=10 loops=1)
                                       Index Cond: ((queue = 'default'::text) AND (run_at <= now()))
                                       Filter: (state = 'pending'::job_state)
                                       Buffers: shared hit=12
         ->  Index Scan using jobs_pkey on jobs (actual time=0.042..0.042 rows=1 loops=10)
 Planning Time: 2.547 ms
 Trigger jobs_touch_updated_at: time=0.503 calls=10
 Execution Time: 4.047 ms
```

The two things to look for:

- **`Index Scan using jobs_claim_idx`, and no `Sort` node.** The index supplies
  the `ORDER BY priority, run_at` ordering directly, so `LIMIT 10` reads twelve
  buffers and stops. It does not sort 33k candidate rows to throw away 33k of
  them.
- **`LockRows` sits above the index scan, under the `Limit`.** That is
  `SKIP LOCKED` doing its job: rows already locked by another worker are stepped
  over during the scan, so the scan keeps going until it has ten it can actually
  have.

Warm repeats land at **0.8–1.1 ms execution**. The 4 ms above is the first,
cold run, and roughly 0.5 ms of it is the `updated_at` trigger.

## Running it

```
docker compose up -d --build   # or: make up
curl localhost:8080/healthz
curl localhost:8080/readyz
```

`make help` lists the rest.

## Configuration

Everything is environment driven; see `.env.example`.
