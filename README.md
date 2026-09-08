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

Phases 0–3 are done: scaffold, schema, the claim query, and the worker pool.
Everything else is marked in the source with the phase that fills it in.

## The worker pool

`internal/worker/pool.go`. One claimer goroutine feeding N executors over a
buffered channel.

**One claimer, not N.** If every executor ran its own claim query, the load on
the hot path would scale with concurrency — and the claim query is the one thing
every worker in the fleet contends on. A single claimer taking ten at a time
keeps that contention flat as concurrency goes up.

**Nothing busy-loops.** An idle claimer blocks on a three-way select: a NOTIFY
wakeup, the poll timer, or shutdown.

**Every job runs under `context.WithTimeout(ctx, job.VisibilityTimeout)`.** Past
that deadline the reaper is entitled to hand the job to somebody else, so a
handler still working past it is at best wasting effort and at worst about to
become a second concurrent execution.

**Panics are recovered per job.** A panicking handler is a bug in one job's
code, not in the queue, and it must not stop the worker from running the other
thousand. The stack goes into `last_error` — a panic with no stack is nearly
useless to whoever has to fix it.

### Retryable vs permanent errors

The question is not "did it fail" but "would running it again plausibly give a
different answer".

- **Retryable** — a timeout, a 503 from an upstream, a deadlock. Nothing about
  the job is wrong; the world was temporarily unhelpful. Full retry budget.
- **Permanent** — the payload fails validation, or names a user that does not
  exist. The next four attempts fail identically, so burning the budget on them
  only delays the operator finding out. Handlers signal it with
  `worker.Permanent(err)`, and the job goes straight to `dead`.

One case that looks permanent and deliberately is not: **an unregistered kind**.
During a rolling deploy the old workers do not yet have the handler for a kind
the new code enqueues. Treating that as permanent would dead-letter every such
job in the window between the first new enqueue and the last old worker going
away. Retrying instead means the job waits and lands on a worker that knows what
to do with it. If the kind really was a typo, it reaches the DLQ a few minutes
later — a much cheaper mistake than the other direction.

### Backoff

`base * 2^attempt` capped at `max`, with **full jitter**: the exponential curve
sets the ceiling and the actual delay is drawn uniformly from zero up to it.

The jitter is not a nicety. The failure this queue is most likely to meet is a
shared dependency going down, which fails every in-flight job at nearly the same
instant. Pure exponential backoff schedules all of those retries for the same
moment, so the recovering dependency is hit by the whole fleet at once, falls
over again, and the herd re-forms — now synchronised more tightly than before.
Spreading each retry across its window turns that spike into a flat arrival
rate. Full jitter rather than the milder "half plus jitter" because nothing here
cares whether one job waits 0.2s or 1.9s, and the herd protection is strictly
better the wider the draw.

### LISTEN/NOTIFY *and* polling

Migration 0003 adds triggers that `pg_notify('jobs_pending')`. The insert
trigger is **statement-level**, not row-level, which matters because
`EnqueueMany` goes over `COPY` — a row-level trigger would emit 50,000
notifications for one 50,000-row load. Retries and reclaims put existing rows
back to `pending` without inserting, so they get a second, row-level trigger.

The two mechanisms fail in opposite directions, which is exactly why you need
both:

| | polling | NOTIFY |
| --- | --- | --- |
| reliable? | yes — no state, survives any disconnect, cannot miss work | no — fire-and-forget, notifications sent while disconnected are simply gone |
| fast? | no — latency and idle query load are one dial | yes — woken the instant the transaction commits, costs nothing idle |

So NOTIFY collapses common-case latency from one poll interval to microseconds,
and the poll timer underneath guarantees that a missed notification costs one
interval of delay rather than a job that never runs. **The queue is correct
because of the polling and fast because of the NOTIFY.**

### Graceful shutdown

`signal.NotifyContext` cancels one context; every shutdown decision hangs off
that single cancellation.

1. **The claimer stops immediately.** No new work from the moment the signal
   lands.
2. **Claimed-but-unstarted jobs are released straight back to `pending`.** They
   have not run, so there is nothing to wait for.
3. **In-flight jobs keep running** on a context that deliberately does *not*
   inherit the cancellation, and get `DrainTimeout` (default 30s) to finish.
4. **Anything still running at the deadline is cancelled and its job released
   explicitly**, rather than left for the reaper. That turns a
   visibility-timeout-long stall into an immediate handover.

One subtlety worth naming: Go's `select` chooses at random when several cases
are ready, so a plain two-case select would have let executors keep pulling
buffered jobs after the signal for as long as the buffer lasted. The executors
check cancellation in a non-blocking select first, so "stop taking new work"
means what it says.

Real output from `docker compose stop worker`, with 60 ten-second jobs in
flight:

```json
{"level":"INFO","msg":"shutdown: released jobs back to pending","worker_id":"b73fa79b9431-1","count":2,"reason":"worker shut down before dispatch"}
{"level":"INFO","msg":"shutdown: claimer stopped, draining in-flight jobs","worker_id":"b73fa79b9431-1"}
{"level":"INFO","msg":"shutdown: releasing claimed-but-unstarted jobs","worker_id":"b73fa79b9431-1","count":10}
{"level":"INFO","msg":"shutdown: released jobs back to pending","worker_id":"b73fa79b9431-1","count":10,"reason":"worker shut down before the job started"}
{"level":"INFO","msg":"shutdown: all in-flight jobs finished","worker_id":"b73fa79b9431-1"}
{"level":"INFO","msg":"shutdown complete","worker_id":"b73fa79b9431-1"}
```

Final state: 16 succeeded, 44 back in `pending`, **0 left in `claimed`**, all 60
accounted for. The whole drain took 7.8s — the length of the longest job still
running, not the 30s deadline.

### The handlers

`testdata/handlers` has six, each existing because some property needs a job
that behaves that way: one that succeeds, one that always fails, one that fails
twice then succeeds, one that runs past its visibility timeout, one that panics,
and one that rejects its payload permanently.

Note that the go tool skips `testdata/` when expanding `./...`, so that package
is not built by `go build ./...`. It is compiled by the tests that import it.

## The claim query

`internal/queue/store.go`. This is the core of the project.

```sql
WITH candidate AS (
    SELECT id FROM jobs
    WHERE state = 'pending'
      AND queue = $1
      AND run_at <= now()
    ORDER BY priority ASC, run_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $3
), claimed AS (
    UPDATE jobs SET
        state            = 'claimed',
        claimed_at       = now(),
        claimed_by       = $2,
        claim_expires_at = now() + make_interval(secs => visibility_timeout_seconds),
        attempt          = attempt + 1
    WHERE id IN (SELECT id FROM candidate)
    RETURNING ...
), history AS (
    INSERT INTO job_attempts (job_id, attempt, worker_id, started_at)
    SELECT id, attempt, $2, now() FROM claimed
)
SELECT ... FROM claimed ORDER BY priority ASC, run_at ASC
```

**Why `SKIP LOCKED`.** Without it, a worker whose index scan reaches a row
another worker has locked *blocks* until that worker's transaction ends. Every
worker then queues up behind whichever one got to the head of the queue first,
and N workers deliver the throughput of one — worse, actually, since you now pay
for the lock waits too. With `SKIP LOCKED` a locked row is stepped over and the
scan keeps going, so each worker walks away with a disjoint set and no worker
ever waits on another. This one clause is the entire reason Postgres is a
viable queue rather than a bottleneck pretending to be one.

**Why the subquery.** `FOR UPDATE` is what makes the choice of rows exclusive,
and it can only be attached to a `SELECT`. You cannot write
`UPDATE ... ORDER BY ... LIMIT ... FOR UPDATE`. So the selection happens in a
subquery that takes the locks, and the `UPDATE` operates on the ids it returns.

**Why `attempt` increments at claim time, not at failure time.** A worker that
is SIGKILLed, loses its network, or wedges never reports anything at all. A
counter that only advanced on a *reported* failure would therefore never advance
for exactly the jobs most likely to be killing workers, and a poison job would
be reclaimed forever. Charging the attempt up front means such a job still walks
to the DLQ. The cost is real — a worker restarted mid-job makes its job pay for
the interruption — and it is the right trade: bounded retries (property 4)
matter more than squeezing a last attempt out of an unlucky job.

**Why the attempt row is written in the same statement.** If the worker inserted
into `job_attempts` after the claim returned, a worker that died in between
would leave no trace of having taken the job at all. Writing it inside the claim
means every claim is on the record before anyone can act on it.

### The reaper's index

Migration 0002 adds `claim_expires_at`. The natural way to write the reaper is

```sql
WHERE state = 'claimed'
  AND claimed_at + make_interval(secs => visibility_timeout_seconds) < now()
```

which cannot use an index: the timeout is per-row, so the comparison spans two
columns of the same row and the planner has to sequential-scan to evaluate it.
That is fine on a small table and quietly becomes the most expensive thing in
the system on a large one — and the reaper runs every few seconds, forever.
Materialising the deadline at claim time turns it into a range scan. With 20,050
claimed rows of which 55 are expired:

```
Limit (actual time=0.068..0.081 rows=55 loops=1)
  ->  LockRows
        ->  Sort
              ->  Bitmap Heap Scan on jobs
                    ->  Bitmap Index Scan on jobs_reap_idx (actual time=0.007..0.007 rows=55)
                          Index Cond: (claim_expires_at < now())
Execution Time: 0.116 ms
```

It touches the 55 expired rows, not the 20,000 live ones.

### Guarding against stale claims

`Complete`, `Fail` and `Release` all carry `AND claimed_by = $2`. This is not
decoration. If the reaper has already decided a worker is dead and handed its
job to somebody else, the slow worker coming back to report must not be allowed
to mark the job succeeded while the new owner is still running it. The update
matches nothing and the caller gets `ErrStaleClaim` instead.

### What is proven so far

`go test -race ./internal/queue` — 15 tests, all against a real Postgres started
by testcontainers (see `internal/testutil`). Nothing is mocked: SKIP LOCKED
handing disjoint sets to concurrent transactions *is* the mechanism, and a fake
would only prove that the fake agrees with itself.

| test | what it pins down |
| --- | --- |
| `ClaimHandsOutDisjointSets` | 1000 jobs, 20 concurrent claimers, no id returned twice, all 1000 accounted for (property 1) |
| `ClaimSkipsAlreadyClaimed` | a claimed job is invisible to the next claimer |
| `ClaimIncrementsAttemptAndRecordsHistory` | attempt moves at claim, expiry is set, the attempt row exists immediately |
| `ClaimRespectsPriorityThenRunAt` | ordering is honoured |
| `ClaimIgnoresFutureAndOtherQueues` | delayed jobs and other queues stay invisible |
| `CompleteClosesTheAttempt` | success closes the history row |
| `CompleteAndFailRejectStaleClaims` | a worker cannot report on a job it no longer holds |
| `FailSchedulesRetryThenGivesUp` | retries, then `dead` after exactly `max_attempts`, and no more (property 4) |
| `FailPermanentSkipsTheRetryBudget` | a permanent error goes straight to `dead` |
| `ReapReturnsExpiredClaims` | an unreported claim comes back and another worker runs it (property 2) |
| `ReapSendsExhaustedJobsToTheDLQ` | a job that keeps killing workers still stops |
| `ReleaseReturnsJobsImmediately` | explicit handback, no visibility-timeout wait (backs property 3) |
| `EnqueueIdempotencyKeyDedupes` | two enqueues, one row |
| `EnqueueIdempotencyKeyUnderRace` | 16 concurrent enqueues with one key: one row, one reported insert |
| `EnqueueRejectsBadInput` | missing kind, bad JSON, empty key, zero max_attempts |

`ClaimHandsOutDisjointSets` also passes under `-race -count=10`.

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
