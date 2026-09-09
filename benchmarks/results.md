# Benchmark results

Measured with `make bench` (`internal/bench`, behind the `bench` build tag).

**Machine and setup, because a queue benchmark without them is a decoration:**

- Apple Silicon laptop, macOS 15, Docker Desktop with ~4 GB allocated to the VM
- Postgres 16.15 (alpine) in that VM, **default settings, `fsync` on**
- Client (the benchmark) on the host, talking over the published port
- Workers stopped during the runs, so nothing competes for the jobs

That last detail about `fsync` matters. The correctness suite uses a throwaway
testcontainers Postgres with `fsync=off`, which roughly doubles write throughput
on this workload. These numbers deliberately do not use it.

Two caveats on the numbers below. The Postgres is containerised on a laptop, so
these are a **floor, not a ceiling** — real hardware with real disks and Postgres
on bare metal will do considerably better. And the client crosses the VM
boundary for every query, which inflates latency relative to a worker running
alongside the database.

---

## Throughput, no-op handler

20,000 jobs per run. Each "worker" is an independent claim/complete loop with
its own connections, claiming in batches of 10.

| workers | jobs/sec | claim p50 | claim p95 | claim p99 |
| ---: | ---: | ---: | ---: | ---: |
| 1  | 2,520  | 587 µs  | 789 µs   | 1.06 ms |
| 4  | 6,936  | 721 µs  | 976 µs   | 1.47 ms |
| 16 | 8,719  | 1.87 ms | 3.35 ms  | 14.1 ms |
| 64 | 10,672 | 6.14 ms | 21.6 ms  | 109 ms  |

### Where it stops scaling, and why

**Between 4 and 16 workers.** 1→4 workers is a 2.75× gain on 4× the
concurrency — close to linear. 4→16 is 4× the concurrency for **1.26×** the
throughput. 16→64 is another 4× for **1.22×**.

Meanwhile latency goes the other way, and almost exactly in proportion to the
worker count: p50 climbs 587 µs → 721 µs → 1.87 ms → 6.14 ms. Between 16 and 64
workers, p50 grows 3.3× while throughput grows 1.22×.

That shape — throughput flat, latency rising linearly with concurrency — is the
signature of a saturated server. Past the knee, additional workers are not doing
more work; they are queueing, and each one's added wait is what shows up in the
percentiles. The p99 at 64 workers (109 ms, versus a 6 ms p50) is the tail of
that queue.

**It is Postgres, not Go.** The Go side is a few goroutines per worker doing
almost nothing; the process is idle. The limit is the write path, and the
arithmetic explains it: **every job costs four row versions.**

1. `UPDATE jobs` to claim it
2. `INSERT job_attempts` for the attempt row
3. `UPDATE jobs` again to complete it
4. `UPDATE job_attempts` to close the attempt

Postgres has no in-place update — each of those writes a new row version and
leaves the old one for autovacuum. At 10,000 jobs/sec that is 40,000 row
versions per second being written and then garbage collected, all against the
same two tables. WAL and autovacuum are the ceiling.

Note what is **not** the limit: lock contention. `SKIP LOCKED` means no worker
ever waits for another, so the curve flattens rather than collapsing. Without
it, this table would show throughput *falling* as workers were added.

## Batched vs single claim

8 workers, 10,000 jobs, identical in every respect except the claim batch size.

| batch | jobs/sec | |
| ---: | ---: | --- |
| 1  | 4,589 | one query per job |
| 10 | 8,431 | one query per ten jobs |

**Batching is worth 1.84×.** Ten jobs per claim replaces ten round trips, ten
transactions and ten index scans with one of each. The gain is less than 10×
because the completion write is still per-job — the claim is now amortised but
`Complete` is not, which is exactly where the remaining round trips are.

## Queue depth: does the claim query hold up at 1M rows?

| depth | table | `jobs_claim_idx` | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 320 kB | 16 kB | 566 µs | 683 µs | 765 µs |
| 1,000,000 | 216 MB | 6.3 MB | 589 µs | 779 µs | 1.04 ms |

**A thousand times the rows costs 4% on p50.** That is the partial index doing
the single job it exists for.

`EXPLAIN ANALYZE` at 1,000,002 rows:

```
 Limit (actual time=0.100..0.104 rows=10 loops=1)
   Buffers: shared hit=16
   ->  LockRows (actual time=0.099..0.103 rows=10 loops=1)
         Buffers: shared hit=16
         ->  Index Scan using jobs_claim_idx on jobs (actual time=0.055..0.056 rows=10 loops=1)
               Index Cond: ((queue = 'default'::text) AND (run_at <= now()))
               Filter: (state = 'pending'::job_state)
               Buffers: shared hit=6
 Planning Time: 0.444 ms
 Execution Time: 0.132 ms
```

**Six buffers touched to find ten rows out of a million.** No `Sort` node — the
index supplies the `ORDER BY priority, run_at` ordering, so `LIMIT 10` reads what
it needs and stops. The query does not care how deep the queue is, because it
never looks past the first ten index entries.

The gap between the 0.132 ms server-side execution and the 589 µs the client
measures is round trip: the connection crosses the Docker VM boundary. On a
worker sitting next to its database, expect the client-side number to be much
closer to the server-side one.

### The earlier 600k measurement, for comparison

Taken during phase 1 on a table with 500k succeeded rows as well as the pending
ones, so a more realistic mix than the pure-backlog table above:

| index | size |
| --- | --- |
| partial, `WHERE state = 'pending'` | 912 kB |
| same columns, unfiltered | 20 MB |

Table 124 MB. The partial index tracks the size of the *backlog*, not the size
of the *table* — which is what keeps it cache-resident when the table is tens of
gigabytes.

## Other measured numbers

- **Bulk enqueue over `COPY`:** 50,000 jobs in 335 ms (~150,000/sec).
- **Reaper scan**, 20,050 claimed rows of which 55 expired: `Bitmap Index Scan on
  jobs_reap_idx`, **0.116 ms**. It touches the 55, not the 20,000.
- **Graceful shutdown**, 4 in-flight 5-second jobs, real SIGTERM to the real
  binary: drained in **4.03 s**, exit 0, nothing left in `claimed`.

## LISTEN/NOTIFY versus polling at 100 ms

The interesting one, because the two do not compete on the same axis.

### Throughput on a saturated queue: no measurable difference

20,000 jobs, 8 executors, batches of 10. Six runs each, alternating which went
first, `-count=1` throughout.

| run | notify | polling 100 ms |
| ---: | ---: | ---: |
| 1 | 7,766 | 7,001 |
| 2 | 6,144 | 6,875 |
| 3 | 8,121 | 6,392 |
| 4 | 6,534 | 6,745 |
| 5 | 7,664 | 7,828 |
| 6 | 6,704 | 7,335 |
| **mean** | **7,156** | **7,029** |
| range | 6,144–8,121 | 6,392–7,828 |

**1.8% apart on the means, with ranges that overlap almost completely.** There is
no throughput difference here; run-to-run variance is several times larger than
the gap.

That is the predicted result, and the reason is structural: a claimer with work
waiting reclaims a full batch and loops straight back round. It only reaches the
select — the only place a notification can be read — when a batch comes back
short. On a saturated queue that never happens, so NOTIFY has nothing to
contribute.

Worth recording how nearly this went wrong. The first single run showed notify
ahead by 18%, which looked like a real finding. It was ordering and noise: with
n=1 and polling running first on a cold cache, the gap was an artifact. Three
repeats then produced *byte-identical* numbers, which is not variance but Go's
test cache returning a previous run's timings for an unchanged package. `make
bench` now passes `-count=1`, and it is not optional — a benchmark reporting
numbers it did not measure is worse than no benchmark at all.

### Latency on an idle queue: 9.4× better

One job at a time into an empty queue, waiting for each to be picked up before
enqueueing the next, so every sample starts from a genuinely idle claimer. 40
samples. Measured from `Enqueue` returning to the handler being entered.

| | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| polling 100 ms | 55.1 ms | 58.3 ms | 58.7 ms |
| **notify** | **5.9 ms** | **11.3 ms** | **12.9 ms** |

**9.4× lower median.** And the polling number is a satisfying confirmation that
the model is right rather than the measurement being lucky: a job arriving at a
uniformly random point in a 100 ms poll cycle waits 50 ms on average, and the
measured p50 is 55 ms. The p95/p99 sit just under 60 ms because the wait is
bounded by the interval — polling's latency distribution is flat-topped, not
long-tailed.

NOTIFY's 5.9 ms is round trip plus scheduling, not waiting.

### What the pair is actually for

The two numbers together are the whole argument for carrying both mechanisms:

- **NOTIFY buys latency, not throughput.** It is worth nothing on a busy queue
  and nearly an order of magnitude on an idle one — which is the state most
  queues are in most of the time.
- **Polling buys correctness, not latency.** It costs nothing measurable in
  throughput, and it is what makes a missed notification a 100 ms delay instead
  of a job that never runs.

Dropping polling to chase NOTIFY's latency would trade a guarantee for something
already had. Dropping NOTIFY and shortening the poll interval to chase its
latency would mean querying the database ten or a hundred times more often while
idle, to approximate a wakeup that costs one connection.

## What these numbers justify

Roughly **10,000 jobs/sec** on a laptop VM, with the knee at 4–16 workers per
database. For the overwhelming majority of background-job workloads — sending
email, generating reports, processing uploads, calling webhooks — that is
comfortably more than enough, and it comes without operating a broker.

If you need more, the ordering is: fewer writes per job first (drop the
`job_attempts` close, or write history asynchronously), then partition `jobs`,
then shard. Reaching for Kafka before doing any of that is a decision to operate
a second stateful system in exchange for headroom you have not yet needed.
