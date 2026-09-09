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

## Still missing

**LISTEN/NOTIFY versus 100 ms polling throughput.** The mechanism is implemented
and its correctness is tested — `TestNotifyWakesTheClaimerBeforeThePollInterval`
proves a job is picked up in under a second with a 30-second poll interval, so
NOTIFY is demonstrably the thing finding the work — but the *throughput*
comparison between the two is not in the harness.

The honest expectation is that it makes very little difference to throughput and
a large difference to latency. On a busy queue the claimer never sleeps: it
reclaims a full batch and loops straight back without ever reaching the select,
so the notification path is not exercised. NOTIFY earns its place on a queue
that is mostly idle, where it turns "up to one poll interval" into
"microseconds" — a latency win, not a throughput one. That is a prediction, and
it is the next thing to measure.

## What these numbers justify

Roughly **10,000 jobs/sec** on a laptop VM, with the knee at 4–16 workers per
database. For the overwhelming majority of background-job workloads — sending
email, generating reports, processing uploads, calling webhooks — that is
comfortably more than enough, and it comes without operating a broker.

If you need more, the ordering is: fewer writes per job first (drop the
`job_attempts` close, or write history asynchronously), then partition `jobs`,
then shard. Reaching for Kafka before doing any of that is a decision to operate
a second stateful system in exchange for headroom you have not yet needed.
