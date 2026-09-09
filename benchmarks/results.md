# Benchmark results

**Not yet populated.** The harness is written (`internal/bench`, behind the
`bench` build tag) and vets clean, but the machine this was developed on ran out
of disk before the numbers could be taken — Docker's VM image is 15 GB and the
host was left with under 1 GB free, which took the Docker daemon down with it.

Rather than fabricate plausible-looking numbers, this file records what the
harness measures and how to run it.

## Running it

```sh
make up      # compose Postgres, fsync ON — see the note below
make bench
```

Results land in the test log. Paste them here with the machine spec.

The benchmarks run against the compose database rather than the testcontainers
one used by the correctness suite. That container runs with `fsync=off`, which
roughly doubles write throughput on this workload and would make every number
here an overstatement.

Record alongside any numbers: CPU, RAM, whether Postgres is containerised, and
the Postgres version. A queue benchmark without the hardware is a decoration.

## What it measures

| test | question |
| --- | --- |
| `TestThroughput` | jobs/sec end to end with a no-op handler at 1, 4, 16, 64 workers, plus claim p50/p95/p99 at each level |
| `TestBatchedVsSingleClaim` | what batching ten claims per query is actually worth against one-at-a-time |
| `TestClaimAtDepth` | whether the claim query holds up at 1M rows, with index size and `EXPLAIN ANALYZE` at 1k and 1M |

A no-op handler on purpose: any real handler measures the handler, and the
question is what the *queue* costs.

Still to add: LISTEN/NOTIFY versus 100ms polling. The mechanism is implemented
and tested for correctness (`TestNotifyWakesTheClaimerBeforeThePollInterval`
proves NOTIFY beats a 30-second poll interval), but the throughput comparison
between the two is not in the harness yet.

## Measurements already taken during development

These are real, taken on Docker Desktop on an M-series laptop with 3.9 GB
allocated to the VM. They are not the phase 8 numbers, but they are not nothing.

**Claim query, 600k rows (100k pending), `LIMIT 10`:**

```
 Update on jobs (actual time=3.380..3.906 rows=10 loops=1)
   ->  Nested Loop
         ->  HashAggregate
               ->  Limit (actual time=0.078..0.227 rows=10)
                     ->  LockRows (actual time=0.077..0.225 rows=10)
                           ->  Index Scan using jobs_claim_idx  (actual time=0.042..0.148 rows=10)
                                 Index Cond: ((queue = 'default') AND (run_at <= now()))
                                 Buffers: shared hit=12
 Execution Time: 4.047 ms
```

Warm repeats: **0.8–1.1 ms**. No `Sort` node — the index supplies the ordering,
so `LIMIT 10` touches twelve buffers and stops.

**Reaper scan, 20,050 claimed rows of which 55 expired:** `Bitmap Index Scan on
jobs_reap_idx`, **0.116 ms**.

**Bulk enqueue over `COPY`:** 50,000 jobs in **335 ms** (~150k/sec).

**Partial index size**, 500k succeeded + 100k pending, 124 MB table:

| index | size |
| --- | --- |
| partial, `WHERE state = 'pending'` | 912 kB |
| same columns, unfiltered | 20 MB |

## The prediction to check

Throughput should stop scaling at Postgres, not Go. The specific expectation:
the limit is write amplification, not lock contention. Each job costs two
`UPDATE`s to the same row (claim, then complete) plus a `job_attempts` insert
and its later update — several row versions per job for autovacuum to clean up.
`SKIP LOCKED` means workers never block each other, so the ceiling should show
up as WAL throughput and autovacuum falling behind rather than as lock waits.

If the numbers show contention instead, that prediction is wrong and the claim
query needs another look — which is the point of measuring.
