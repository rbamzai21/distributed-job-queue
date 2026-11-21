# Distributed Job Queue

A distributed job queue built from scratch: a REST API accepts jobs, PostgreSQL stores them durably,
and a pool of worker processes claims and runs them. Failed jobs retry with exponential backoff, and
jobs that keep failing move to a dead-letter queue for inspection. Think of it as a small SQS or Celery.

**Stack:** Java 21, Spring Boot 3, PostgreSQL 17, Redis 7, Flyway, Micrometer + Prometheus,
React + TypeScript (Vite), Docker Compose, JUnit + Testcontainers, k6.

## Architecture

```
                                  ┌──────────── Postgres (jobs) ◄──── source of truth ─────┐
            POST /jobs            │ INSERT                                                 │ claim by id /
 client ──────────────►  api ─────┤                                                        │ SKIP LOCKED sweep
            GET /jobs             │ ZADD (after commit)                                    │ finish / retry
                                  └──────────► Redis  ──── BZPOPMIN ────────────────►  worker ×N
                                              ready / delayed sets                    ├─ email, cpu, flaky,
                                              (index only)                            └─ noop handlers
```

In the default `postgres` mode there is no Redis step: workers poll Postgres with `SKIP LOCKED`.
`QUEUE_MODE=redis` adds the fast path above.

| Module | What it does |
|---|---|
| `common/` | `Job` model, row mapping, backoff calculation, the Redis ready-queue index |
| `api/` | REST endpoints; runs the Flyway migrations on startup |
| `worker/` | Claim loop and claim sources (Postgres / Redis), handlers, leases, reaper, retry/dead-letter logic |
| `dashboard/` | React + TypeScript dashboard, served by nginx, which also proxies `/api` to the API |
| `db/migrations/` | Flyway SQL migrations |
| `load-tests/` | k6 load script and the benchmark harness |

## Running it

Requires Docker. Nothing else needs to be installed locally.

```bash
docker compose up -d --build                     # Postgres polling: Postgres, Redis, API on :8080, 2 workers
QUEUE_MODE=redis docker compose up -d --build    # Redis fast path
```

The dashboard runs at [localhost:3000](http://localhost:3000). It shows the queue at a glance (ready
backlog and its age, running jobs, throughput, dead-letter count, latency percentiles), a live table of
recent jobs filterable by status, the dead-letter queue with per-job and bulk **Retry**, and a form for
enqueueing demo jobs. It polls every 2–3s, never overlaps requests, and pauses while the tab is hidden.
For frontend work with hot reload (needs Node 20+): `cd dashboard && npm install && npm run dev`.

```bash
curl -X POST localhost:8080/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: order-42' \
  -d '{"type":"email","payload":{"to":"a@example.com"}}'
curl localhost:8080/jobs/<id>
curl "localhost:8080/jobs?status=DEAD"
```

| Endpoint | |
|---|---|
| `POST /jobs` | Enqueue. Body: `type` (required), `payload` (any JSON), `priority` (higher runs first, default 0), `maxAttempts` (1–100, default 5), `delaySeconds` (default 0). Optional `Idempotency-Key` header. |
| `GET /jobs/{id}` | One job |
| `GET /jobs?status=DEAD&limit=50` | Recent jobs, optionally filtered by status |
| `POST /jobs/{id}/cancel` | Cancel a `PENDING` job (409 otherwise) |
| `POST /jobs/{id}/retry` | Rerun a `DEAD` job with a fresh attempt budget (409 otherwise) |

| Endpoint | |
|---|---|
| `GET /stats` | Depth by status, ready backlog and its oldest age, jobs finished in the last minute, enqueue-to-success p50/p95/p99 over 5 minutes. Cached and refreshed every 5s. |

Prometheus runs at [localhost:9090](http://localhost:9090) and scrapes the API and every worker replica.
Useful queries:

```promql
sum by (type, outcome) (rate(jobqueue_job_execution_seconds_count[1m]))                    # throughput
histogram_quantile(0.99, sum by (le) (rate(jobqueue_job_queue_wait_seconds_bucket[1m])))  # p99 wait to be claimed
histogram_quantile(0.99, sum by (le) (rate(jobqueue_claim_latency_seconds_bucket[1m])))   # p99 claim query
jobqueue_jobs_depth                                                                        # backlog by status
```

## Tests

Without a local JDK, run everything in a Maven container. The Docker socket is mounted so
Testcontainers can start Postgres next to it:

```bash
docker run --rm -v "$PWD:/src" -v jobqueue-m2:/root/.m2 -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal -w /src maven:3.9-eclipse-temurin-21 mvn -B verify
```

`mvn test` runs only the unit tests; `mvn verify` adds the integration tests (`*IT`), which run against
a real Postgres 17 with the production migrations. Each simulated worker is the real claim loop,
heartbeat and reaper with its own connection pool, so one can be crashed without affecting the others.

| Test | Proves |
|---|---|
| `manyWorkersRunEveryJobExactlyOnce` | 4 workers × 1,000 jobs: every job succeeds, each executes exactly once, the load spreads across workers |
| `jobsSurviveAWorkerCrashingMidRun` | Cut a worker's database connections mid-job: all its jobs are reaped and completed by another worker. Each ran twice, which is at-least-once delivery in action |
| `gracefulShutdownFinishesShortJobsAndReleasesLongOnes` | On stop, short jobs finish; long ones get their leases released and are rerun immediately elsewhere; late results from interrupted handlers are fenced off |
| `staleWorkerCannotHeartbeatOrFinishAJobThatWasReclaimed` | A worker that stalls past its lease can't extend or overwrite the new owner's run, even when the same worker reclaims the job |
| `claimSkipsRowsLockedByAnotherTransactionInsteadOfBlocking` | `SKIP LOCKED` skips a row locked by another transaction instead of waiting on it |
| `reaperDeadLettersJobsThatAreOutOfAttempts` | A job that keeps killing its worker ends up in the DLQ instead of looping forever |
| `RedisModeIT.manyWorkersRunEveryJobExactlyOnce` | The same exactly-once guarantee on the Redis fast path |
| `jobsMissingFromRedisAreFoundByThePostgresSweep` | A job committed to Postgres but never indexed in Redis (API crashed between the two) still runs |
| `losingAllRedisDataLosesNoJobs` | Redis restarts empty mid-run; every job still completes |
| `workersFallBackToPostgresWhenRedisGoesDown` | Redis dies mid-run; workers degrade to Postgres polling and finish every job |
| `popsHighestPriorityFirstThenOldestFirst` | Redis hands out jobs in exactly the Postgres claim order |

## Benchmarks: Redis fast path vs. Postgres polling

Run with [`load-tests/benchmark.sh`](load-tests/benchmark.sh): the full Compose stack, k6 enqueueing
through the real API, and the no-op handler so the numbers measure the queue rather than the work.
Latency is enqueue-to-completion (`updated_at − created_at` of each `SUCCEEDED` row). Raw results are
in [`load-tests/results/`](load-tests/results/).

**Environment:** one Windows laptop, Docker Desktop with 8 CPUs and 8 GB shared by Postgres, Redis, the
API, every worker and k6. Absolute numbers are laptop numbers; the comparisons between modes are the point.

### Latency under steady load: 300 jobs/s for 30s, 2 workers (9,000 jobs each)

| Claim path | p50 | p95 | p99 | Postgres tx/s |
|---|---|---|---|---|
| Postgres polling, 500ms interval (default) | 240 ms | 463 ms | 491 ms | 661 |
| Postgres polling, 50ms interval | 16 ms | 49 ms | 101 ms | 688 |
| **Redis fast path** | **8.5 ms** | **20 ms** | **37 ms** | 791 |

Redis cuts p50 latency **28×** and p99 **13×** against the default polling interval, and still beats
aggressive 50ms polling by 2–3× at p99. A blocked `BZPOPMIN` wakes the moment a job is enqueued, while
a polling worker finds it, on average, half an interval later.

### Database load while idle: 4 workers, empty queue, 30s

| Claim path | Postgres tx/s |
|---|---|
| Postgres polling, 500ms | 13.8 |
| Postgres polling, 50ms | 82.2 |
| **Redis fast path** (5s safety sweep only) | **4.8** |

Polling buys latency with database load: getting polling down to ~16ms p50 costs **17×** more idle
transactions than Redis, and that cost grows linearly with the number of workers.

### Drain throughput: 20,000-job backlog, median of 3 runs (jobs/s)

| Workers | Postgres polling | Redis fast path |
|---|---|---|
| 1 | 1,206 | 734 |
| 2 | **1,619** | 1,169 |
| 4 | 1,388 | 966 |
| 8 | 912 | 807 |

**Redis does not help throughput, and costs 10–40%.** With a backlog, a polling worker never sleeps, so
Redis's wake-up advantage disappears, while each Redis batch costs extra round trips (`BZPOPMIN`, then
`ZPOPMIN`, then the Postgres claim) where polling needs one query. The same effect shows in the steady-load
table: Redis claims jobs in smaller batches as they arrive, so it spends more Postgres transactions per job.

**Throughput peaks at 2 workers here**, because the bottleneck is Postgres (one committed `UPDATE` per
finished job) competing for the same 8 CPUs as up to 8 worker JVMs. Adding workers on the same machine
adds contention, not capacity; measuring real horizontal scaling needs Postgres on its own host.

**Methodology notes.** Single runs varied by up to ~40% (e.g. 824–1,353 jobs/s for the same
configuration), so drain results are medians of 3 interleaved runs. One run in the first pass
(Redis, 4 workers) stalled for ~5 minutes; it didn't reproduce in 4 later attempts and its worker logs
were lost when the containers were recreated, so it's excluded and noted here rather than hidden.

**Takeaway:** use the Redis path when latency matters and the queue is often idle or lightly loaded
(user-facing jobs, notifications). For batch backlogs, plain `SKIP LOCKED` polling is simpler and faster.

## Design decisions

**Claiming with `FOR UPDATE SKIP LOCKED`.** Workers claim a batch with one statement: select the
highest-priority ready rows, lock them, and flip them to `RUNNING` with `attempts + 1`. `SKIP LOCKED`
makes concurrent claimers skip rows another worker is mid-claim on instead of blocking on them, so
many workers can poll at once and no job is ever handed to two of them.

**Partial claim index.** `(priority DESC, run_at) WHERE status = 'PENDING'` holds only claimable rows,
so finished jobs never bloat it and the scan returns rows already in claim order.

**At-least-once delivery.** A worker can crash after a handler finishes but before the job is marked
done, and the job will run again. Handlers must be **idempotent**: running one twice has the same
effect as running it once.

**Retries with exponential backoff and jitter.** `delay = min(max, base × 2^(attempt−1)) + random(0, base)`.
A retrying job is simply `PENDING` with a future `run_at`, so delayed jobs and retries share one code
path. After `max_attempts` the job moves to `DEAD`, the dead-letter queue. Handlers throw
`NonRetryableJobException` for failures retrying can't fix (bad payload, unknown type), which skip
straight to `DEAD`.

**Leases and heartbeats (visibility timeouts).** A claim gives the job a 30s lease. While the job runs,
the worker extends its leases every 10s with one batched query, on its own thread so a saturated job
pool can't starve it. It heartbeats only jobs actually executing, not "everything with my worker id",
so a job whose thread died without recording an outcome is left to expire and get recovered.

**The reaper recovers crashed workers.** Every worker periodically moves `RUNNING` jobs with expired
leases back to `PENDING` (or to `DEAD` if out of attempts, so a job that crashes its worker every time
can't loop forever). Running it in every worker means recovery has no single point of failure;
`SKIP LOCKED` keeps concurrent reapers from blocking each other. All times use the database clock, so
clock skew between machines doesn't matter.

**Fencing tokens.** Each claim gets a fresh random `lease_id`, and every heartbeat and finishing update
must present it (`WHERE lease_id = :mine`). A worker that was paused past its lease (GC, network
partition) and wakes up after the job was reaped and reclaimed matches nothing, so it can't extend
or overwrite the new owner's run, even if the new owner is the same worker. Fencing on `worker_id`
alone wouldn't catch that case, and fencing on `attempts` would break once a DLQ retry resets it.

**Idempotent enqueue.** `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` makes the duplicate check
and the insert one atomic step, so even concurrent retries create exactly one job. A replay returns the
original job with `200` (vs. `201` for a new one); reusing a key for a *different* job returns `409`
instead of silently handing back the wrong job.

**Bounded concurrency on virtual threads.** Each job runs on a Java 21 virtual thread, and a semaphore
caps in-flight jobs per worker. The loop only claims as many jobs as it has free slots, leaving the
rest of the queue for other workers.

**Graceful shutdown.** On SIGTERM a worker stops claiming and keeps heartbeating while in-flight jobs
finish. Jobs still running after the 30s shutdown timeout get their leases released, so other workers
rerun them immediately instead of waiting for the leases to expire.

**Metrics that aggregate across workers.** Timers publish histograms rather than per-process
percentiles, because percentiles from different workers can't be averaged; Prometheus computes fleet-wide
p99s with `histogram_quantile`. Job types come from API clients, so only registered handler types become
metric tags (anything else is `unknown`); otherwise each typo would create a new time series. Queue wait
is measured as `claimed_at − run_at` using database timestamps, so worker clock skew can't distort it.

**Redis as an index, never the source of truth.** In Redis mode the API commits the job to Postgres,
*then* adds its id to a Redis sorted set, and idle workers block on `BZPOPMIN` instead of polling. A
popped id is claimed in Postgres with the same status checks as always, so Redis can be stale, lossy or
down without breaking correctness:
- *Ordering:* score is `-priority` and the member is `<zero-padded run_at ms>:<id>`. Redis sorts equal
  scores by member, so pops come out highest priority first, then oldest first: the same order as the
  Postgres claim query.
- *Delayed jobs and retries* sit in a second sorted set scored by `run_at`; a Lua script run every 100ms
  atomically moves due ones to the ready set.
- *Lost index entries* (API crashed between commit and `ZADD`, a worker died holding popped ids, Redis
  restarted empty) are caught by a periodic `SKIP LOCKED` sweep of Postgres. If a sweep finds work, the
  worker keeps draining Postgres until it's empty, so recovery after a Redis wipe is fast.
- *Redis outage:* workers fall back to plain Postgres polling and switch back on their own. Redis runs
  with persistence off, since there's nothing in it worth persisting.
- *Stale entries* (cancelled or already-claimed jobs) are dropped by the claim's `status = 'PENDING'` check.

**Status as `TEXT` + `CHECK`** instead of a Postgres `ENUM`: same safety, and adding a status is a
constraint change instead of `ALTER TYPE`.