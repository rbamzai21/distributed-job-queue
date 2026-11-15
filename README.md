# job-queue

A distributed job queue built from scratch: a REST API accepts jobs, PostgreSQL stores them durably,
and a pool of worker processes claims and runs them. Failed jobs retry with exponential backoff, and
jobs that keep failing move to a dead-letter queue for inspection. Think of it as a small SQS or Celery.

**Stack:** Java 21, Spring Boot 3, PostgreSQL 17, Flyway, Micrometer + Prometheus, Docker Compose,
JUnit + Testcontainers.

## Architecture

```
            POST /jobs                       claim (SKIP LOCKED)
 client ──────────────►  api  ──INSERT──►  Postgres  ◄────────────  worker ×N
            GET /jobs                        (jobs)   ─────────────►  ├─ email handler
                                                       finish/retry   ├─ cpu handler
                                                                      └─ flaky handler
```

| Module | What it does |
|---|---|
| `common/` | `Job` model, row mapping, backoff calculation |
| `api/` | REST endpoints; runs the Flyway migrations on startup |
| `worker/` | Claim loop, handler registry, retry/dead-letter logic |
| `db/migrations/` | Flyway SQL migrations |

## Running it

Requires Docker. Nothing else needs to be installed locally.

```bash
docker compose up -d --build        # Postgres, the API on :8080, and 2 workers
```

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

**Status as `TEXT` + `CHECK`** instead of a Postgres `ENUM`: same safety, and adding a status is a
constraint change instead of `ALTER TYPE`.

## Roadmap

- [x] Week 1: schema, enqueue/get/list API, `SKIP LOCKED` claim loop, handlers, backoff, dead-letter queue, Compose
- [x] Week 2a: lease heartbeats, reaper, fencing tokens, graceful lease release, idempotency keys, cancel/retry
- [x] Week 2b: Testcontainers crash and concurrency tests, Micrometer + Prometheus metrics, `GET /stats`
- [ ] Week 3: Redis fast path, Redis vs. Postgres claiming benchmark, k6 load tests, React dashboard, AWS deploy
