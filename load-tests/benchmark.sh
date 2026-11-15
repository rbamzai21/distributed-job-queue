#!/usr/bin/env bash
# Redis fast path vs. Postgres polling, on the full Docker Compose stack.
#
#   1. Latency under steady load: enqueue-to-completion percentiles at a fixed arrival rate.
#   2. Database load while idle: Postgres transactions/s with workers polling an empty queue.
#   3. Drain throughput: how fast N workers clear a backlog (the scaling curve).
#
# Jobs use the no-op handler, so every number measures the queue, not the work.
# Usage (from the repo root, Git Bash or Linux):  ./load-tests/benchmark.sh
# Knobs: SCENARIOS, RATE, DURATION, BACKLOG_JOBS, WORKER_COUNTS, DRAIN_REPEATS, IDLE_SECONDS.
# Drain runs repeat (modes interleaved) because single runs on a shared laptop can stall; report medians.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: don't rewrite container paths like /scripts

RATE=${RATE:-300}
DURATION=${DURATION:-30s}
BACKLOG_JOBS=${BACKLOG_JOBS:-20000}
WORKER_COUNTS=${WORKER_COUNTS:-"1 2 4 8"}
DRAIN_REPEATS=${DRAIN_REPEATS:-3}
SCENARIOS=${SCENARIOS:-"steady idle drain"}
IDLE_SECONDS=${IDLE_SECONDS:-30}
K6_IMAGE=grafana/k6:1.0.0

cd "$(dirname "$0")/.."
HOST_DIR=$(pwd -W 2>/dev/null || pwd)
NETWORK=job-queue_default
STAMP=$(date +%Y%m%d-%H%M%S)
OUT=load-tests/results/$STAMP
mkdir -p "$OUT"
CSV=$OUT/results.csv
echo "scenario,mode,poll,workers,jobs,p50_ms,p95_ms,p99_ms,max_ms,throughput_jobs_s,db_tx_s,enqueue_p99_ms" > "$CSV"

psql() { docker compose exec -T postgres psql -U jobqueue -d jobqueue -tA -F, -c "$1"; }
log() { echo "[$(date +%T)] $*" | tee -a "$OUT/log.txt"; }

reset_data() {
  psql "TRUNCATE jobs" > /dev/null
  docker compose exec -T redis redis-cli FLUSHALL > /dev/null
}

# Restart the API and workers with the given mode, poll interval and worker count, and wait for the API.
deploy() {
  local mode=$1 poll=$2 workers=$3
  QUEUE_MODE=$mode WORKER_POLL_INTERVAL=$poll docker compose up -d --force-recreate --no-build \
    --scale worker="$workers" api worker > /dev/null 2>&1
  for _ in $(seq 1 60); do
    curl -sf localhost:8080/actuator/health > /dev/null && break
    sleep 1
  done
  sleep 8   # let workers finish booting and connecting
}

k6() {
  local run=$1; shift
  docker run --rm --network "$NETWORK" -v "$HOST_DIR/load-tests:/scripts" "$K6_IMAGE" run --quiet \
    --summary-export "/scripts/results/$STAMP/k6-$run.json" \
    -e API_URL=http://api:8080 -e RUN_ID="$run" "$@" /scripts/enqueue.js > "$OUT/k6-$run.log" 2>&1 \
    || log "  k6 reported failed thresholds for $run (see k6-$run.log)"
}

k6_p99() {
  python -c "import json,sys; print(round(json.load(open(sys.argv[1]))['metrics']['http_req_duration']['p(99)'], 1))" \
    "$OUT/k6-$1.json"
}

wait_drained() {
  local run=$1
  for _ in $(seq 1 600); do
    local left
    left=$(psql "SELECT count(*) FROM jobs WHERE payload->>'run' = '$run' AND status IN ('PENDING','RUNNING')")
    [ "$left" = "0" ] && return 0
    sleep 1
  done
  log "WARNING: $run did not drain in 10 minutes"
}

db_commits() { psql "SELECT xact_commit FROM pg_stat_database WHERE datname = 'jobqueue'"; }

# count, p50, p95, p99, max of enqueue-to-completion, and completions/s from first to last finish
run_stats() {
  psql "SELECT count(*),
               round(percentile_cont(0.50) WITHIN GROUP (ORDER BY ms)::numeric, 1),
               round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)::numeric, 1),
               round(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms)::numeric, 1),
               round(max(ms)::numeric, 1),
               round((count(*) / greatest(extract(epoch FROM max(updated_at) - min(updated_at)), 0.001))::numeric, 0)
        FROM (SELECT updated_at, extract(epoch FROM updated_at - created_at) * 1000 AS ms
              FROM jobs WHERE payload->>'run' = '$1' AND status = 'SUCCEEDED') t"
}

steady() {
  local mode=$1 poll=$2 workers=2
  local run="steady-$mode-$poll"
  log "steady: mode=$mode poll=$poll workers=$workers rate=$RATE/s for $DURATION"
  deploy "$mode" "$poll" "$workers"
  k6 "warmup-$run" -e SHAPE=steady -e RATE="$RATE" -e DURATION=10s
  reset_data
  local before after start end
  before=$(db_commits); start=$(date +%s)
  k6 "$run" -e SHAPE=steady -e RATE="$RATE" -e DURATION="$DURATION"
  wait_drained "$run"
  after=$(db_commits); end=$(date +%s)
  local stats; stats=$(run_stats "$run")
  local tx_s=$(( (after - before) / (end - start) ))
  echo "steady,$mode,$poll,$workers,$stats,$tx_s,$(k6_p99 "$run")" >> "$CSV"
  log "  -> count,p50,p95,p99,max,throughput = $stats | db tx/s = $tx_s"
}

idle() {
  local mode=$1 poll=$2 workers=4
  log "idle: mode=$mode poll=$poll workers=$workers for ${IDLE_SECONDS}s"
  deploy "$mode" "$poll" "$workers"
  reset_data
  local before after
  before=$(db_commits)
  sleep "$IDLE_SECONDS"
  after=$(db_commits)
  local tx_s
  tx_s=$(python -c "print(round(($after - $before) / $IDLE_SECONDS, 1))")
  echo "idle,$mode,$poll,$workers,0,,,,,,$tx_s," >> "$CSV"
  log "  -> db tx/s = $tx_s"
}

drain() {
  local mode=$1 workers=$2 repeat=$3
  local run="drain-$mode-w$workers-r$repeat"
  log "drain: mode=$mode workers=$workers backlog=$BACKLOG_JOBS"
  deploy "$mode" 500ms 0
  reset_data
  k6 "$run" -e SHAPE=backlog -e JOBS="$BACKLOG_JOBS"
  QUEUE_MODE=$mode docker compose up -d --no-build --no-recreate --scale worker="$workers" worker > /dev/null 2>&1
  wait_drained "$run"
  local stats; stats=$(run_stats "$run")
  echo "drain,$mode,500ms,$workers,$stats,," >> "$CSV"
  log "  -> count,p50,p95,p99,max,throughput = $stats"
}

log "Building images"
docker compose build -q
docker compose up -d postgres redis prometheus > /dev/null 2>&1
docker info --format 'Docker: {{.NCPU}} CPUs, {{.MemTotal}} bytes RAM, {{.OperatingSystem}}' | tee -a "$OUT/log.txt"

if [[ " $SCENARIOS " == *" steady "* ]]; then
  steady postgres 500ms
  steady postgres 50ms
  steady redis 500ms
fi

if [[ " $SCENARIOS " == *" idle "* ]]; then
  idle postgres 500ms
  idle postgres 50ms
  idle redis 500ms
fi

if [[ " $SCENARIOS " == *" drain "* ]]; then
  for repeat in $(seq 1 "$DRAIN_REPEATS"); do
    for workers in $WORKER_COUNTS; do
      drain postgres "$workers" "$repeat"
      drain redis "$workers" "$repeat"
    done
  done
fi

log "Done. Results: $CSV"
cat "$CSV" | tee -a "$OUT/log.txt"
