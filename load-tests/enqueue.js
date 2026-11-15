// Enqueues no-op jobs through the real API. Two shapes:
//   SHAPE=steady  : constant arrival rate (RATE jobs/s for DURATION), for latency under a known load
//   SHAPE=backlog : JOBS jobs as fast as possible, for building a backlog to measure drain throughput
// Every job carries RUN_ID in its payload so the benchmark can query exactly this run's jobs.
import http from 'k6/http';
import { check } from 'k6';

const shape = __ENV.SHAPE || 'steady';
const api = __ENV.API_URL || 'http://localhost:8080';
const runId = __ENV.RUN_ID || 'adhoc';

export const options = {
  scenarios: {
    enqueue: shape === 'backlog'
      ? {
          executor: 'shared-iterations',
          vus: Number(__ENV.VUS || 50),
          iterations: Number(__ENV.JOBS || 20000),
          maxDuration: '10m',
        }
      : {
          executor: 'constant-arrival-rate',
          rate: Number(__ENV.RATE || 300),
          timeUnit: '1s',
          duration: __ENV.DURATION || '30s',
          preAllocatedVUs: 50,
          maxVUs: 300,
        },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

const body = JSON.stringify({ type: 'noop', payload: { run: runId } });
const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const res = http.post(`${api}/jobs`, body, params);
  check(res, { 'enqueued (201)': (r) => r.status === 201 });
}
