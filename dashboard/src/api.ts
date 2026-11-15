// Typed client for the job-queue API. Every path goes through /api, which the Vite dev server
// and nginx both proxy to the API service.

export type JobStatus = 'PENDING' | 'RUNNING' | 'SUCCEEDED' | 'DEAD' | 'CANCELLED';

export const JOB_STATUSES: JobStatus[] = ['PENDING', 'RUNNING', 'SUCCEEDED', 'DEAD', 'CANCELLED'];

export interface Job {
  id: string;
  type: string;
  payload: unknown;
  priority: number;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  runAt: string;
  leaseExpiresAt: string | null;
  workerId: string | null;
  idempotencyKey: string | null;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface Stats {
  asOf: string | null;
  depth: Partial<Record<JobStatus, number>>;
  ready: number;
  oldestReadyAgeSeconds: number;
  lastMinute: { succeeded: number; dead: number };
  latencyMs: { p50: number | null; p95: number | null; p99: number | null };
}

export interface EnqueueRequest {
  type: string;
  payload?: unknown;
  priority?: number;
  maxAttempts?: number;
  delaySeconds?: number;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...init?.headers },
  });
  if (!res.ok) {
    // The API returns Spring's error body; its `message` is written for clients.
    const body = await res.json().catch(() => null);
    throw new Error(body?.message ?? `${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  stats: () => request<Stats>('/stats'),
  jobs: (status?: JobStatus, limit = 50) =>
    request<Job[]>(`/jobs?limit=${limit}${status ? `&status=${status}` : ''}`),
  enqueue: (body: EnqueueRequest) => request<Job>('/jobs', { method: 'POST', body: JSON.stringify(body) }),
  cancel: (id: string) => request<Job>(`/jobs/${id}/cancel`, { method: 'POST' }),
  retry: (id: string) => request<Job>(`/jobs/${id}/retry`, { method: 'POST' }),
};
