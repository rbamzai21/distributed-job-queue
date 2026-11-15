import { type FormEvent, useState } from 'react';
import { api, type EnqueueRequest } from './api';

const MAX_COUNT = 1000;
const CONCURRENCY = 20;

const TYPES: { value: string; label: string }[] = [
  { value: 'email', label: 'email: simulated send, 50–250 ms' },
  { value: 'cpu', label: 'cpu: repeated SHA-256 hashing' },
  { value: 'flaky', label: 'flaky: fails at a set rate' },
  { value: 'noop', label: 'noop: does nothing' },
];

function payloadFor(type: string, failureRate: number): unknown {
  switch (type) {
    case 'email':
      return { to: 'demo@example.com' };
    case 'cpu':
      return { iterations: 200_000 };
    case 'flaky':
      return { failureRate };
    default:
      return {};
  }
}

/** Enqueues demo jobs through the real API, a few requests at a time. */
export function EnqueueForm({ onEnqueued }: { onEnqueued: () => void }) {
  const [type, setType] = useState('email');
  const [count, setCount] = useState(25);
  const [priority, setPriority] = useState(0);
  const [delaySeconds, setDelaySeconds] = useState(0);
  const [maxAttempts, setMaxAttempts] = useState(3);
  const [failureRate, setFailureRate] = useState(0.5);
  const [status, setStatus] = useState<{ text: string; error?: boolean }>();
  const [submitting, setSubmitting] = useState(false);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setStatus({ text: `Enqueueing ${count}…` });
    const request: EnqueueRequest = {
      type,
      payload: payloadFor(type, failureRate),
      priority,
      maxAttempts,
      delaySeconds,
    };
    let created = 0;
    let failed = 0;
    let lastError = '';
    for (let start = 0; start < count; start += CONCURRENCY) {
      const batch = Math.min(CONCURRENCY, count - start);
      const results = await Promise.allSettled(Array.from({ length: batch }, () => api.enqueue(request)));
      for (const r of results) {
        if (r.status === 'fulfilled') created++;
        else {
          failed++;
          lastError = r.reason instanceof Error ? r.reason.message : String(r.reason);
        }
      }
    }
    setSubmitting(false);
    setStatus(
      failed === 0
        ? { text: `Enqueued ${created} ${type} job${created === 1 ? '' : 's'}.` }
        : { text: `Enqueued ${created}, ${failed} failed: ${lastError}`, error: true },
    );
    onEnqueued();
  };

  return (
    <section aria-labelledby="enqueue-heading">
      <div className="section-header">
        <h2 id="enqueue-heading">Enqueue demo jobs</h2>
      </div>
      <form className="enqueue-form" onSubmit={submit}>
        <label className="field field-wide">
          <span>Type</span>
          <select value={type} onChange={(e) => setType(e.target.value)}>
            {TYPES.map((t) => (
              <option key={t.value} value={t.value}>{t.label}</option>
            ))}
          </select>
        </label>
        <label className="field">
          <span>Count</span>
          <input type="number" min={1} max={MAX_COUNT} value={count}
            onChange={(e) => setCount(Math.min(MAX_COUNT, Math.max(1, Number(e.target.value))))} />
        </label>
        <label className="field">
          <span>Priority</span>
          <input type="number" value={priority} onChange={(e) => setPriority(Number(e.target.value))} />
        </label>
        <label className="field">
          <span>Delay (s)</span>
          <input type="number" min={0} value={delaySeconds}
            onChange={(e) => setDelaySeconds(Math.max(0, Number(e.target.value)))} />
        </label>
        <label className="field">
          <span>Max attempts</span>
          <input type="number" min={1} max={100} value={maxAttempts}
            onChange={(e) => setMaxAttempts(Math.min(100, Math.max(1, Number(e.target.value))))} />
        </label>
        {type === 'flaky' && (
          <label className="field">
            <span>Failure rate</span>
            <input type="number" min={0} max={1} step={0.1} value={failureRate}
              onChange={(e) => setFailureRate(Math.min(1, Math.max(0, Number(e.target.value))))} />
          </label>
        )}
        <button className="button button-primary" type="submit" disabled={submitting}>
          {submitting ? 'Enqueueing…' : 'Enqueue'}
        </button>
      </form>
      {status && (
        <p className={status.error ? 'error-text' : 'muted'} role="status">{status.text}</p>
      )}
    </section>
  );
}
