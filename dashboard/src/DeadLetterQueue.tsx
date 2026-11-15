import { useState } from 'react';
import { api, type Job } from './api';
import { relativeTime, shortId } from './format';
import { usePolling } from './usePolling';

/** Jobs that ran out of attempts or failed permanently, each with a button to rerun it. */
export function DeadLetterQueue({ onChange }: { onChange: () => void }) {
  const { data: jobs, error, refresh } = usePolling(() => api.jobs('DEAD', 100), 3000);
  const [busy, setBusy] = useState<Set<string>>(new Set());
  const [actionError, setActionError] = useState<string>();

  const retry = async (targets: Job[]) => {
    setActionError(undefined);
    setBusy(new Set(targets.map((j) => j.id)));
    const results = await Promise.allSettled(targets.map((j) => api.retry(j.id)));
    const failed = results.filter((r) => r.status === 'rejected').length;
    if (failed > 0) setActionError(`${failed} of ${targets.length} retries failed; they may no longer be dead.`);
    setBusy(new Set());
    refresh();
    onChange();
  };

  return (
    <section aria-labelledby="dlq-heading">
      <div className="section-header">
        <h2 id="dlq-heading">Dead-letter queue</h2>
        {jobs && jobs.length > 1 && (
          <button className="button" onClick={() => retry(jobs)} disabled={busy.size > 0}>
            Retry all {jobs.length}
          </button>
        )}
      </div>

      {(error || actionError) && <p className="error-text">{actionError ?? `Could not load the DLQ: ${error}`}</p>}

      {jobs?.length === 0 && <p className="empty">Empty. Every job either succeeded or is still in flight.</p>}

      <ul className="dlq">
        {jobs?.map((job) => (
          <li key={job.id} className="dlq-item">
            <div className="dlq-main">
              <div className="dlq-title">
                <span className="mono" title={job.id}>{shortId(job.id)}</span>
                <span>{job.type}</span>
                <span className="muted">
                  {job.attempts} / {job.maxAttempts} attempts · died {relativeTime(job.updatedAt)}
                </span>
              </div>
              <div className="dlq-error">{job.lastError ?? 'No error recorded'}</div>
            </div>
            <button className="button" onClick={() => retry([job])} disabled={busy.has(job.id)}>
              {busy.has(job.id) ? 'Retrying…' : 'Retry'}
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
