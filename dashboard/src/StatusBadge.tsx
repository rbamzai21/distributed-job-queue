import type { Job } from './api';

type Tone = 'neutral' | 'active' | 'good' | 'warning' | 'critical';

// Color never carries the status alone: every badge pairs an icon with its label.
const STYLES: Record<string, { label: string; icon: string; tone: Tone }> = {
  PENDING: { label: 'Pending', icon: '○', tone: 'neutral' },
  RETRYING: { label: 'Retrying', icon: '↻', tone: 'warning' },
  RUNNING: { label: 'Running', icon: '▶', tone: 'active' },
  SUCCEEDED: { label: 'Succeeded', icon: '✓', tone: 'good' },
  DEAD: { label: 'Dead', icon: '✕', tone: 'critical' },
  CANCELLED: { label: 'Cancelled', icon: '⊘', tone: 'neutral' },
};

/** A PENDING job that has already failed at least once is waiting out its backoff, so it shows as retrying. */
export function displayStatus(job: Pick<Job, 'status' | 'attempts'>): string {
  return job.status === 'PENDING' && job.attempts > 0 ? 'RETRYING' : job.status;
}

export function StatusBadge({ job }: { job: Pick<Job, 'status' | 'attempts'> }) {
  const style = STYLES[displayStatus(job)];
  return (
    <span className={`badge badge-${style.tone}`}>
      <span className="badge-icon" aria-hidden="true">
        {style.icon}
      </span>
      {style.label}
    </span>
  );
}
