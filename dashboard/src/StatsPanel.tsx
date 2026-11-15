import type { Stats } from './api';
import { formatCount, formatDuration, formatMs, relativeTime } from './format';

/** Ready jobs waiting longer than this mean workers are falling behind. */
const BACKLOG_WARNING_SECONDS = 30;

interface TileProps {
  label: string;
  value: string;
  detail?: string;
  alert?: { icon: string; tone: 'warning' | 'critical'; text: string };
}

function Tile({ label, value, detail, alert }: TileProps) {
  return (
    <div className="tile">
      <div className="tile-label">{label}</div>
      <div className="tile-value">{value}</div>
      {alert ? (
        <div className={`tile-detail tile-alert-${alert.tone}`}>
          <span aria-hidden="true">{alert.icon}</span> {alert.text}
        </div>
      ) : (
        detail && <div className="tile-detail">{detail}</div>
      )}
    </div>
  );
}

export function StatsPanel({ stats, error }: { stats?: Stats; error?: string }) {
  const depth = stats?.depth ?? {};
  const pending = depth.PENDING;
  const waiting = pending === undefined || !stats ? undefined : pending - stats.ready;
  const backlogAge = stats?.oldestReadyAgeSeconds ?? 0;
  const dead = depth.DEAD ?? 0;

  return (
    <section aria-labelledby="stats-heading">
      <div className="section-header">
        <h2 id="stats-heading">Queue</h2>
        <span className="muted">
          {error ? `Stats unavailable: ${error}` : stats?.asOf ? `Updated ${relativeTime(stats.asOf)}` : 'Loading…'}
        </span>
      </div>

      <div className="tiles">
        <Tile
          label="Ready to run"
          value={formatCount(stats?.ready)}
          alert={
            backlogAge > BACKLOG_WARNING_SECONDS
              ? { icon: '⚠', tone: 'warning', text: `Oldest waiting ${formatDuration(backlogAge)}` }
              : undefined
          }
          detail={waiting ? `+${formatCount(waiting)} delayed or backing off` : 'Nothing waiting on a delay'}
        />
        <Tile label="Running" value={formatCount(depth.RUNNING)} detail="Leased by a worker" />
        <Tile
          label="Succeeded / min"
          value={formatCount(stats?.lastMinute.succeeded)}
          detail={stats ? `${(stats.lastMinute.succeeded / 60).toFixed(1)} jobs/s · ${formatCount(depth.SUCCEEDED)} total` : undefined}
        />
        <Tile
          label="Dead-letter queue"
          value={formatCount(depth.DEAD)}
          alert={
            stats && stats.lastMinute.dead > 0
              ? { icon: '✕', tone: 'critical', text: `${formatCount(stats.lastMinute.dead)} in the last minute` }
              : undefined
          }
          detail={dead === 0 ? 'Empty' : 'Out of attempts or failed permanently'}
        />
        <Tile
          label="Latency p50 / p99"
          value={`${formatMs(stats?.latencyMs.p50)} / ${formatMs(stats?.latencyMs.p99)}`}
          detail={`p95 ${formatMs(stats?.latencyMs.p95)} · enqueue to success, last 5 min`}
        />
      </div>
    </section>
  );
}
