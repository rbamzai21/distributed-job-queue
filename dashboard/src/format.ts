const numberFormat = new Intl.NumberFormat('en-US');

export function formatCount(n: number | undefined): string {
  return n === undefined ? '–' : numberFormat.format(n);
}

/** 840 -> "840 ms", 12_500 -> "12.5 s", null -> "–" */
export function formatMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return '–';
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`;
}

/** Seconds as a short duration: "45 s", "3 m 20 s", "2 h 5 m". */
export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)} m ${s % 60} s`;
  return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} m`;
}

/** "just now", "12 s ago", "4 m ago"; future times (delayed jobs) read "in 30 s". */
export function relativeTime(iso: string, now = Date.now()): string {
  const deltaSeconds = (now - new Date(iso).getTime()) / 1000;
  if (deltaSeconds < -1) return `in ${formatDuration(-deltaSeconds)}`;
  if (deltaSeconds < 2) return 'just now';
  return `${formatDuration(deltaSeconds)} ago`;
}

export function shortId(id: string): string {
  return id.slice(0, 8);
}
