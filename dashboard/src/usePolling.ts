import { useCallback, useEffect, useRef, useState } from 'react';

/**
 * Loads data now and then every `intervalMs`. The next request is scheduled only after the previous
 * one settles, so a slow API never gets a pile-up of overlapping requests, and polling pauses while
 * the tab is hidden. `refresh()` reloads immediately (after a retry or cancel, say).
 */
export function usePolling<T>(load: () => Promise<T>, intervalMs: number, deps: unknown[] = []) {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<string>();
  const [generation, setGeneration] = useState(0);
  const loadRef = useRef(load);
  loadRef.current = load;

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;

    const run = async () => {
      if (document.visibilityState === 'visible') {
        try {
          const next = await loadRef.current();
          if (!cancelled) {
            setData(next);
            setError(undefined);
          }
        } catch (e) {
          if (!cancelled) setError(e instanceof Error ? e.message : String(e));
        }
      }
      if (!cancelled) timer = window.setTimeout(run, intervalMs);
    };

    run();
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, generation, ...deps]);

  const refresh = useCallback(() => setGeneration((g) => g + 1), []);
  return { data, error, refresh };
}
