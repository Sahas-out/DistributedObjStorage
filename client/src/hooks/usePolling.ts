import { useEffect, useRef, useState } from "react";

export interface PollState<T> {
  data: T | undefined;
  error: unknown;
  loading: boolean;
  /** Time of the last successful poll (ms). */
  updatedAt: number;
  refresh: () => void;
}

/**
 * Calls fn every intervalMs (and once immediately). A poll is skipped while
 * the previous one is still running, and paused while the tab is hidden.
 */
export function usePolling<T>(fn: (signal: AbortSignal) => Promise<T>, intervalMs: number): PollState<T> {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<unknown>();
  const [loading, setLoading] = useState(true);
  const [updatedAt, setUpdatedAt] = useState(0);
  const [tick, setTick] = useState(0);
  const fnRef = useRef(fn);
  fnRef.current = fn;

  useEffect(() => {
    const ctrl = new AbortController();
    let running = false;
    const run = async () => {
      if (running || document.hidden) return;
      running = true;
      try {
        const d = await fnRef.current(ctrl.signal);
        if (!ctrl.signal.aborted) {
          setData(d);
          setError(undefined);
          setUpdatedAt(Date.now());
        }
      } catch (e) {
        if (!ctrl.signal.aborted) setError(e);
      } finally {
        running = false;
        if (!ctrl.signal.aborted) setLoading(false);
      }
    };
    void run();
    const id = window.setInterval(run, intervalMs);
    return () => {
      ctrl.abort();
      window.clearInterval(id);
    };
  }, [intervalMs, tick]);

  return { data, error, loading, updatedAt, refresh: () => setTick((t) => t + 1) };
}
