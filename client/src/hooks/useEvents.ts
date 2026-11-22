import { useEffect, useRef, useState } from "react";
import { getEvents } from "../api/dashboard";
import type { DashboardEvent } from "../api/types";

const KEEP = 200;

/** Polls the controller's activity feed, fetching only new events. */
export function useEvents(intervalMs: number) {
  const [events, setEvents] = useState<DashboardEvent[]>([]);
  const [error, setError] = useState<unknown>();
  const after = useRef(0);

  useEffect(() => {
    const ctrl = new AbortController();
    let running = false;
    const run = async () => {
      if (running || document.hidden) return;
      running = true;
      try {
        const res = await getEvents(after.current, ctrl.signal);
        if (ctrl.signal.aborted) return;
        if (res.last_seq < after.current) {
          // The controller restarted and its sequence began again.
          after.current = 0;
          setEvents([]);
          return;
        }
        after.current = res.last_seq;
        if (res.events.length > 0) {
          setEvents((es) => [...res.events.slice().reverse(), ...es].slice(0, KEEP));
        }
        setError(undefined);
      } catch (e) {
        if (!ctrl.signal.aborted) setError(e);
      } finally {
        running = false;
      }
    };
    void run();
    const id = window.setInterval(run, intervalMs);
    return () => {
      ctrl.abort();
      window.clearInterval(id);
    };
  }, [intervalMs]);

  return { events, error };
}
