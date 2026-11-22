import { useEffect, useRef, useState } from "react";
import type { DashboardOverview, RequestCounts } from "../api/types";

/** Requests/second for one worker at one sample, by kind. */
export interface Rate {
  put: number;
  get: number;
  delete: number;
  list: number;
  total: number;
}

export interface ThroughputPoint {
  t: number; // ms, controller clock
  rates: Record<string, Rate>;
}

interface Prev {
  t: number;
  startedAt: string;
  req: RequestCounts;
}

/**
 * Turns the workers' cumulative request counters into per-worker
 * requests/second, keeping a rolling window of samples. All computed in the
 * browser from successive /dashboard/overview polls. A counter that goes
 * backwards (the worker restarted) restarts the series instead of producing
 * a negative rate.
 */
export function useThroughput(overview: DashboardOverview | undefined, windowMs: number) {
  const prev = useRef(new Map<string, Prev>());
  const [points, setPoints] = useState<ThroughputPoint[]>([]);
  const lastServerTime = useRef<string>("");

  useEffect(() => {
    if (!overview || overview.server_time === lastServerTime.current) return;
    lastServerTime.current = overview.server_time;
    const t = Date.parse(overview.server_time);
    const rates: Record<string, Rate> = {};
    for (const w of overview.workers) {
      const s = w.stats;
      if (!s) continue;
      const p = prev.current.get(w.worker_id);
      const r = s.requests;
      if (
        p &&
        p.startedAt === s.started_at &&
        t > p.t &&
        r.put >= p.req.put &&
        r.get >= p.req.get &&
        r.delete >= p.req.delete &&
        r.list >= p.req.list
      ) {
        const dt = (t - p.t) / 1000;
        const put = (r.put - p.req.put) / dt;
        const get = (r.get - p.req.get) / dt;
        const del = (r.delete - p.req.delete) / dt;
        const list = (r.list - p.req.list) / dt;
        rates[w.worker_id] = { put, get, delete: del, list, total: put + get + del + list };
      }
      prev.current.set(w.worker_id, { t, startedAt: s.started_at, req: { ...r } });
    }
    setPoints((ps) => [...ps.filter((x) => x.t >= t - windowMs), { t, rates }]);
  }, [overview, windowMs]);

  const latest = points.length > 0 ? points[points.length - 1].rates : {};
  return { points, latest };
}
