import type { DashboardOverview } from "../../api/types";
import { formatDuration, shortId } from "../../lib/format";
import { workerColor } from "../../lib/palette";

type Beat = "fresh" | "late" | "stale";

const beatLabel: Record<Beat, { icon: string; text: string }> = {
  fresh: { icon: "●", text: "on time" },
  late: { icon: "◆", text: "late" },
  stale: { icon: "▲", text: "missed" },
};

/**
 * Seconds since each worker's last announcement (its heartbeat), measured on
 * the controller's clock. The controller doesn't act on missed heartbeats
 * yet (failure detection is a later phase); this shows what it would see.
 */
export function HeartbeatList({ overview }: { overview: DashboardOverview }) {
  const now = Date.parse(overview.server_time);
  const interval = overview.controller.announce_interval_seconds || 30;
  const limit = interval * 2.5;

  return (
    <section className="card">
      <header className="chart-head">
        <div>
          <h2>Heartbeats</h2>
          <p className="muted small">
            Time since each worker last announced itself · expected every {interval}s
          </p>
        </div>
      </header>
      <ul className="beats">
        {overview.workers.length === 0 && <li className="empty">No workers registered.</li>}
        {overview.workers.map((w) => {
          const age = Math.max(0, (now - Date.parse(w.last_seen)) / 1000);
          const beat: Beat = age <= interval + 5 ? "fresh" : age <= interval * 2 ? "late" : "stale";
          const l = beatLabel[beat];
          return (
            <li key={w.worker_id} className={`beat is-${beat}`}>
              <span className="swatch" style={{ background: workerColor(w.worker_id) }} aria-hidden />
              <span className="mono beat-id" title={w.worker_id}>
                {shortId(w.worker_id, 12)}
              </span>
              <div className="beat-track" aria-hidden>
                <div className="beat-fill" style={{ width: `${Math.min(100, (age / limit) * 100)}%` }} />
                <div className="beat-mark" style={{ left: `${(interval / limit) * 100}%` }} />
              </div>
              <span className="beat-age">{formatDuration(age)}</span>
              <span className={`beat-state is-${beat}`}>
                <span aria-hidden>{l.icon}</span> {l.text}
              </span>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
