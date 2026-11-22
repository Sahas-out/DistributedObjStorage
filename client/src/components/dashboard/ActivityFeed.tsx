import type { DashboardEvent } from "../../api/types";
import { describeError } from "../../api/http";
import { formatClock, shortId } from "../../lib/format";

const icons: Record<string, string> = {
  object_uploaded: "⇪",
  object_deleted: "🗑",
  upload_failed: "✕",
  replica_corrupted: "⚠",
  replica_missing: "?",
  replica_orphaned: "⌀",
  worker_registered: "＋",
  worker_ready: "✓",
  worker_moved: "↔",
  worker_replaced: "↻",
  worker_sync_failed: "!",
  worker_deregistered: "−",
};

export function ActivityFeed({ events, error }: { events: DashboardEvent[]; error: unknown }) {
  return (
    <section className="card feed-card">
      <header className="chart-head">
        <div>
          <h2>Activity</h2>
          <p className="muted small">Recent controller events (kept in memory, last 200)</p>
        </div>
        <span className="live-dot" title="live" />
      </header>
      {error !== undefined && <div className="alert small">{describeError(error)}</div>}
      <ol className="feed">
        {events.length === 0 && <li className="empty">No events yet. Upload something!</li>}
        {events.map((e) => (
          <li key={e.seq} className={`feed-item level-${e.level}`}>
            <span className="feed-icon" aria-hidden>
              {icons[e.type] ?? "•"}
            </span>
            <div className="feed-text">
              <div>{e.message}</div>
              <div className="muted small">
                {formatClock(e.time)}
                {e.worker_id && <> · worker <span className="mono">{shortId(e.worker_id, 12)}</span></>}
                {e.object_id && <> · object <span className="mono">{shortId(e.object_id, 10)}</span></>}
              </div>
            </div>
          </li>
        ))}
      </ol>
    </section>
  );
}
