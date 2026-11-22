import { useMemo } from "react";
import { getOverview } from "../api/dashboard";
import { describeError } from "../api/http";
import { ActivityFeed } from "../components/dashboard/ActivityFeed";
import { ChunkDistributionChart } from "../components/dashboard/ChunkDistributionChart";
import { HeartbeatList } from "../components/dashboard/HeartbeatList";
import { KpiRow } from "../components/dashboard/KpiRow";
import { NetworkChart } from "../components/dashboard/NetworkChart";
import { WorkerCard } from "../components/dashboard/WorkerCard";
import { useEvents } from "../hooks/useEvents";
import { usePolling } from "../hooks/usePolling";
import { useThroughput } from "../hooks/useThroughput";
import { formatClock } from "../lib/format";

const POLL_MS = 2000;
const WINDOW_MS = 5 * 60 * 1000;

export function DashboardPage() {
  const overview = usePolling(getOverview, POLL_MS);
  const feed = useEvents(POLL_MS);
  const { points, latest } = useThroughput(overview.data, WINDOW_MS);

  // Every worker seen in the chart window, so a worker that left keeps its
  // line (and color) until it scrolls out.
  const chartWorkers = useMemo(() => {
    const ids = new Set<string>();
    overview.data?.workers.forEach((w) => ids.add(w.worker_id));
    points.forEach((p) => Object.keys(p.rates).forEach((id) => ids.add(id)));
    return [...ids].sort();
  }, [overview.data, points]);

  const clusterRate = Object.values(latest).reduce((s, r) => s + r.total, 0);
  const data = overview.data;
  const maxChunks = data ? Math.max(1, ...data.workers.map((w) => w.chunk_count)) : 1;

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Cluster dashboard</h1>
          <p className="muted">
            Live view of the controller and its workers · refreshes every {POLL_MS / 1000}s
            {overview.updatedAt > 0 && <> · updated {formatClock(overview.updatedAt)}</>}
          </p>
        </div>
      </div>

      {overview.error !== undefined && (
        <div className="alert">Can't reach the controller: {describeError(overview.error)}. Showing the last known state.</div>
      )}

      {!data ? (
        <div className="card empty">{overview.loading ? "Loading…" : "No data"}</div>
      ) : (
        <>
          <KpiRow overview={data} clusterRate={clusterRate} />

          <section>
            <h2 className="section-title">Workers</h2>
            <div className="worker-grid">
              {data.workers.length === 0 && <div className="card empty">No workers registered yet.</div>}
              {data.workers.map((w) => (
                <WorkerCard key={w.worker_id} worker={w} rate={latest[w.worker_id]} maxChunks={maxChunks} />
              ))}
            </div>
          </section>

          <NetworkChart points={points} workerIds={chartWorkers} windowMs={WINDOW_MS} />

          <div className="grid-2">
            <ChunkDistributionChart workers={data.workers} />
            <HeartbeatList overview={data} />
          </div>
        </>
      )}

      <ActivityFeed events={feed.events} error={feed.error} />
    </div>
  );
}
