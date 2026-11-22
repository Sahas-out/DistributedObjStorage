import type { DashboardOverview } from "../../api/types";
import { formatBytes, formatDuration, formatReqRate } from "../../lib/format";

export function KpiRow({ overview, clusterRate }: { overview: DashboardOverview; clusterRate: number }) {
  const ws = overview.workers;
  const ready = ws.filter((w) => w.status === "READY" && w.stats).length;
  const copies = ws.reduce((s, w) => s + w.chunk_count, 0);
  const stored = ws.reduce((s, w) => s + (w.stats?.stored_bytes ?? 0), 0);
  const c = overview.controller;
  const uptime = (Date.parse(overview.server_time) - Date.parse(c.started_at)) / 1000;
  const r = c.recovery;
  const dead = ws.filter((w) => w.status === "DEAD").length;
  const scanned = r && Date.parse(r.last_scan) > 0;
  const recovery = !scanned
    ? { value: "–", sub: "waiting for first scan", warn: false }
    : r.unavailable_chunks > 0
      ? { value: `${r.unavailable_chunks} unavailable`, sub: "no readable copy; waiting for a dead worker", warn: true }
      : r.degraded_chunks > 0
        ? { value: `${r.degraded_chunks} degraded`, sub: "restoring missing copies", warn: true }
        : { value: "Healthy", sub: `every chunk has ${c.replication_factor} copies${r.orphan_replicas ? ` · ${r.orphan_replicas} orphans` : ""}`, warn: false };

  const tiles = [
    { label: "Workers ready", value: `${ready} / ${ws.length}`, sub: ready < c.replication_factor ? `need ≥ ${c.replication_factor} to accept uploads` : dead > 0 ? `${dead} dead` : `replication factor ${c.replication_factor}`, warn: ready < c.replication_factor || dead > 0 },
    { label: "Data health", ...recovery },
    { label: "Objects", value: c.object_count.toLocaleString(), sub: `${formatBytes(c.logical_bytes)} logical` },
    { label: "Chunk copies", value: copies.toLocaleString(), sub: `${formatBytes(stored)} on worker disks` },
    { label: "Cluster load", value: formatReqRate(clusterRate), sub: "chunk requests, all workers" },
    { label: "Controller uptime", value: formatDuration(uptime), sub: `announce every ${c.announce_interval_seconds}s` },
  ];
  return (
    <div className="kpi-row">
      {tiles.map((t) => (
        <div className={`kpi ${t.warn ? "is-warn" : ""}`} key={t.label}>
          <div className="kpi-label">{t.label}</div>
          <div className="kpi-value">{t.value}</div>
          <div className="kpi-sub">{t.warn ? "⚠ " : ""}{t.sub}</div>
        </div>
      ))}
    </div>
  );
}
