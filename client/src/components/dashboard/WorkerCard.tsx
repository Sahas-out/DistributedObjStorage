import type { DashboardWorker } from "../../api/types";
import type { Rate } from "../../hooks/useThroughput";
import { formatBytes, formatDuration, formatReqRate } from "../../lib/format";
import { workerColor } from "../../lib/palette";

export type Health = "ready" | "syncing" | "unreachable" | "dead";

export function workerHealth(w: DashboardWorker): Health {
  if (w.status === "DEAD") return "dead";
  if (!w.stats) return "unreachable";
  return w.status === "READY" ? "ready" : "syncing";
}

const healthLabel: Record<Health, { icon: string; text: string }> = {
  ready: { icon: "●", text: "READY" },
  syncing: { icon: "◐", text: "AVAILABLE · loading chunks" },
  unreachable: { icon: "▲", text: "UNREACHABLE" },
  dead: { icon: "✕", text: "DEAD · missed heartbeats" },
};

export function StatusPill({ health }: { health: Health }) {
  const l = healthLabel[health];
  return (
    <span className={`pill pill-${health}`}>
      <span aria-hidden>{l.icon}</span> {l.text}
    </span>
  );
}

export function WorkerCard({ worker, rate, maxChunks }: { worker: DashboardWorker; rate?: Rate; maxChunks: number }) {
  const h = workerHealth(worker);
  const s = worker.stats;
  const share = maxChunks > 0 ? (worker.chunk_count / maxChunks) * 100 : 0;
  return (
    <article className={`worker-card is-${h}`} style={{ "--worker": workerColor(worker.worker_id) } as React.CSSProperties}>
      <header>
        <span className="swatch" aria-hidden />
        <span className="mono worker-id" title={worker.worker_id}>
          {worker.worker_id}
        </span>
        <StatusPill health={h} />
      </header>
      <div className="muted small mono">{worker.addr.replace(/^https?:\/\//, "")}</div>

      <div className="worker-chunks">
        <span className="big">{worker.chunk_count.toLocaleString()}</span>
        <span className="muted small">chunk copies</span>
      </div>
      <div className="meter" aria-hidden>
        <div style={{ width: `${share}%` }} />
      </div>

      <dl className="worker-stats">
        <div>
          <dt>Stored</dt>
          <dd>{s ? formatBytes(s.stored_bytes) : "–"}</dd>
        </div>
        <div>
          <dt>Uptime</dt>
          <dd>{s ? formatDuration(s.uptime_seconds) : "–"}</dd>
        </div>
        <div>
          <dt>Load now</dt>
          <dd>{rate ? formatReqRate(rate.total) : "–"}</dd>
        </div>
        <div>
          <dt>Writes / reads now</dt>
          <dd>{rate ? `${formatReqRate(rate.put)} / ${formatReqRate(rate.get)}` : "–"}</dd>
        </div>
        <div>
          <dt>Total requests</dt>
          <dd title={s ? `PUT ${s.requests.put} · GET ${s.requests.get} · DELETE ${s.requests.delete} · LIST ${s.requests.list}` : ""}>
            {s ? (
              <>
                {s.requests.put} put · {s.requests.get} get · {s.requests.delete} del
                {s.requests.errors > 0 && <span className="err"> · {s.requests.errors} err</span>}
              </>
            ) : (
              "–"
            )}
          </dd>
        </div>
      </dl>
      {h === "dead" ? (
        <div className="small err worker-error">No heartbeat; its copies are being restored on other workers</div>
      ) : worker.stats_error && (
        <div className="small err worker-error" title={worker.stats_error}>
          Controller can't reach this worker
        </div>
      )}
    </article>
  );
}
