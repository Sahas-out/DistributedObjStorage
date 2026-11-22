import type { Transfer } from "../../hooks/useTransfers";
import { formatBytes, formatDuration, formatRate } from "../../lib/format";

export function TransferList({
  transfers,
  onCancel,
  onClear,
}: {
  transfers: Transfer[];
  onCancel: (id: number) => void;
  onClear: () => void;
}) {
  if (transfers.length === 0) return null;
  const running = transfers.filter((t) => t.state === "running").length;
  return (
    <section className="transfers" aria-label="Transfers">
      <header>
        <strong>Transfers</strong>
        <span className="muted small">{running > 0 ? `${running} in progress` : "all finished"}</span>
        <button type="button" className="link-btn small" onClick={onClear} disabled={running === transfers.length}>
          Clear finished
        </button>
      </header>
      <ul>
        {transfers.map((t) => {
          const pct = t.total > 0 ? Math.min(100, (t.loaded / t.total) * 100) : 0;
          const eta = t.rate > 0 && t.total > 0 ? (t.total - t.loaded) / t.rate : NaN;
          // Upload bytes finish leaving the browser before the controller has
          // written the last chunk to both workers and answered.
          const finishing = t.kind === "upload" && t.state === "running" && t.total > 0 && t.loaded >= t.total;
          return (
            <li key={t.id} className={`transfer is-${t.state}`}>
              <div className="transfer-top">
                <span className={`kind kind-${t.kind}`}>{t.kind === "upload" ? "↑" : "↓"}</span>
                <span className="transfer-name" title={t.name}>
                  {t.name}
                </span>
                {t.state === "running" && (
                  <button type="button" className="icon-btn" onClick={() => onCancel(t.id)} aria-label="Cancel">
                    ✕
                  </button>
                )}
              </div>
              <div
                className="progress"
                role="progressbar"
                aria-valuemin={0}
                aria-valuemax={100}
                aria-valuenow={Math.round(pct)}
              >
                <div className={`progress-bar ${finishing ? "is-indeterminate" : ""}`} style={{ width: `${pct}%` }} />
              </div>
              <div className="transfer-meta small">
                {t.state === "running" && (
                  <>
                    <span>
                      {pct.toFixed(0)}% · {formatBytes(t.loaded)} / {formatBytes(t.total)}
                    </span>
                    <span className="muted">
                      {finishing ? "storing replicas…" : `${formatRate(t.rate)}${Number.isFinite(eta) ? ` · ${eta < 1 ? "<1s" : formatDuration(eta)} left` : ""}`}
                    </span>
                  </>
                )}
                {t.state === "done" && (
                  <>
                    <span className="ok">✓ {t.kind === "upload" ? "Uploaded" : "Downloaded"} {formatBytes(t.total)}</span>
                    <span className="muted">
                      {formatRate((t.total * 1000) / Math.max(1, (t.finishedAt ?? Date.now()) - t.startedAt))} avg
                    </span>
                  </>
                )}
                {t.state === "failed" && <span className="err">✕ {t.error}</span>}
                {t.state === "cancelled" && <span className="muted">Cancelled</span>}
              </div>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
