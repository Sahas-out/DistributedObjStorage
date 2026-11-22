import { useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { ThroughputPoint } from "../../hooks/useThroughput";
import { formatClock, formatReqRate, shortId } from "../../lib/format";
import { workerColor } from "../../lib/palette";
import { ChartTooltipBox } from "./ChartTooltip";

type Mode = "total" | "put" | "get" | "delete";
const modes: { key: Mode; label: string; title: string }[] = [
  { key: "total", label: "All", title: "Every chunk-API request" },
  { key: "put", label: "Writes", title: "PUT: chunks received from the controller" },
  { key: "get", label: "Reads", title: "GET: chunks sent to the controller" },
  { key: "delete", label: "Deletes", title: "DELETE: chunks removed" },
];

export function NetworkChart({ points, workerIds, windowMs }: { points: ThroughputPoint[]; workerIds: string[]; windowMs: number }) {
  const [mode, setMode] = useState<Mode>("total");
  const [hidden, setHidden] = useState<Set<string>>(new Set());

  const data = useMemo(
    () =>
      points.map((p) => {
        const row: Record<string, number | null> = { t: p.t };
        for (const id of workerIds) {
          const r = p.rates[id];
          row[id] = r ? r[mode] : null;
        }
        return row;
      }),
    [points, workerIds, mode],
  );

  const now = points.length ? points[points.length - 1].t : Date.now();

  return (
    <section className="card chart-card">
      <header className="chart-head">
        <div>
          <h2>Network load per worker</h2>
          <p className="muted small">
            Requests/second each worker serves for the controller, from its request counters · last{" "}
            {Math.round(windowMs / 60000)} min
          </p>
        </div>
        <div className="seg" role="group" aria-label="Direction">
          {modes.map((m) => (
            <button
              key={m.key}
              type="button"
              title={m.title}
              className={mode === m.key ? "is-on" : ""}
              onClick={() => setMode(m.key)}
            >
              {m.label}
            </button>
          ))}
        </div>
      </header>
      <div className="legend">
        {workerIds.map((id) => (
          <button
            type="button"
            key={id}
            className={`legend-item ${hidden.has(id) ? "is-off" : ""}`}
            onClick={() =>
              setHidden((h) => {
                const n = new Set(h);
                if (n.has(id)) n.delete(id);
                else n.add(id);
                return n;
              })
            }
            title={hidden.has(id) ? "Show" : "Hide"}
          >
            <span className="swatch-line" style={{ background: workerColor(id) }} />
            <span className="mono">{shortId(id, 10)}</span>
          </button>
        ))}
      </div>
      <div className="chart-body">
        {points.length < 2 ? (
          <div className="empty">Collecting samples…</div>
        ) : (
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={data} margin={{ top: 8, right: 16, bottom: 4, left: 0 }}>
              <CartesianGrid stroke="var(--grid)" vertical={false} />
              <XAxis
                dataKey="t"
                type="number"
                scale="time"
                domain={[now - windowMs, now]}
                tickFormatter={(t: number) => formatClock(t)}
                stroke="var(--axis)"
                tick={{ fill: "var(--muted)", fontSize: 11 }}
                tickLine={false}
                minTickGap={40}
              />
              <YAxis
                stroke="var(--axis)"
                tick={{ fill: "var(--muted)", fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                width={40}
                tickFormatter={(v: number) => (v < 10 ? v.toFixed(1) : v.toFixed(0))}
                allowDecimals
              />
              <Tooltip
                cursor={{ stroke: "var(--axis)", strokeWidth: 1 }}
                isAnimationActive={false}
                content={({ active, label, payload }) => {
                  if (!active || !payload?.length) return null;
                  const rows = payload
                    .filter((p) => typeof p.value === "number")
                    .sort((a, b) => (b.value as number) - (a.value as number))
                    .map((p) => ({
                      key: String(p.dataKey),
                      color: workerColor(String(p.dataKey)),
                      label: shortId(String(p.dataKey), 12),
                      value: formatReqRate(p.value as number),
                    }));
                  return <ChartTooltipBox title={formatClock(Number(label))} rows={rows} />;
                }}
              />
              {workerIds
                .filter((id) => !hidden.has(id))
                .map((id) => (
                  <Line
                    key={id}
                    dataKey={id}
                    type="monotone"
                    stroke={workerColor(id)}
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4, stroke: "var(--surface)", strokeWidth: 2 }}
                    isAnimationActive={false}
                    connectNulls={false}
                  />
                ))}
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  );
}
