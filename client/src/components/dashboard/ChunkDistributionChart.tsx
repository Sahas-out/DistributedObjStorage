import { Bar, BarChart, CartesianGrid, Cell, LabelList, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { DashboardWorker } from "../../api/types";
import { formatBytes, shortId } from "../../lib/format";
import { workerColor } from "../../lib/palette";
import { ChartTooltipBox } from "./ChartTooltip";

export function ChunkDistributionChart({ workers }: { workers: DashboardWorker[] }) {
  const data = workers.map((w) => ({
    id: w.worker_id,
    label: shortId(w.worker_id, 10),
    chunks: w.chunk_count,
    bytes: w.stats?.stored_bytes ?? null,
  }));
  const counts = workers.map((w) => w.chunk_count);
  const spread = counts.length ? Math.max(...counts) - Math.min(...counts) : 0;

  return (
    <section className="card chart-card">
      <header className="chart-head">
        <div>
          <h2>Chunk copies per worker</h2>
          <p className="muted small">
            Placement sends each chunk to the 2 least-loaded READY workers · spread (max − min): <strong>{spread}</strong>
          </p>
        </div>
      </header>
      <div className="chart-body">
        {data.length === 0 ? (
          <div className="empty">No workers registered.</div>
        ) : (
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={data} margin={{ top: 20, right: 8, bottom: 4, left: 0 }} barCategoryGap="30%">
              <CartesianGrid stroke="var(--grid)" vertical={false} />
              <XAxis dataKey="label" stroke="var(--axis)" tick={{ fill: "var(--muted)", fontSize: 11 }} tickLine={false} />
              <YAxis
                stroke="var(--axis)"
                tick={{ fill: "var(--muted)", fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                allowDecimals={false}
                width={40}
              />
              <Tooltip
                cursor={{ fill: "var(--hover)" }}
                isAnimationActive={false}
                content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload as (typeof data)[number];
                  return (
                    <ChartTooltipBox
                      title={d.id}
                      rows={[
                        { key: "c", color: workerColor(d.id), label: "chunk copies", value: d.chunks.toLocaleString() },
                        { key: "b", color: workerColor(d.id), label: "stored", value: d.bytes === null ? "–" : formatBytes(d.bytes) },
                      ]}
                    />
                  );
                }}
              />
              <Bar dataKey="chunks" radius={[4, 4, 0, 0]} isAnimationActive={false} maxBarSize={56}>
                {data.map((d) => (
                  <Cell key={d.id} fill={workerColor(d.id)} />
                ))}
                <LabelList dataKey="chunks" position="top" fill="var(--text-2)" fontSize={11} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  );
}
