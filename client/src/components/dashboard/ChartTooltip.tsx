export interface TooltipRow {
  key: string;
  color: string;
  label: string;
  value: string;
}

export function ChartTooltipBox({ title, rows }: { title: string; rows: TooltipRow[] }) {
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-title">{title}</div>
      {rows.map((r) => (
        <div className="chart-tooltip-row" key={r.key}>
          <span className="swatch-line" style={{ background: r.color }} />
          <span className="mono">{r.label}</span>
          <span className="chart-tooltip-value">{r.value}</span>
        </div>
      ))}
    </div>
  );
}
