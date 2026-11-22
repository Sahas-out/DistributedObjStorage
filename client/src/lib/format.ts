const units = ["B", "KiB", "MiB", "GiB", "TiB"];

export function formatBytes(n: number, digits = 1): string {
  if (!Number.isFinite(n) || n < 0) return "–";
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${i === 0 ? v : v.toFixed(digits)} ${units[i]}`;
}

/** Bytes per second as MB/s-style text (binary units). */
export function formatRate(bps: number): string {
  if (!Number.isFinite(bps) || bps <= 0) return "0 B/s";
  return `${formatBytes(bps)}/s`;
}

/** Requests per second. */
export function formatReqRate(rps: number): string {
  if (!Number.isFinite(rps) || rps <= 0) return "0 req/s";
  return `${rps < 10 ? rps.toFixed(1) : Math.round(rps)} req/s`;
}

export function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "–";
  const s = Math.floor(seconds);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

export function formatRelative(iso: string, now = Date.now()): string {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "–";
  const d = Math.max(0, (now - t) / 1000);
  if (d < 5) return "just now";
  return `${formatDuration(d)} ago`;
}

export function formatClock(iso: string | number): string {
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export function shortId(id: string, n = 8): string {
  return id.length > n ? `${id.slice(0, n)}…` : id;
}

export function formatCount(n: number): string {
  return n.toLocaleString();
}
