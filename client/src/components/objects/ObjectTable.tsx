import { useMemo, useState } from "react";
import type { ObjectInfo } from "../../api/types";
import { formatBytes, formatRelative, shortId } from "../../lib/format";
import { CopyButton } from "../CopyButton";

type SortKey = "created_at" | "size" | "chunk_count" | "name";

export function ObjectTable({
  objects,
  busy,
  onDownload,
  onDelete,
  onDetails,
}: {
  objects: ObjectInfo[];
  busy: Set<string>;
  onDownload: (o: ObjectInfo) => void;
  onDelete: (o: ObjectInfo) => void;
  onDetails: (o: ObjectInfo) => void;
}) {
  const [filter, setFilter] = useState("");
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({ key: "created_at", desc: true });

  const rows = useMemo(() => {
    const f = filter.trim().toLowerCase();
    const list = f
      ? objects.filter(
          (o) =>
            o.name.toLowerCase().includes(f) ||
            o.content_type.toLowerCase().includes(f) ||
            o.object_id.includes(f) ||
            o.checksum.includes(f),
        )
      : objects.slice();
    list.sort((a, b) => {
      if (sort.key === "name") return sort.desc ? b.name.localeCompare(a.name) : a.name.localeCompare(b.name);
      const av = sort.key === "created_at" ? Date.parse(a.created_at) : a[sort.key];
      const bv = sort.key === "created_at" ? Date.parse(b.created_at) : b[sort.key];
      return sort.desc ? bv - av : av - bv;
    });
    return list;
  }, [objects, filter, sort]);

  const header = (key: SortKey, label: string, num = true) => (
    <th
      className={`sortable${num ? " num" : ""}`}
      aria-sort={sort.key === key ? (sort.desc ? "descending" : "ascending") : "none"}
      onClick={() => setSort((s) => ({ key, desc: s.key === key ? !s.desc : true }))}
    >
      {label} {sort.key === key ? (sort.desc ? "↓" : "↑") : ""}
    </th>
  );

  return (
    <>
      <div className="table-tools">
        <input
          className="search"
          placeholder="Filter by name, type, object ID or checksum…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
        <span className="muted small">
          {rows.length} of {objects.length} objects
        </span>
      </div>
      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr>
              {header("name", "Name", false)}
              <th>Type</th>
              <th>Object ID</th>
              {header("size", "Size")}
              {header("chunk_count", "Chunks")}
              <th>SHA-256</th>
              {header("created_at", "Created")}
              <th className="actions-col">Actions</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && (
              <tr>
                <td colSpan={8} className="empty">
                  {objects.length === 0 ? "No objects yet. Upload a file to get started." : "No objects match the filter."}
                </td>
              </tr>
            )}
            {rows.map((o) => (
              <tr key={o.object_id} className={busy.has(o.object_id) ? "is-busy" : ""}>
                <td>
                  <button type="button" className="link-btn" onClick={() => onDetails(o)} title={o.name}>
                    {o.name}
                  </button>
                </td>
                <td className="muted small">{o.content_type}</td>
                <td>
                  <span className="id-cell mono muted" title={o.object_id}>
                    {shortId(o.object_id, 12)}
                    <CopyButton text={o.object_id} label="Copy object ID" />
                  </span>
                </td>
                <td className="num">{formatBytes(o.size)}</td>
                <td className="num">{o.chunk_count}</td>
                <td>
                  <span className="id-cell mono muted" title={o.checksum}>
                    {shortId(o.checksum, 12)}
                    <CopyButton text={o.checksum} label="Copy checksum" />
                  </span>
                </td>
                <td title={new Date(o.created_at).toLocaleString()}>{formatRelative(o.created_at)}</td>
                <td className="actions-col">
                  <button type="button" className="btn btn-ghost" onClick={() => onDownload(o)}>
                    Download
                  </button>
                  <button type="button" className="btn btn-ghost" onClick={() => onDetails(o)}>
                    Details
                  </button>
                  <button
                    type="button"
                    className="btn btn-danger"
                    disabled={busy.has(o.object_id)}
                    onClick={() => onDelete(o)}
                  >
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
