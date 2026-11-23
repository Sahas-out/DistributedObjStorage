import { useEffect, useState } from "react";
import { describeError } from "../../api/http";
import { getMetadata, patchMetadata } from "../../api/objects";
import type { CustomMetadata, MetadataPatch, ObjectInfo, ObjectMetadata } from "../../api/types";
import { formatBytes, shortId } from "../../lib/format";
import { CopyButton } from "../CopyButton";
import { MetadataEditor } from "./MetadataEditor";

/** Pill colour for a replica status. */
function replicaPill(status: string): string {
  if (status === "ACTIVE") return "ready";
  if (status === "CORRUPTED" || status === "LOST") return "dead";
  return "syncing";
}

/** The PATCH body that turns `saved` into the edited values; null removes a key. */
function buildPatch(saved: ObjectMetadata, name: string, contentType: string, custom: CustomMetadata): MetadataPatch {
  const patch: MetadataPatch = {};
  if (name !== saved.name) patch.name = name;
  if (contentType !== saved.content_type) patch.content_type = contentType;
  const changes: Record<string, string | null> = {};
  for (const k of Object.keys(saved.custom_metadata)) if (!(k in custom)) changes[k] = null;
  for (const [k, v] of Object.entries(custom)) if (saved.custom_metadata[k] !== v) changes[k] = v;
  if (Object.keys(changes).length > 0) patch.custom_metadata = changes;
  return patch;
}

export function MetadataDrawer({
  object,
  onClose,
  onDownload,
  onChanged,
}: {
  object: ObjectInfo;
  onClose: () => void;
  onDownload: (o: ObjectInfo) => void;
  onChanged: () => void;
}) {
  const [saved, setSaved] = useState<ObjectMetadata | null>(null);
  const [name, setName] = useState(object.name);
  const [contentType, setContentType] = useState(object.content_type);
  const [custom, setCustom] = useState<CustomMetadata>({});
  const [status, setStatus] = useState<string>("");
  const [saving, setSaving] = useState(false);

  const load = (m: ObjectMetadata) => {
    setSaved(m);
    setName(m.name);
    setContentType(m.content_type);
    setCustom(m.custom_metadata);
  };

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    const ctrl = new AbortController();
    setStatus("");
    getMetadata(object.object_id, ctrl.signal).then(load, (e) => !ctrl.signal.aborted && setStatus(describeError(e)));
    return () => ctrl.abort();
  }, [object.object_id]);

  const info: ObjectInfo = saved ?? object;
  const patch = saved ? buildPatch(saved, name, contentType, custom) : {};
  const dirty = Object.keys(patch).length > 0;

  const save = async () => {
    setSaving(true);
    try {
      load(await patchMetadata(object.object_id, patch));
      setStatus("Saved");
      onChanged();
    } catch (e) {
      setStatus(describeError(e));
    } finally {
      setSaving(false);
    }
  };

  const rows: [string, React.ReactNode][] = [
    ["Object ID", <span className="id-cell mono">{info.object_id}<CopyButton text={info.object_id} /></span>],
    ["Size", `${formatBytes(info.size, 2)} (${info.size.toLocaleString()} bytes)`],
    ["Chunks", `${info.chunk_count} × up to 1 MiB, 2 copies each`],
    ["SHA-256", <span className="id-cell mono wrap">{info.checksum}<CopyButton text={info.checksum} /></span>],
    ["Created", new Date(info.created_at).toLocaleString()],
    ["Modified", new Date(info.last_modified_at).toLocaleString()],
  ];

  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" role="dialog" aria-label="Object details" onClick={(e) => e.stopPropagation()}>
        <header className="drawer-head">
          <h2>{info.name}</h2>
          <button type="button" className="icon-btn" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </header>

        <h3>System metadata</h3>
        <dl className="meta-list">
          {rows.map(([k, v]) => (
            <div key={k}>
              <dt>{k}</dt>
              <dd>{v}</dd>
            </div>
          ))}
        </dl>
        <button type="button" className="btn btn-primary" onClick={() => onDownload(info)}>
          Download
        </button>

        <h3>Editable metadata</h3>
        <p className="muted small">
          Saved with <code>PATCH /api/v1/objects/&#123;id&#125;/metadata</code>; the object's data is not uploaded again.
        </p>
        <div className="meta-fields">
          <label>
            <span className="muted">Name</span>
            <input value={name} onChange={(e) => setName(e.target.value)} disabled={!saved} />
          </label>
          <label>
            <span className="muted">Content type</span>
            <input value={contentType} onChange={(e) => setContentType(e.target.value)} disabled={!saved} />
          </label>
        </div>
        <h4 className="muted small">Custom metadata</h4>
        <MetadataEditor value={custom} onChange={setCustom} disabled={!saved} />
        <div className="drawer-actions">
          <button type="button" className="btn btn-primary" disabled={!dirty || saving} onClick={save}>
            {saving ? "Saving…" : "Save changes"}
          </button>
          {dirty && (
            <button type="button" className="btn btn-ghost" disabled={saving} onClick={() => saved && load(saved)}>
              Discard
            </button>
          )}
          {status && <span className="muted small">{status}</span>}
        </div>

        <h3>Chunk locations</h3>
        {!saved ? (
          <div className="muted small">Loading…</div>
        ) : saved.chunks.length === 0 ? (
          <div className="muted small">Empty object: no chunks.</div>
        ) : (
          <table className="table chunk-table">
            <thead>
              <tr>
                <th className="num">#</th>
                <th className="num">Size</th>
                <th>Copies</th>
              </tr>
            </thead>
            <tbody>
              {saved.chunks.map((c) => (
                <tr key={c.sequence_no}>
                  <td className="num" title={`SHA-256 ${c.checksum}`}>
                    {c.sequence_no}
                  </td>
                  <td className="num">{formatBytes(c.size)}</td>
                  <td>
                    {c.replicas.length === 0 && <span className="muted small">no known copy</span>}
                    {c.replicas.map((r) => (
                      <div key={r.chunk_id} className="replica" title={`chunk ${r.chunk_id} at ${r.worker_addr}`}>
                        <span className="mono">{r.worker_id}</span>
                        <span className="muted mono small">{shortId(r.chunk_id)}</span>
                        <span className={`pill pill-${replicaPill(r.status)}`}>
                          <span>●</span> {r.status}
                        </span>
                      </div>
                    ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </aside>
    </div>
  );
}
