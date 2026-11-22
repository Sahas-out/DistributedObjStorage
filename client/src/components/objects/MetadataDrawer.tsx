import { useEffect, useState } from "react";
import { describeError } from "../../api/http";
import { METADATA_API_ENABLED, getMetadata, patchMetadata } from "../../api/objects";
import type { CustomMetadata, ObjectInfo } from "../../api/types";
import { formatBytes } from "../../lib/format";
import { CopyButton } from "../CopyButton";
import { MetadataEditor } from "./MetadataEditor";

export function MetadataDrawer({
  object,
  onClose,
  onDownload,
}: {
  object: ObjectInfo;
  onClose: () => void;
  onDownload: (o: ObjectInfo) => void;
}) {
  const [custom, setCustom] = useState<CustomMetadata>({});
  const [saved, setSaved] = useState<CustomMetadata>({});
  const [status, setStatus] = useState<string>("");

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    if (!METADATA_API_ENABLED) return;
    const ctrl = new AbortController();
    getMetadata(object.object_id, ctrl.signal).then(
      (m) => {
        setCustom(m);
        setSaved(m);
      },
      (e) => !ctrl.signal.aborted && setStatus(describeError(e)),
    );
    return () => ctrl.abort();
  }, [object.object_id]);

  const dirty = JSON.stringify(custom) !== JSON.stringify(saved);

  const rows: [string, React.ReactNode][] = [
    ["Object ID", <span className="id-cell mono">{object.object_id}<CopyButton text={object.object_id} /></span>],
    ["Size", `${formatBytes(object.size, 2)} (${object.size.toLocaleString()} bytes)`],
    ["Chunks", `${object.chunk_count} × up to 1 MiB, 2 copies each`],
    ["SHA-256", <span className="id-cell mono wrap">{object.checksum}<CopyButton text={object.checksum} /></span>],
    ["Created", new Date(object.created_at).toLocaleString()],
  ];

  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" role="dialog" aria-label="Object details" onClick={(e) => e.stopPropagation()}>
        <header className="drawer-head">
          <h2>Object details</h2>
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
        <button type="button" className="btn btn-primary" onClick={() => onDownload(object)}>
          Download
        </button>

        <h3>
          Custom metadata {!METADATA_API_ENABLED && <span className="badge badge-soon">coming soon</span>}
        </h3>
        {!METADATA_API_ENABLED && (
          <p className="muted small">
            Viewing and editing custom metadata needs <code>GET</code>/<code>PATCH /api/v1/objects/&#123;id&#125;/metadata</code>, which the
            controller doesn't implement yet. The editor below is a preview.
          </p>
        )}
        <MetadataEditor value={custom} onChange={setCustom} disabled={!METADATA_API_ENABLED} />
        <div className="drawer-actions">
          <button
            type="button"
            className="btn btn-primary"
            disabled={!METADATA_API_ENABLED || !dirty}
            onClick={async () => {
              try {
                const m = await patchMetadata(object.object_id, custom);
                setCustom(m);
                setSaved(m);
                setStatus("Saved");
              } catch (e) {
                setStatus(describeError(e));
              }
            }}
          >
            Save (PATCH)
          </button>
          {status && <span className="muted small">{status}</span>}
        </div>
      </aside>
    </div>
  );
}
