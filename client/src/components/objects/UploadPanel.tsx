import { useRef, useState } from "react";
import { METADATA_API_ENABLED } from "../../api/objects";
import type { CustomMetadata } from "../../api/types";
import { MetadataEditor } from "./MetadataEditor";

export function UploadPanel({ onFiles }: { onFiles: (files: File[]) => void }) {
  const input = useRef<HTMLInputElement>(null);
  const [over, setOver] = useState(false);
  const [meta, setMeta] = useState<CustomMetadata>({});
  const [showMeta, setShowMeta] = useState(false);

  const take = (list: FileList | null) => {
    if (list && list.length > 0) onFiles(Array.from(list));
  };

  return (
    <section className="card upload-card">
      <div
        className={`dropzone ${over ? "is-over" : ""}`}
        onDragOver={(e) => {
          e.preventDefault();
          setOver(true);
        }}
        onDragLeave={() => setOver(false)}
        onDrop={(e) => {
          e.preventDefault();
          setOver(false);
          take(e.dataTransfer.files);
        }}
        onClick={() => input.current?.click()}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && input.current?.click()}
      >
        <div className="dropzone-icon" aria-hidden>
          ⇪
        </div>
        <div>
          <div className="dropzone-title">Drop files here or click to upload</div>
          <div className="muted small">Each file becomes one object, split into 1 MiB chunks with 2 copies of each.</div>
        </div>
        <button
          type="button"
          className="btn btn-primary"
          onClick={(e) => {
            e.stopPropagation();
            input.current?.click();
          }}
        >
          Upload
        </button>
        <input
          ref={input}
          type="file"
          multiple
          hidden
          onChange={(e) => {
            take(e.target.files);
            e.target.value = "";
          }}
        />
      </div>

      <button type="button" className="link-btn" onClick={() => setShowMeta((s) => !s)} aria-expanded={showMeta}>
        {showMeta ? "▾" : "▸"} Custom metadata for new uploads
        {!METADATA_API_ENABLED && <span className="badge badge-soon">coming soon</span>}
      </button>
      {showMeta && (
        <div className="upload-meta">
          {!METADATA_API_ENABLED && (
            <p className="muted small">
              The controller doesn't support custom metadata yet (planned <code>PATCH /api/v1/objects/&#123;id&#125;/metadata</code>).
              Uploads store system metadata only: size, SHA-256 checksum, chunk count and creation time.
            </p>
          )}
          <MetadataEditor value={meta} onChange={setMeta} disabled={!METADATA_API_ENABLED} />
        </div>
      )}
    </section>
  );
}
