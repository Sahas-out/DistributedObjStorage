import { useRef, useState } from "react";
import type { CustomMetadata } from "../../api/types";
import { MetadataEditor } from "./MetadataEditor";

export function UploadPanel({ onFiles }: { onFiles: (files: File[], custom: CustomMetadata) => void }) {
  const input = useRef<HTMLInputElement>(null);
  const [over, setOver] = useState(false);
  const [meta, setMeta] = useState<CustomMetadata>({});
  const [showMeta, setShowMeta] = useState(false);

  const take = (list: FileList | null) => {
    if (list && list.length > 0) onFiles(Array.from(list), meta);
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
        {Object.keys(meta).length > 0 && <span className="badge badge-count">{Object.keys(meta).length}</span>}
      </button>
      {showMeta && (
        <div className="upload-meta">
          <p className="muted small">
            These key/value pairs are attached to every file uploaded while they are set. The file's name and type are stored
            too; all of it can be changed later in the object's details.
          </p>
          <MetadataEditor value={meta} onChange={setMeta} disabled={false} />
        </div>
      )}
    </section>
  );
}
