import { useState } from "react";
import type { CustomMetadata } from "../../api/types";

/**
 * Key/value editor for custom object metadata. Rendered disabled until the
 * controller implements the metadata API (METADATA_API_ENABLED).
 */
export function MetadataEditor({
  value,
  onChange,
  disabled,
}: {
  value: CustomMetadata;
  onChange: (v: CustomMetadata) => void;
  disabled: boolean;
}) {
  const [key, setKey] = useState("");
  const [val, setVal] = useState("");
  const entries = Object.entries(value);

  return (
    <div className={`kv-editor ${disabled ? "is-disabled" : ""}`}>
      {entries.length === 0 && <div className="muted small">No custom metadata.</div>}
      {entries.map(([k, v]) => (
        <div className="kv-row" key={k}>
          <code>{k}</code>
          <span>{v}</span>
          <button
            type="button"
            className="icon-btn"
            disabled={disabled}
            aria-label={`Remove ${k}`}
            onClick={() => {
              const next = { ...value };
              delete next[k];
              onChange(next);
            }}
          >
            ✕
          </button>
        </div>
      ))}
      <form
        className="kv-add"
        onSubmit={(e) => {
          e.preventDefault();
          if (!key.trim()) return;
          onChange({ ...value, [key.trim()]: val });
          setKey("");
          setVal("");
        }}
      >
        <input placeholder="key" value={key} onChange={(e) => setKey(e.target.value)} disabled={disabled} />
        <input placeholder="value" value={val} onChange={(e) => setVal(e.target.value)} disabled={disabled} />
        <button type="submit" className="btn btn-ghost" disabled={disabled}>
          Add
        </button>
      </form>
    </div>
  );
}
