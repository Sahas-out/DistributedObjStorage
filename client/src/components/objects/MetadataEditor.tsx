import { useState } from "react";
import type { CustomMetadata } from "../../api/types";

// Same rule as the controller (controller/internal/object/metadata.go).
const KEY_PATTERN = /^[A-Za-z0-9._-]{1,128}$/;

/** Key/value editor for custom object metadata. */
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
  const trimmed = key.trim();
  const keyError = trimmed && !KEY_PATTERN.test(trimmed) ? "Keys may use letters, digits, '.', '_' and '-' (up to 128)." : "";

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
          if (!trimmed || keyError) return;
          onChange({ ...value, [trimmed]: val });
          setKey("");
          setVal("");
        }}
      >
        <input placeholder="key" value={key} onChange={(e) => setKey(e.target.value)} disabled={disabled} />
        <input placeholder="value" value={val} onChange={(e) => setVal(e.target.value)} disabled={disabled} />
        <button type="submit" className="btn btn-ghost" disabled={disabled || !trimmed || !!keyError}>
          Add
        </button>
      </form>
      {keyError && <div className="small kv-error">{keyError}</div>}
    </div>
  );
}
