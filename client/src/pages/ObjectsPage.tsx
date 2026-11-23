import { useCallback, useState } from "react";
import { describeError } from "../api/http";
import { deleteObject, listObjects } from "../api/objects";
import type { ObjectInfo } from "../api/types";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { MetadataDrawer } from "../components/objects/MetadataDrawer";
import { ObjectTable } from "../components/objects/ObjectTable";
import { TransferList } from "../components/objects/TransferList";
import { UploadPanel } from "../components/objects/UploadPanel";
import { usePolling } from "../hooks/usePolling";
import { useTransfers } from "../hooks/useTransfers";
import { formatBytes, shortId } from "../lib/format";

export function ObjectsPage() {
  const objects = usePolling(listObjects, 5000);
  const [busy, setBusy] = useState<Set<string>>(new Set());
  const [details, setDetails] = useState<ObjectInfo | null>(null);
  const [confirm, setConfirm] = useState<ObjectInfo | null>(null);
  const [toast, setToast] = useState<{ kind: "ok" | "err"; text: string } | null>(null);

  const refresh = objects.refresh;
  const onUploaded = useCallback(() => refresh(), [refresh]);
  const { transfers, startUpload, startDownload, cancel, clearFinished } = useTransfers(onUploaded);

  const flash = (kind: "ok" | "err", text: string) => {
    setToast({ kind, text });
    setTimeout(() => setToast(null), 4000);
  };

  const doDelete = async (o: ObjectInfo) => {
    setConfirm(null);
    setBusy((b) => new Set(b).add(o.object_id));
    try {
      await deleteObject(o.object_id);
      flash("ok", `Deleted ${shortId(o.object_id)}`);
      if (details?.object_id === o.object_id) setDetails(null);
    } catch (e) {
      flash("err", `Delete failed: ${describeError(e)}`);
    } finally {
      setBusy((b) => {
        const n = new Set(b);
        n.delete(o.object_id);
        return n;
      });
      refresh();
    }
  };

  const list = objects.data ?? [];
  const total = list.reduce((s, o) => s + o.size, 0);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Objects</h1>
          <p className="muted">
            {list.length} objects · {formatBytes(total)} stored (×2 replicas on workers)
          </p>
        </div>
        <button type="button" className="btn btn-ghost" onClick={refresh}>
          ↻ Refresh
        </button>
      </div>

      <UploadPanel onFiles={(files, custom) => files.forEach((f) => startUpload(f, custom))} />

      {objects.error !== undefined && (
        <div className="alert">Could not load objects: {describeError(objects.error)}</div>
      )}

      <section className="card">
        {objects.loading && !objects.data ? (
          <div className="empty">Loading…</div>
        ) : (
          <ObjectTable
            objects={list}
            busy={busy}
            onDownload={startDownload}
            onDelete={setConfirm}
            onDetails={setDetails}
          />
        )}
      </section>

      {details && (
        <MetadataDrawer object={details} onClose={() => setDetails(null)} onDownload={startDownload} onChanged={refresh} />
      )}
      {confirm && (
        <ConfirmDialog
          title="Delete object?"
          body={
            <p>
              <strong>{confirm.name}</strong> (<code>{confirm.object_id}</code>, {formatBytes(confirm.size)}, {confirm.chunk_count} chunks) and all of its
              copies on the workers will be removed. This can't be undone.
            </p>
          }
          confirmLabel="Delete"
          onCancel={() => setConfirm(null)}
          onConfirm={() => doDelete(confirm)}
        />
      )}
      {toast && <div className={`toast toast-${toast.kind}`}>{toast.text}</div>}
      <TransferList transfers={transfers} onCancel={cancel} onClear={clearFinished} />
    </div>
  );
}
