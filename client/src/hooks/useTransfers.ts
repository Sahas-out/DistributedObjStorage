import { useCallback, useRef, useState } from "react";
import { describeError } from "../api/http";
import { downloadObject, saveBlob, uploadObject, type Progress } from "../api/objects";
import type { CustomMetadata, ObjectInfo } from "../api/types";

export type TransferKind = "upload" | "download";
export type TransferState = "running" | "done" | "failed" | "cancelled";

export interface Transfer {
  id: number;
  kind: TransferKind;
  name: string;
  loaded: number;
  total: number;
  /** Smoothed bytes/second, measured in the browser. */
  rate: number;
  state: TransferState;
  error?: string;
  objectId?: string;
  startedAt: number;
  finishedAt?: number;
}

let nextId = 1;

/** Tracks uploads and downloads with client-side progress and speed. */
export function useTransfers(onUploaded: (info: ObjectInfo) => void) {
  const [transfers, setTransfers] = useState<Transfer[]>([]);
  const aborts = useRef(new Map<number, () => void>());
  const samples = useRef(new Map<number, { t: number; loaded: number }>());

  const patch = useCallback((id: number, p: Partial<Transfer>) => {
    setTransfers((ts) => ts.map((t) => (t.id === id ? { ...t, ...p } : t)));
  }, []);

  const progress = useCallback(
    (id: number) => (p: Progress) => {
      const now = performance.now();
      const prev = samples.current.get(id);
      let rateUpdate: Partial<Transfer> = {};
      if (!prev) {
        samples.current.set(id, { t: now, loaded: p.loaded });
      } else if (now - prev.t >= 250) {
        const inst = ((p.loaded - prev.loaded) * 1000) / (now - prev.t);
        samples.current.set(id, { t: now, loaded: p.loaded });
        rateUpdate = { rate: inst };
      }
      setTransfers((ts) =>
        ts.map((t) =>
          t.id === id
            ? {
                ...t,
                loaded: p.loaded,
                total: p.total || t.total,
                rate: rateUpdate.rate === undefined ? t.rate : t.rate === 0 ? rateUpdate.rate : t.rate * 0.6 + rateUpdate.rate * 0.4,
              }
            : t,
        ),
      );
    },
    [],
  );

  const finish = useCallback(
    (id: number, p: Partial<Transfer>) => {
      aborts.current.delete(id);
      samples.current.delete(id);
      patch(id, { ...p, finishedAt: Date.now() });
    },
    [patch],
  );

  const startUpload = useCallback(
    (file: File, custom: CustomMetadata) => {
      const id = nextId++;
      const handle = uploadObject(file, custom, progress(id));
      aborts.current.set(id, handle.abort);
      setTransfers((ts) => [
        { id, kind: "upload", name: file.name, loaded: 0, total: file.size, rate: 0, state: "running", startedAt: Date.now() },
        ...ts,
      ]);
      handle.promise.then(
        (info) => {
          finish(id, { state: "done", loaded: file.size, objectId: info.object_id });
          onUploaded(info);
        },
        (err) =>
          finish(id, {
            state: err instanceof DOMException && err.name === "AbortError" ? "cancelled" : "failed",
            error: describeError(err),
          }),
      );
    },
    [finish, onUploaded, progress],
  );

  const startDownload = useCallback(
    (obj: ObjectInfo) => {
      const id = nextId++;
      const ctrl = new AbortController();
      aborts.current.set(id, () => ctrl.abort());
      const name = obj.name || `${obj.object_id}.bin`;
      setTransfers((ts) => [
        { id, kind: "download", name, loaded: 0, total: obj.size, rate: 0, state: "running", objectId: obj.object_id, startedAt: Date.now() },
        ...ts,
      ]);
      downloadObject(obj.object_id, progress(id), ctrl.signal).then(
        (blob) => {
          finish(id, { state: "done", loaded: blob.size });
          saveBlob(blob, name);
        },
        (err) =>
          finish(id, {
            state: ctrl.signal.aborted ? "cancelled" : "failed",
            error: describeError(err),
          }),
      );
    },
    [finish, progress],
  );

  const cancel = useCallback((id: number) => aborts.current.get(id)?.(), []);
  const clearFinished = useCallback(() => setTransfers((ts) => ts.filter((t) => t.state === "running")), []);

  return { transfers, startUpload, startDownload, cancel, clearFinished };
}
