// Client API of the controller (docs/ClientApi.png, README "Client API").
import { ApiError, errorFromBody, errorFromResponse, getJSON } from "./http";
import type { CustomMetadata, MetadataPatch, ObjectInfo, ObjectMetadata } from "./types";

const OBJECTS = "/api/v1/objects";

export function listObjects(signal?: AbortSignal): Promise<ObjectInfo[]> {
  return getJSON<ObjectInfo[]>(OBJECTS, signal);
}

export async function deleteObject(id: string): Promise<void> {
  const res = await fetch(`${OBJECTS}/${id}`, { method: "DELETE" });
  if (res.status !== 204) throw await errorFromResponse(res);
}

export interface Progress {
  loaded: number;
  total: number; // 0 when unknown
}

export interface UploadHandle {
  promise: Promise<ObjectInfo>;
  abort: () => void;
}

/**
 * Uploads a file as a new object (POST /api/v1/objects, multipart/form-data
 * with a "file" field and an optional "metadata" JSON field). The browser
 * sends the file's name and type, which become the object's name and content
 * type. Uses XMLHttpRequest because fetch() can't report upload progress;
 * progress is the browser's count of request bytes sent.
 */
export function uploadObject(file: File, custom: CustomMetadata, onProgress: (p: Progress) => void): UploadHandle {
  const xhr = new XMLHttpRequest();
  const form = new FormData();
  // Metadata first, so the controller can reject bad metadata before the data is stored.
  if (Object.keys(custom).length > 0) form.append("metadata", JSON.stringify(custom));
  form.append("file", file, file.name);
  const promise = new Promise<ObjectInfo>((resolve, reject) => {
    xhr.open("POST", OBJECTS);
    xhr.responseType = "text";
    xhr.upload.onprogress = (e) => onProgress({ loaded: e.loaded, total: e.lengthComputable ? e.total : file.size });
    xhr.onload = () => {
      if (xhr.status === 201) {
        try {
          resolve(JSON.parse(xhr.responseText) as ObjectInfo);
        } catch {
          reject(new ApiError(xhr.status, "bad_response", "The controller sent an unreadable response."));
        }
      } else {
        reject(errorFromBody(xhr.status, xhr.responseText));
      }
    };
    xhr.onerror = () => reject(new ApiError(0, "network_error", "Connection to the controller failed during upload."));
    xhr.onabort = () => reject(new DOMException("Upload cancelled", "AbortError"));
    xhr.send(form);
  });
  return { promise, abort: () => xhr.abort() };
}

/**
 * Downloads an object, reporting bytes received against Content-Length.
 * If the controller aborts mid-stream (a chunk failed after the 200 was
 * sent) the body ends short, which is reported as an error rather than
 * saving a truncated file.
 */
export async function downloadObject(
  id: string,
  onProgress: (p: Progress) => void,
  signal: AbortSignal,
): Promise<Blob> {
  const res = await fetch(`${OBJECTS}/${id}`, { signal });
  if (!res.ok) throw await errorFromResponse(res);
  const total = Number(res.headers.get("Content-Length") ?? 0);
  if (!res.body) {
    const blob = await res.blob();
    onProgress({ loaded: blob.size, total: blob.size });
    return blob;
  }
  const reader = res.body.getReader();
  const parts: Uint8Array<ArrayBuffer>[] = [];
  let loaded = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts.push(value as Uint8Array<ArrayBuffer>);
      loaded += value.byteLength;
      onProgress({ loaded, total });
    }
  } catch (e) {
    if (signal.aborted) throw new DOMException("Download cancelled", "AbortError");
    throw new ApiError(0, "download_interrupted", `Download interrupted after ${loaded} bytes.`);
  }
  if (total > 0 && loaded !== total) {
    throw new ApiError(0, "download_interrupted", `Download ended early: got ${loaded} of ${total} bytes.`);
  }
  return new Blob(parts, { type: "application/octet-stream" });
}

/** Saves a blob through a temporary object URL. */
export function saveBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

// --- Metadata (GET/PATCH /api/v1/objects/{id}/metadata) ---

/** System metadata, custom metadata and chunk locations, without the data. */
export function getMetadata(id: string, signal?: AbortSignal): Promise<ObjectMetadata> {
  return getJSON<ObjectMetadata>(`${OBJECTS}/${id}/metadata`, signal);
}

/** Changes name, content type or custom metadata; the data is not re-sent. */
export async function patchMetadata(id: string, patch: MetadataPatch): Promise<ObjectMetadata> {
  const res = await fetch(`${OBJECTS}/${id}/metadata`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  if (!res.ok) throw await errorFromResponse(res);
  return (await res.json()) as ObjectMetadata;
}
