import type { ErrorResponse } from "./types";

/** An error returned by the controller (JSON error body) or the network. */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

// Plain-language versions of the controller's error codes (README "Client API").
const friendly: Record<string, string> = {
  insufficient_workers: "Fewer than 2 workers are READY, so the object can't be stored with 2 copies.",
  worker_unavailable: "A worker failed while storing the upload. Nothing was stored.",
  upload_incomplete: "The upload was interrupted before it finished. Nothing was stored.",
  object_not_found: "This object no longer exists.",
  object_unavailable: "Some chunk has no reachable copy right now. Try again shortly.",
  object_corrupted: "Every copy of some chunk is missing or failed its checksum.",
  invalid_object_id: "Invalid object ID.",
  internal_error: "The controller hit an internal error (see its logs).",
};

export function describeError(err: unknown): string {
  if (err instanceof ApiError) {
    return friendly[err.code] ?? err.message ?? `HTTP ${err.status}`;
  }
  if (err instanceof DOMException && err.name === "AbortError") return "Cancelled";
  if (err instanceof Error) return err.message;
  return String(err);
}

/** Builds an ApiError from a status code and a (possibly JSON) body. */
export function errorFromBody(status: number, body: string): ApiError {
  try {
    const parsed = JSON.parse(body) as ErrorResponse;
    if (parsed?.error?.code) {
      return new ApiError(status, parsed.error.code, parsed.error.message);
    }
  } catch {
    /* not JSON */
  }
  if (status === 0) return new ApiError(0, "network_error", "Could not reach the controller.");
  return new ApiError(status, `http_${status}`, body || `HTTP ${status}`);
}

export async function errorFromResponse(res: Response): Promise<ApiError> {
  return errorFromBody(res.status, await res.text().catch(() => ""));
}

export async function getJSON<T>(url: string, signal?: AbortSignal): Promise<T> {
  let res: Response;
  try {
    res = await fetch(url, { signal, headers: { Accept: "application/json" } });
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") throw e;
    throw new ApiError(0, "network_error", "Could not reach the controller.");
  }
  if (!res.ok) throw await errorFromResponse(res);
  return (await res.json()) as T;
}
