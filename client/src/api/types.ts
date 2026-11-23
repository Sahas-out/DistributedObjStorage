// Mirrors the JSON in pkg/protocol (object.go, cluster.go, dashboard.go).

export interface ObjectInfo {
  object_id: string;
  name: string;
  content_type: string;
  size: number;
  checksum: string; // SHA-256 hex of the whole object
  chunk_count: number;
  created_at: string;
  last_modified_at: string; // last change of name, content type or custom metadata
}

/** Where one copy of a chunk is stored. */
export interface ChunkLocation {
  chunk_id: string;
  worker_id: string;
  worker_addr: string;
  status: string; // ACTIVE, CORRUPTED, LOST, ...
}

export interface ChunkInfo {
  sequence_no: number;
  size: number;
  checksum: string;
  replicas: ChunkLocation[];
}

/** GET/PATCH /api/v1/objects/{id}/metadata: system + custom metadata. */
export interface ObjectMetadata extends ObjectInfo {
  custom_metadata: CustomMetadata;
  chunks: ChunkInfo[];
}

/** PATCH body: omitted fields are unchanged; a null custom value removes the key. */
export interface MetadataPatch {
  name?: string;
  content_type?: string;
  custom_metadata?: Record<string, string | null>;
}

export interface ErrorResponse {
  error: { code: string; message: string };
}

export interface RequestCounts {
  put: number;
  get: number;
  delete: number;
  list: number;
  errors: number;
}

export interface WorkerStats {
  worker_id: string;
  started_at: string;
  uptime_seconds: number;
  chunk_count: number;
  stored_bytes: number;
  requests: RequestCounts;
}

export type WorkerStatus = "AVAILABLE" | "READY" | "DEAD";

export interface DashboardWorker {
  worker_id: string;
  addr: string;
  status: WorkerStatus;
  chunk_count: number;
  last_seen: string;
  stats: WorkerStats | null;
  stats_error?: string;
}

export interface ControllerSummary {
  started_at: string;
  replication_factor: number;
  announce_interval_seconds: number;
  object_count: number;
  logical_bytes: number;
  recovery: RecoveryStatus;
}

/** Result of the controller's latest recovery scan. */
export interface RecoveryStatus {
  last_scan: string; // zero time before the first scan
  degraded_chunks: number; // fewer good copies than the replication factor
  unavailable_chunks: number; // no readable copy; waiting for a dead worker
  orphan_replicas: number;
}

export interface DashboardOverview {
  server_time: string;
  controller: ControllerSummary;
  workers: DashboardWorker[];
}

export type EventLevel = "info" | "warn" | "error";

export interface DashboardEvent {
  seq: number;
  time: string;
  type: string;
  level: EventLevel;
  message: string;
  object_id?: string;
  worker_id?: string;
}

export interface EventsResponse {
  events: DashboardEvent[];
  last_seq: number;
}

/** Custom, user-defined key/value metadata. */
export type CustomMetadata = Record<string, string>;
