package protocol

import "time"

// Types for the read-only /dashboard endpoints. Workers serve WorkerStats on
// GET /dashboard/stats; only the controller calls it, from inside the
// cluster network. The controller serves DashboardOverview and
// DashboardEvent to the web GUI.

// RequestCounts counts requests a worker has served on its chunk API.
type RequestCounts struct {
	Put    int64 `json:"put"`
	Get    int64 `json:"get"`
	Delete int64 `json:"delete"`
	List   int64 `json:"list"`
	// Errors counts responses with status 400 or higher, of any kind.
	Errors int64 `json:"errors"`
}

// WorkerStats is the body of a worker's GET /dashboard/stats. The request
// counters are cumulative since StartedAt; the GUI turns them into
// requests/second. They cover the chunk API only, not /healthz or /dashboard.
type WorkerStats struct {
	WorkerID      string        `json:"worker_id"`
	StartedAt     time.Time     `json:"started_at"`
	UptimeSeconds int64         `json:"uptime_seconds"`
	ChunkCount    int           `json:"chunk_count"`
	StoredBytes   int64         `json:"stored_bytes"`
	Requests      RequestCounts `json:"requests"`
}

// ControllerSummary describes the controller in a DashboardOverview.
type ControllerSummary struct {
	StartedAt               time.Time `json:"started_at"`
	ReplicationFactor       int       `json:"replication_factor"`
	AnnounceIntervalSeconds int       `json:"announce_interval_seconds"`
	ObjectCount             int       `json:"object_count"`
	LogicalBytes            int64     `json:"logical_bytes"`
	// Recovery is the result of the recovery job's latest scan.
	Recovery RecoveryStatus `json:"recovery"`
}

// RecoveryStatus summarises the recovery job's latest scan of every chunk.
type RecoveryStatus struct {
	LastScan time.Time `json:"last_scan"` // zero before the first scan
	// DegradedChunks have at least one readable copy but fewer than the
	// replication factor (recovery is restoring them).
	DegradedChunks int `json:"degraded_chunks"`
	// UnavailableChunks have no readable copy right now; a copy is on a dead
	// worker that may come back.
	UnavailableChunks int `json:"unavailable_chunks"`
	// OrphanReplicas are copies that belong to no object, waiting to be
	// deleted.
	OrphanReplicas int `json:"orphan_replicas"`
}

// DashboardWorker is one worker in a DashboardOverview: the controller's view
// of it plus the stats the worker reported. Stats is nil and StatsError set
// when the worker could not be asked.
type DashboardWorker struct {
	WorkerInfo
	Stats      *WorkerStats `json:"stats"`
	StatsError string       `json:"stats_error,omitempty"`
}

// DashboardOverview is the body of the controller's GET /dashboard/overview.
// ServerTime lets the GUI compute heartbeat ages without clock skew.
type DashboardOverview struct {
	ServerTime time.Time         `json:"server_time"`
	Controller ControllerSummary `json:"controller"`
	Workers    []DashboardWorker `json:"workers"`
}

// DashboardEvent is one entry of the controller's activity feed
// (GET /dashboard/events). Seq increases by one per event.
type DashboardEvent struct {
	Seq      uint64    `json:"seq"`
	Time     time.Time `json:"time"`
	Type     string    `json:"type"`
	Level    string    `json:"level"` // info, warn or error
	Message  string    `json:"message"`
	ObjectID string    `json:"object_id,omitempty"`
	WorkerID string    `json:"worker_id,omitempty"`
}

// Event types in the activity feed.
const (
	EventObjectUploaded     = "object_uploaded"
	EventUploadFailed       = "upload_failed"
	EventObjectDeleted      = "object_deleted"
	EventMetadataUpdated    = "metadata_updated"
	EventReplicaCorrupted   = "replica_corrupted"
	EventReplicaMissing     = "replica_missing"
	EventReplicaOrphaned    = "replica_orphaned"
	EventWorkerRegistered   = "worker_registered"
	EventWorkerMoved        = "worker_moved"
	EventWorkerReplaced     = "worker_replaced"
	EventWorkerReady        = "worker_ready"
	EventWorkerSyncFailed   = "worker_sync_failed"
	EventWorkerDeregistered = "worker_deregistered"

	// Failure detection and recovery (phase 5).
	EventWorkerDead       = "worker_dead"
	EventWorkerRevived    = "worker_revived"
	EventReplicaRestored  = "replica_restored"  // a new copy replaced a lost one
	EventReplicaRepaired  = "replica_repaired"  // a corrupted or missing copy was rewritten in place
	EventReplicaTrimmed   = "replica_trimmed"   // an extra copy beyond the replication factor was removed
	EventOrphanRemoved    = "orphan_removed"    // an orphan copy was deleted from its worker
	EventChunkUnavailable = "chunk_unavailable" // no readable copy; waiting for a dead worker to return
	EventObjectLost       = "object_lost"       // every copy of a chunk was corrupted; object deleted
)
