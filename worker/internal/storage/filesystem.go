package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

// On-disk layout inside the data directory (the worker's Docker volume).
const (
	dbFileName = "metadata.db" // SQLite database holding the chunks table
	chunksDir  = "chunks"      // committed chunk files
	tmpDir     = "tmp"         // in-progress writes; emptied on startup
)

const schema = `
CREATE TABLE IF NOT EXISTS chunks (
	chunk_id    TEXT    PRIMARY KEY,
	object_id   TEXT    NOT NULL,
	sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
	file_path   TEXT    NOT NULL UNIQUE,
	size        INTEGER NOT NULL CHECK (size >= 0),
	created_at  INTEGER NOT NULL -- Unix time in nanoseconds, UTC
);
CREATE INDEX IF NOT EXISTS chunks_by_object ON chunks (object_id, sequence_no);
CREATE TABLE IF NOT EXISTS worker_identity (
	id        INTEGER PRIMARY KEY CHECK (id = 1),
	worker_id TEXT    NOT NULL
);
`

// schemaVersion is stored in PRAGMA user_version. Version 1 (phase 1-2)
// stored a checksum per chunk; version 2 doesn't.
const schemaVersion = 2

// Filesystem is the worker's persistent Storage. Chunk data lives in files
// under <dataDir>/chunks and the ChunkMetadata table lives in SQLite at
// <dataDir>/metadata.db.
//
// Put follows the upload sequence in docs/upload_sequence.mmd: the data is
// written to a temporary file and fsynced, renamed into chunks/, and only
// then is the metadata row committed. Each write goes to a new file name, so
// overwriting a chunk never touches the file the current row points to; the
// old file is removed after the new row commits.
//
// A crash can therefore leave only files that no row references, or rows
// whose file has gone missing (lost or damaged volume). NewFilesystem cleans
// up both before the worker serves requests, so List reports only chunks
// the worker can actually serve, which is what the controller expects in the
// startup chunk report (docs/system_lifetime_sequence.mmd).
type Filesystem struct {
	dataDir string
	db      *sql.DB
	log     *slog.Logger

	// mu orders metadata changes against Open. Open holds it for reading
	// while it looks up a row and opens the file; Put and Delete hold it for
	// writing while they change a row and remove the file it replaced. Once a
	// file is open it stays readable even after it is unlinked, so a reader
	// never loses its chunk to a concurrent overwrite or delete.
	mu sync.RWMutex
}

var _ Storage = (*Filesystem)(nil)

// NewFilesystem opens (creating if needed) the store in dataDir, then
// reconciles chunk files with the metadata table. A nil logger discards logs.
// The caller must Close the store.
func NewFilesystem(ctx context.Context, dataDir string, log *slog.Logger) (*Filesystem, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data dir: %w", err)
	}
	for _, d := range []string{chunksDir, tmpDir} {
		if err := os.MkdirAll(filepath.Join(abs, d), 0o750); err != nil {
			return nil, fmt.Errorf("create %s dir: %w", d, err)
		}
	}

	db, err := sql.Open("sqlite", dsn(filepath.Join(abs, dbFileName)))
	if err != nil {
		return nil, fmt.Errorf("open metadata db: %w", err)
	}
	f := &Filesystem{dataDir: abs, db: db, log: log}
	if err := f.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := f.reconcile(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("reconcile chunk files: %w", err)
	}
	return f, nil
}

// dsn builds the SQLite connection string. WAL lets reads run alongside a
// write, synchronous=FULL makes a commit durable before Put returns, and
// _txlock=immediate takes the write lock when a transaction begins so two
// writers wait on busy_timeout instead of failing mid-transaction.
func dsn(dbPath string) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Set("_txlock", "immediate")
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(dbPath), RawQuery: q.Encode()}
	return u.String()
}

// migrate creates the schema. A database written by an older version is
// rejected rather than silently mixed with the new layout.
func (f *Filesystem) migrate(ctx context.Context) error {
	var version int
	if err := f.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version != 0 && version != schemaVersion {
		return fmt.Errorf("metadata db in %s has schema version %d, want %d: remove the worker volume to start fresh", f.dataDir, version, schemaVersion)
	}
	if version == 0 {
		var tables int
		if err := f.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'chunks'`).Scan(&tables); err != nil {
			return fmt.Errorf("inspect metadata db: %w", err)
		}
		if tables > 0 {
			return fmt.Errorf("metadata db in %s was written by an older worker version: remove the worker volume to start fresh", f.dataDir)
		}
	}
	if _, err := f.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create metadata schema: %w", err)
	}
	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
		return fmt.Errorf("set schema version: %w", err)
	}
	return nil
}

// Close closes the metadata database.
func (f *Filesystem) Close() error {
	return f.db.Close()
}

// Ping reports whether the metadata database is usable.
func (f *Filesystem) Ping(ctx context.Context) error {
	return f.db.PingContext(ctx)
}

// LoadWorkerID returns the worker ID the controller assigned on an earlier
// registration, or "" if the worker has never registered.
func (f *Filesystem) LoadWorkerID(ctx context.Context) (string, error) {
	var id string
	err := f.db.QueryRowContext(ctx, `SELECT worker_id FROM worker_identity WHERE id = 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load worker id: %w", err)
	}
	return id, nil
}

// SaveWorkerID durably records the worker ID assigned by the controller.
func (f *Filesystem) SaveWorkerID(ctx context.Context, id string) error {
	_, err := f.db.ExecContext(ctx, `
		INSERT INTO worker_identity (id, worker_id) VALUES (1, ?)
		ON CONFLICT (id) DO UPDATE SET worker_id = excluded.worker_id`, id)
	if err != nil {
		return fmt.Errorf("save worker id: %w", err)
	}
	return nil
}

// Put writes the chunk data durably, then registers its metadata row.
func (f *Filesystem) Put(ctx context.Context, meta ChunkMetadata, src io.Reader) (ChunkMetadata, error) {
	if !ValidChunkID(meta.ChunkID) {
		return ChunkMetadata{}, ErrInvalidChunkID
	}

	rel, size, err := f.writeChunkFile(meta.ChunkID, src)
	if err != nil {
		return ChunkMetadata{}, err
	}
	newPath := f.abs(rel)
	if err := ctx.Err(); err != nil {
		f.removeFile(newPath)
		return ChunkMetadata{}, err
	}

	meta.FilePath = rel
	meta.Size = size
	meta.CreatedAt = time.Now().UTC()

	f.mu.Lock()
	defer f.mu.Unlock()
	oldRel, err := f.upsert(ctx, meta)
	if err != nil {
		f.removeFile(newPath)
		return ChunkMetadata{}, fmt.Errorf("record chunk metadata: %w", err)
	}
	if oldRel != "" && oldRel != rel {
		f.removeFile(f.abs(oldRel))
	}
	return meta, nil
}

// writeChunkFile streams src into a temporary file, fsyncs it and renames it
// into chunks/ under a fresh name. It returns the path relative to the data
// directory and the size.
func (f *Filesystem) writeChunkFile(chunkID string, src io.Reader) (rel string, size int64, err error) {
	tmp, err := os.CreateTemp(filepath.Join(f.dataDir, tmpDir), chunkID+".*.part")
	if err != nil {
		return "", 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			f.removeFile(tmpName)
		}
	}()

	size, err = io.Copy(tmp, src)
	if err != nil {
		return "", 0, fmt.Errorf("write chunk data: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		return "", 0, fmt.Errorf("fsync chunk data: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return "", 0, fmt.Errorf("close chunk file: %w", err)
	}

	suffix, err := randomHex(8)
	if err != nil {
		return "", 0, err
	}
	// Chunk IDs never contain '.', so "<id>.<suffix>" can't collide across IDs.
	rel = path.Join(chunksDir, chunkID+"."+suffix)
	if err = os.Rename(tmpName, f.abs(rel)); err != nil {
		return "", 0, fmt.Errorf("move chunk into place: %w", err)
	}
	if err = syncDir(filepath.Join(f.dataDir, chunksDir)); err != nil {
		f.removeFile(f.abs(rel))
		return "", 0, fmt.Errorf("fsync chunks dir: %w", err)
	}
	return rel, size, nil
}

// upsert inserts or replaces the row for meta.ChunkID and returns the
// file_path of the row it replaced, or "" if there was none.
func (f *Filesystem) upsert(ctx context.Context, meta ChunkMetadata) (oldRel string, err error) {
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	err = tx.QueryRowContext(ctx, `SELECT file_path FROM chunks WHERE chunk_id = ?`, meta.ChunkID).Scan(&oldRel)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO chunks (chunk_id, object_id, sequence_no, file_path, size, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (chunk_id) DO UPDATE SET
			object_id   = excluded.object_id,
			sequence_no = excluded.sequence_no,
			file_path   = excluded.file_path,
			size        = excluded.size,
			created_at  = excluded.created_at`,
		meta.ChunkID, meta.ObjectID, meta.SequenceNo, meta.FilePath,
		meta.Size, meta.CreatedAt.UnixNano())
	if err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return oldRel, nil
}

// Open returns the chunk file and its metadata.
func (f *Filesystem) Open(ctx context.Context, chunkID string) (io.ReadCloser, ChunkMetadata, error) {
	if !ValidChunkID(chunkID) {
		return nil, ChunkMetadata{}, ErrInvalidChunkID
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	meta, err := scanMetadata(f.db.QueryRowContext(ctx, selectMetadata+` WHERE chunk_id = ?`, chunkID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ChunkMetadata{}, ErrNotFound
	}
	if err != nil {
		return nil, ChunkMetadata{}, fmt.Errorf("read chunk metadata: %w", err)
	}
	file, err := os.Open(f.abs(meta.FilePath))
	if err != nil {
		// The row exists but its data is gone: report a storage failure, not
		// "not found", so the controller treats this replica as bad.
		return nil, ChunkMetadata{}, fmt.Errorf("open chunk %s data: %w", chunkID, err)
	}
	return file, meta, nil
}

// Delete removes the chunk's metadata row, then its file.
func (f *Filesystem) Delete(ctx context.Context, chunkID string) error {
	if !ValidChunkID(chunkID) {
		return ErrInvalidChunkID
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var rel string
	err := f.db.QueryRowContext(ctx,
		`DELETE FROM chunks WHERE chunk_id = ? RETURNING file_path`, chunkID).Scan(&rel)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("delete chunk metadata: %w", err)
	}
	f.removeFile(f.abs(rel))
	return nil
}

// List returns the metadata of all stored chunks, sorted by chunk ID.
func (f *Filesystem) List(ctx context.Context) ([]ChunkMetadata, error) {
	rows, err := f.db.QueryContext(ctx, selectMetadata+` ORDER BY chunk_id`)
	if err != nil {
		return nil, fmt.Errorf("list chunk metadata: %w", err)
	}
	defer rows.Close()

	out := []ChunkMetadata{}
	for rows.Next() {
		meta, err := scanMetadata(rows)
		if err != nil {
			return nil, fmt.Errorf("list chunk metadata: %w", err)
		}
		out = append(out, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list chunk metadata: %w", err)
	}
	return out, nil
}

const selectMetadata = `SELECT chunk_id, object_id, sequence_no, file_path, size, created_at FROM chunks`

type scanner interface{ Scan(dest ...any) error }

func scanMetadata(s scanner) (ChunkMetadata, error) {
	var m ChunkMetadata
	var created int64
	if err := s.Scan(&m.ChunkID, &m.ObjectID, &m.SequenceNo, &m.FilePath, &m.Size, &created); err != nil {
		return ChunkMetadata{}, err
	}
	m.CreatedAt = time.Unix(0, created).UTC()
	return m, nil
}

// reconcile runs once at startup, before any request is served. It empties
// tmp/, drops rows whose chunk file is missing, and deletes chunk files that
// no row references (left by a crash between the rename and the commit, or
// between the commit and removing a replaced file).
func (f *Filesystem) reconcile(ctx context.Context) error {
	tmpEntries, err := os.ReadDir(filepath.Join(f.dataDir, tmpDir))
	if err != nil {
		return err
	}
	for _, e := range tmpEntries {
		f.removeFile(filepath.Join(f.dataDir, tmpDir, e.Name()))
	}

	metas, err := f.List(ctx)
	if err != nil {
		return err
	}
	referenced := make(map[string]bool, len(metas))
	for _, m := range metas {
		if _, err := os.Stat(f.abs(m.FilePath)); errors.Is(err, os.ErrNotExist) {
			f.log.Warn("chunk data file missing; dropping its metadata",
				"chunk_id", m.ChunkID, "file_path", m.FilePath)
			if _, err := f.db.ExecContext(ctx, `DELETE FROM chunks WHERE chunk_id = ?`, m.ChunkID); err != nil {
				return err
			}
			continue
		} else if err != nil {
			return err
		}
		referenced[m.FilePath] = true
	}

	entries, err := os.ReadDir(filepath.Join(f.dataDir, chunksDir))
	if err != nil {
		return err
	}
	orphans := 0
	for _, e := range entries {
		rel := path.Join(chunksDir, e.Name())
		if !referenced[rel] {
			f.removeFile(f.abs(rel))
			orphans++
		}
	}
	f.log.Info("storage ready",
		"data_dir", f.dataDir, "chunks", len(referenced),
		"orphan_files_removed", orphans, "temp_files_removed", len(tmpEntries))
	return nil
}

// abs converts a path relative to the data directory (as stored in
// file_path) into an absolute filesystem path.
func (f *Filesystem) abs(rel string) string {
	return filepath.Join(f.dataDir, filepath.FromSlash(rel))
}

// removeFile deletes a file, logging failures other than "already gone".
// A file left behind is only wasted space: reconcile removes it on restart.
func (f *Filesystem) removeFile(p string) {
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		f.log.Warn("failed to remove file", "path", p, "error", err)
	}
}

// syncDir fsyncs a directory so a rename into it survives a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate file name: %w", err)
	}
	return hex.EncodeToString(b), nil
}
