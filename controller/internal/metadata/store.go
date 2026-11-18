// Package metadata persists object metadata and each object's ordered chunk
// list in SQLite.
package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

const dbFileName = "metadata.db"

const schema = `
CREATE TABLE IF NOT EXISTS objects (
	object_id   TEXT    PRIMARY KEY,
	size        INTEGER NOT NULL CHECK (size >= 0),
	checksum    TEXT    NOT NULL,
	chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0),
	created_at  INTEGER NOT NULL -- Unix time in nanoseconds, UTC
);
CREATE TABLE IF NOT EXISTS object_chunks (
	object_id   TEXT    NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
	sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
	chunk_id    TEXT    NOT NULL UNIQUE,
	size        INTEGER NOT NULL CHECK (size > 0),
	checksum    TEXT    NOT NULL,
	PRIMARY KEY (object_id, sequence_no)
);
`

// ErrNotFound is returned when an object has no metadata.
var ErrNotFound = errors.New("object not found")

// Chunk records where one piece of an object sits in the sequence and how to
// verify it.
type Chunk struct {
	SequenceNo int
	ChunkID    string
	Size       int64
	Checksum   string // SHA-256 hex digest
}

// Object is the persisted description of a stored object.
type Object struct {
	ObjectID  string
	Size      int64
	Checksum  string // SHA-256 hex digest of the whole object
	CreatedAt time.Time
	Chunks    []Chunk // ordered by SequenceNo
}

// Store is the SQLite-backed metadata repository.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the metadata database inside dataDir.
// The caller must Close the store.
func Open(ctx context.Context, dataDir string) (*Store, error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data dir: %w", err)
	}
	if err := os.MkdirAll(abs, 0o750); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	db, err := sql.Open("sqlite", dsn(filepath.Join(abs, dbFileName)))
	if err != nil {
		return nil, fmt.Errorf("open metadata db: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create metadata schema: %w", err)
	}
	return &Store{db: db}, nil
}

// dsn builds the SQLite connection string. synchronous=FULL makes a commit
// durable before Create returns, so an acknowledged upload survives a crash.
func dsn(dbPath string) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Set("_txlock", "immediate")
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(dbPath), RawQuery: q.Encode()}
	return u.String()
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Ping reports whether the database is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Create commits an object and all of its chunks in one transaction, so a
// reader can never observe an object with a partial chunk list.
func (s *Store) Create(ctx context.Context, obj Object) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	_, err = tx.ExecContext(ctx,
		`INSERT INTO objects (object_id, size, checksum, chunk_count, created_at) VALUES (?, ?, ?, ?, ?)`,
		obj.ObjectID, obj.Size, obj.Checksum, len(obj.Chunks), obj.CreatedAt.UnixNano())
	if err != nil {
		return fmt.Errorf("insert object: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO object_chunks (object_id, sequence_no, chunk_id, size, checksum) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare chunk insert: %w", err)
	}
	defer stmt.Close()
	for _, c := range obj.Chunks {
		if _, err := stmt.ExecContext(ctx, obj.ObjectID, c.SequenceNo, c.ChunkID, c.Size, c.Checksum); err != nil {
			return fmt.Errorf("insert chunk %d: %w", c.SequenceNo, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit object: %w", err)
	}
	return nil
}

// Get returns an object and its chunks in sequence order, or ErrNotFound.
func (s *Store) Get(ctx context.Context, objectID string) (Object, error) {
	obj := Object{ObjectID: objectID}
	var chunkCount int
	var created int64
	err := s.db.QueryRowContext(ctx,
		`SELECT size, checksum, chunk_count, created_at FROM objects WHERE object_id = ?`, objectID).
		Scan(&obj.Size, &obj.Checksum, &chunkCount, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("read object: %w", err)
	}
	obj.CreatedAt = time.Unix(0, created).UTC()

	rows, err := s.db.QueryContext(ctx,
		`SELECT sequence_no, chunk_id, size, checksum FROM object_chunks WHERE object_id = ? ORDER BY sequence_no`, objectID)
	if err != nil {
		return Object{}, fmt.Errorf("read chunks: %w", err)
	}
	defer rows.Close()

	obj.Chunks = make([]Chunk, 0, chunkCount)
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.SequenceNo, &c.ChunkID, &c.Size, &c.Checksum); err != nil {
			return Object{}, fmt.Errorf("read chunk: %w", err)
		}
		if c.SequenceNo != len(obj.Chunks) {
			return Object{}, fmt.Errorf("object %s: expected chunk %d, found %d", objectID, len(obj.Chunks), c.SequenceNo)
		}
		obj.Chunks = append(obj.Chunks, c)
	}
	if err := rows.Err(); err != nil {
		return Object{}, fmt.Errorf("read chunks: %w", err)
	}
	if len(obj.Chunks) != chunkCount {
		return Object{}, fmt.Errorf("object %s: expected %d chunks, found %d", objectID, chunkCount, len(obj.Chunks))
	}
	return obj, nil
}
