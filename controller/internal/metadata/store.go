// Package metadata persists object metadata, each object's custom key/value
// metadata and each object's ordered chunk list in SQLite.
//
// Only facts that don't change while the object exists are persisted: each
// chunk's position, size and checksum. Where the replicas of a chunk live
// (chunk IDs and workers) is runtime state that changes as workers come and
// go or replicas are lost or corrupted, so it is kept in memory by the
// cluster package and rebuilt from the workers' chunk reports.
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
	object_id    TEXT    PRIMARY KEY,
	name         TEXT    NOT NULL,
	content_type TEXT    NOT NULL,
	size         INTEGER NOT NULL CHECK (size >= 0),
	checksum     TEXT    NOT NULL,
	chunk_count  INTEGER NOT NULL CHECK (chunk_count >= 0),
	created_at   INTEGER NOT NULL, -- Unix time in nanoseconds, UTC
	updated_at   INTEGER NOT NULL  -- Unix time in nanoseconds, UTC
);
CREATE INDEX IF NOT EXISTS objects_by_created ON objects (created_at, object_id);
CREATE TABLE IF NOT EXISTS object_chunks (
	object_id   TEXT    NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
	sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
	size        INTEGER NOT NULL CHECK (size > 0),
	checksum    TEXT    NOT NULL,
	PRIMARY KEY (object_id, sequence_no)
);
CREATE TABLE IF NOT EXISTS object_custom_metadata (
	object_id TEXT NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
	key       TEXT NOT NULL,
	value     TEXT NOT NULL,
	PRIMARY KEY (object_id, key)
);
`

var (
	// ErrNotFound is returned when an object has no metadata.
	ErrNotFound = errors.New("object not found")
	// ErrTooManyKeys is returned by UpdateMetadata when the update would
	// leave an object with more custom keys than allowed.
	ErrTooManyKeys = errors.New("too many custom metadata keys")
)

// Chunk records where one piece of an object sits in the sequence and how to
// verify any replica of it.
type Chunk struct {
	SequenceNo int
	Size       int64
	Checksum   string // SHA-256 hex digest
}

// Object is the persisted description of a stored object.
type Object struct {
	ObjectID    string
	Name        string
	ContentType string
	Size        int64
	Checksum    string // SHA-256 hex digest of the whole object
	ChunkCount  int
	CreatedAt   time.Time
	UpdatedAt   time.Time         // last change of name, content type or custom metadata
	Custom      map[string]string // user-defined metadata; nil in List results
	Chunks      []Chunk           // ordered by SequenceNo; nil in List results
}

// Update describes a change to an object's user-editable metadata. Nil
// fields are left unchanged.
type Update struct {
	Name        *string
	ContentType *string
	// Custom sets each key to its value; a nil value removes the key.
	Custom map[string]*string
	// MaxKeys limits how many custom keys the object may have afterwards.
	// Zero means no limit.
	MaxKeys int
	// Now becomes the object's UpdatedAt.
	Now time.Time
}

// Empty reports whether u changes nothing.
func (u Update) Empty() bool {
	return u.Name == nil && u.ContentType == nil && len(u.Custom) == 0
}

// querier is satisfied by *sql.DB and *sql.Tx.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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

	updated := obj.UpdatedAt
	if updated.IsZero() {
		updated = obj.CreatedAt
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO objects (object_id, name, content_type, size, checksum, chunk_count, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		obj.ObjectID, obj.Name, obj.ContentType, obj.Size, obj.Checksum, len(obj.Chunks),
		obj.CreatedAt.UnixNano(), updated.UnixNano())
	if err != nil {
		return fmt.Errorf("insert object: %w", err)
	}
	for k, v := range obj.Custom {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO object_custom_metadata (object_id, key, value) VALUES (?, ?, ?)`, obj.ObjectID, k, v); err != nil {
			return fmt.Errorf("insert custom metadata %q: %w", k, err)
		}
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO object_chunks (object_id, sequence_no, size, checksum) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare chunk insert: %w", err)
	}
	defer stmt.Close()
	for _, c := range obj.Chunks {
		if _, err := stmt.ExecContext(ctx, obj.ObjectID, c.SequenceNo, c.Size, c.Checksum); err != nil {
			return fmt.Errorf("insert chunk %d: %w", c.SequenceNo, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit object: %w", err)
	}
	return nil
}

// Get returns an object with its custom metadata and its chunks in sequence
// order, or ErrNotFound.
func (s *Store) Get(ctx context.Context, objectID string) (Object, error) {
	return get(ctx, s.db, objectID)
}

func get(ctx context.Context, q querier, objectID string) (Object, error) {
	obj := Object{ObjectID: objectID}
	var created, updated int64
	err := q.QueryRowContext(ctx,
		`SELECT name, content_type, size, checksum, chunk_count, created_at, updated_at FROM objects WHERE object_id = ?`, objectID).
		Scan(&obj.Name, &obj.ContentType, &obj.Size, &obj.Checksum, &obj.ChunkCount, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("read object: %w", err)
	}
	obj.CreatedAt = time.Unix(0, created).UTC()
	obj.UpdatedAt = time.Unix(0, updated).UTC()

	if obj.Custom, err = customMetadata(ctx, q, objectID); err != nil {
		return Object{}, err
	}

	rows, err := q.QueryContext(ctx,
		`SELECT sequence_no, size, checksum FROM object_chunks WHERE object_id = ? ORDER BY sequence_no`, objectID)
	if err != nil {
		return Object{}, fmt.Errorf("read chunks: %w", err)
	}
	defer rows.Close()

	obj.Chunks = make([]Chunk, 0, obj.ChunkCount)
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.SequenceNo, &c.Size, &c.Checksum); err != nil {
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
	if len(obj.Chunks) != obj.ChunkCount {
		return Object{}, fmt.Errorf("object %s: expected %d chunks, found %d", objectID, obj.ChunkCount, len(obj.Chunks))
	}
	return obj, nil
}

// customMetadata returns an object's custom key/value pairs; an empty map
// when it has none.
func customMetadata(ctx context.Context, q querier, objectID string) (map[string]string, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT key, value FROM object_custom_metadata WHERE object_id = ?`, objectID)
	if err != nil {
		return nil, fmt.Errorf("read custom metadata: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("read custom metadata: %w", err)
		}
		out[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read custom metadata: %w", err)
	}
	return out, nil
}

// UpdateMetadata applies u to an object in one transaction and returns the
// updated object, or ErrNotFound. If the result would have more than
// u.MaxKeys custom keys, nothing changes and ErrTooManyKeys is returned. An
// empty update changes nothing, not even UpdatedAt.
func (s *Store) UpdateMetadata(ctx context.Context, objectID string, u Update) (Object, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Object{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM objects WHERE object_id = ?`, objectID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("look up object: %w", err)
	}
	if u.Empty() {
		return get(ctx, tx, objectID)
	}

	if u.Name != nil {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET name = ? WHERE object_id = ?`, *u.Name, objectID); err != nil {
			return Object{}, fmt.Errorf("update name: %w", err)
		}
	}
	if u.ContentType != nil {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET content_type = ? WHERE object_id = ?`, *u.ContentType, objectID); err != nil {
			return Object{}, fmt.Errorf("update content type: %w", err)
		}
	}
	for k, v := range u.Custom {
		if v == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM object_custom_metadata WHERE object_id = ? AND key = ?`, objectID, k)
		} else {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO object_custom_metadata (object_id, key, value) VALUES (?, ?, ?)
				 ON CONFLICT (object_id, key) DO UPDATE SET value = excluded.value`, objectID, k, *v)
		}
		if err != nil {
			return Object{}, fmt.Errorf("update custom metadata %q: %w", k, err)
		}
	}
	if u.MaxKeys > 0 {
		var n int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM object_custom_metadata WHERE object_id = ?`, objectID).Scan(&n); err != nil {
			return Object{}, fmt.Errorf("count custom metadata: %w", err)
		}
		if n > u.MaxKeys {
			return Object{}, fmt.Errorf("%w: %d keys, at most %d allowed", ErrTooManyKeys, n, u.MaxKeys)
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE objects SET updated_at = ? WHERE object_id = ?`, u.Now.UnixNano(), objectID); err != nil {
		return Object{}, fmt.Errorf("update modification time: %w", err)
	}

	obj, err := get(ctx, tx, objectID)
	if err != nil {
		return Object{}, err
	}
	if err := tx.Commit(); err != nil {
		return Object{}, fmt.Errorf("commit metadata update: %w", err)
	}
	return obj, nil
}

// List returns every object, oldest first, without chunk lists or custom
// metadata.
func (s *Store) List(ctx context.Context) ([]Object, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT object_id, name, content_type, size, checksum, chunk_count, created_at, updated_at
		 FROM objects ORDER BY created_at, object_id`)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}
	defer rows.Close()
	out := []Object{}
	for rows.Next() {
		var o Object
		var created, updated int64
		if err := rows.Scan(&o.ObjectID, &o.Name, &o.ContentType, &o.Size, &o.Checksum, &o.ChunkCount, &created, &updated); err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		o.CreatedAt = time.Unix(0, created).UTC()
		o.UpdatedAt = time.Unix(0, updated).UTC()
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}
	return out, nil
}

// Delete removes an object with its chunk records and custom metadata, or
// returns ErrNotFound.
func (s *Store) Delete(ctx context.Context, objectID string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM objects WHERE object_id = ?`, objectID)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// HasChunk reports whether objectID exists and has a chunk at position seq.
// It is used to classify chunks reported by workers.
func (s *Store) HasChunk(ctx context.Context, objectID string, seq int) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM object_chunks WHERE object_id = ? AND sequence_no = ?`, objectID, seq).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("look up chunk: %w", err)
	}
	return true, nil
}

// ChunkRef is one chunk of a stored object, as AllChunks lists it.
type ChunkRef struct {
	ObjectID string
	Chunk
}

// AllChunks returns every chunk of every stored object, ordered by object
// and sequence number. The recovery job compares it with the replicas the
// workers actually hold.
func (s *Store) AllChunks(ctx context.Context) ([]ChunkRef, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT object_id, sequence_no, size, checksum FROM object_chunks ORDER BY object_id, sequence_no`)
	if err != nil {
		return nil, fmt.Errorf("list chunks: %w", err)
	}
	defer rows.Close()
	var out []ChunkRef
	for rows.Next() {
		var c ChunkRef
		if err := rows.Scan(&c.ObjectID, &c.SequenceNo, &c.Size, &c.Checksum); err != nil {
			return nil, fmt.Errorf("list chunks: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list chunks: %w", err)
	}
	return out, nil
}
