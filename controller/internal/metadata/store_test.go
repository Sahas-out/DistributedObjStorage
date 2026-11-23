package metadata

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func sampleObject(id string) Object {
	created := time.Unix(1700000000, 42).UTC()
	return Object{
		ObjectID:    id,
		Name:        "photo.jpg",
		ContentType: "image/jpeg",
		Size:        15,
		Checksum:    "whole",
		ChunkCount:  2,
		CreatedAt:   created,
		UpdatedAt:   created,
		Custom:      map[string]string{"owner": "alice", "tag": "demo"},
		Chunks: []Chunk{
			{SequenceNo: 0, Size: 10, Checksum: "a"},
			{SequenceNo: 1, Size: 5, Checksum: "b"},
		},
	}
}

func TestMetadataSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	want := sampleObject("obj")
	if err := s.Create(ctx, want); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Get(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestGetMissingObject(t *testing.T) {
	s, err := Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Get(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

func TestCreateIsAtomic(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	bad := sampleObject("obj")
	bad.Chunks[1].SequenceNo = 0 // violates the primary key on the second insert
	if err := s.Create(ctx, bad); err == nil {
		t.Fatal("Create succeeded, want constraint error")
	}
	if _, err := s.Get(ctx, "obj"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after failed Create: error = %v, want ErrNotFound", err)
	}
}

func TestListAndDelete(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	if got, err := s.List(ctx); err != nil || len(got) != 0 || got == nil {
		t.Fatalf("List on empty store = %#v, %v; want empty non-nil slice", got, err)
	}

	a, b := sampleObject("a"), sampleObject("b")
	b.CreatedAt = a.CreatedAt.Add(time.Second)
	for _, o := range []Object{b, a} {
		if err := s.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ObjectID != "a" || got[1].ObjectID != "b" {
		t.Fatalf("List = %+v, want a then b", got)
	}
	if got[0].ChunkCount != 2 || got[0].Size != 15 || got[0].Chunks != nil || got[0].Custom != nil ||
		got[0].Name != "photo.jpg" || got[0].ContentType != "image/jpeg" || !got[0].UpdatedAt.Equal(a.CreatedAt) {
		t.Fatalf("List entry = %+v", got[0])
	}

	if ok, err := s.HasChunk(ctx, "a", 1); err != nil || !ok {
		t.Fatalf("HasChunk(a, 1) = %v, %v; want true", ok, err)
	}
	if ok, err := s.HasChunk(ctx, "a", 2); err != nil || ok {
		t.Fatalf("HasChunk(a, 2) = %v, %v; want false", ok, err)
	}

	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete: error = %v, want ErrNotFound", err)
	}
	if _, err := s.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete: error = %v, want ErrNotFound", err)
	}
	if ok, _ := s.HasChunk(ctx, "a", 0); ok {
		t.Fatal("chunk rows survived Delete")
	}
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM object_chunks WHERE object_id = 'a'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("object_chunks rows for deleted object = %d, %v", n, err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM object_custom_metadata WHERE object_id = 'a'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("object_custom_metadata rows for deleted object = %d, %v", n, err)
	}
}

func TestAllChunks(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	if got, err := s.AllChunks(ctx); err != nil || len(got) != 0 {
		t.Fatalf("AllChunks on empty store = %v, %v", got, err)
	}
	for _, id := range []string{"b", "a"} {
		if err := s.Create(ctx, sampleObject(id)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.AllChunks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []ChunkRef{
		{ObjectID: "a", Chunk: Chunk{SequenceNo: 0, Size: 10, Checksum: "a"}},
		{ObjectID: "a", Chunk: Chunk{SequenceNo: 1, Size: 5, Checksum: "b"}},
		{ObjectID: "b", Chunk: Chunk{SequenceNo: 0, Size: 10, Checksum: "a"}},
		{ObjectID: "b", Chunk: Chunk{SequenceNo: 1, Size: 5, Checksum: "b"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AllChunks = %+v\nwant %+v", got, want)
	}
}

func ptr(s string) *string { return &s }

func TestUpdateMetadata(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	orig := sampleObject("obj")
	if err := s.Create(ctx, orig); err != nil {
		t.Fatal(err)
	}

	now := orig.CreatedAt.Add(time.Hour)
	got, err := s.UpdateMetadata(ctx, "obj", Update{
		Name:   ptr("renamed.jpg"),
		Custom: map[string]*string{"tag": nil, "reviewed": ptr("yes"), "owner": ptr("bob")},
		Now:    now,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := orig
	want.Name = "renamed.jpg"
	want.UpdatedAt = now
	want.Custom = map[string]string{"owner": "bob", "reviewed": "yes"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UpdateMetadata returned %+v\nwant %+v", got, want)
	}
	if stored, err := s.Get(ctx, "obj"); err != nil || !reflect.DeepEqual(stored, want) {
		t.Fatalf("Get after update = %+v, %v\nwant %+v", stored, err, want)
	}

	// Content type alone; deleting a key that isn't there is fine.
	later := now.Add(time.Minute)
	got, err = s.UpdateMetadata(ctx, "obj", Update{ContentType: ptr("image/png"), Custom: map[string]*string{"missing": nil}, Now: later})
	if err != nil {
		t.Fatal(err)
	}
	if got.ContentType != "image/png" || got.Name != "renamed.jpg" || !got.UpdatedAt.Equal(later) {
		t.Fatalf("after content-type update: %+v", got)
	}
}

func TestUpdateMetadataEmptyKeepsModificationTime(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	orig := sampleObject("obj")
	if err := s.Create(ctx, orig); err != nil {
		t.Fatal(err)
	}
	got, err := s.UpdateMetadata(ctx, "obj", Update{Now: orig.CreatedAt.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, orig) {
		t.Fatalf("empty update changed the object: %+v", got)
	}
}

func TestUpdateMetadataMissingObject(t *testing.T) {
	s := openStore(t)
	_, err := s.UpdateMetadata(context.Background(), "nope", Update{Name: ptr("x"), Now: time.Now()})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
	if _, err := s.UpdateMetadata(context.Background(), "nope", Update{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty update: error = %v, want ErrNotFound", err)
	}
}

func TestUpdateMetadataKeyLimitRollsBack(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	orig := sampleObject("obj") // two custom keys
	if err := s.Create(ctx, orig); err != nil {
		t.Fatal(err)
	}
	_, err := s.UpdateMetadata(ctx, "obj", Update{
		Name:    ptr("other"),
		Custom:  map[string]*string{"c": ptr("3")},
		MaxKeys: 2,
		Now:     orig.CreatedAt.Add(time.Hour),
	})
	if !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("error = %v, want ErrTooManyKeys", err)
	}
	if got, err := s.Get(ctx, "obj"); err != nil || !reflect.DeepEqual(got, orig) {
		t.Fatalf("object changed by rejected update: %+v, %v", got, err)
	}

	// Replacing a key keeps the count at the limit.
	if _, err := s.UpdateMetadata(ctx, "obj", Update{
		Custom: map[string]*string{"tag": nil, "c": ptr("3")}, MaxKeys: 2, Now: orig.CreatedAt.Add(time.Hour),
	}); err != nil {
		t.Fatalf("update within limit: %v", err)
	}
}
