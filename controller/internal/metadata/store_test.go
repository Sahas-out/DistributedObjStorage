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
	return Object{
		ObjectID:   id,
		Size:       15,
		Checksum:   "whole",
		ChunkCount: 2,
		CreatedAt:  time.Unix(1700000000, 42).UTC(),
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
	if got[0].ChunkCount != 2 || got[0].Size != 15 || got[0].Chunks != nil {
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
}
