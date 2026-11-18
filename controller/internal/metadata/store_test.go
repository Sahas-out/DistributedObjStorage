package metadata

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func sampleObject(id string) Object {
	return Object{
		ObjectID:  id,
		Size:      15,
		Checksum:  "whole",
		CreatedAt: time.Unix(1700000000, 42).UTC(),
		Chunks: []Chunk{
			{SequenceNo: 0, ChunkID: id + "_0", Size: 10, Checksum: "a"},
			{SequenceNo: 1, ChunkID: id + "_1", Size: 5, Checksum: "b"},
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
	bad.Chunks[1].ChunkID = bad.Chunks[0].ChunkID // violates UNIQUE on the second insert
	if err := s.Create(ctx, bad); err == nil {
		t.Fatal("Create succeeded, want constraint error")
	}
	if _, err := s.Get(ctx, "obj"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after failed Create: error = %v, want ErrNotFound", err)
	}
}
