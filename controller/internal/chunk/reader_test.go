package chunk

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func collect(t *testing.T, r *Reader) [][]byte {
	t.Helper()
	var out [][]byte
	for {
		c, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if c.SequenceNo != len(out) {
			t.Fatalf("sequence = %d, want %d", c.SequenceNo, len(out))
		}
		out = append(out, bytes.Clone(c.Data))
	}
}

func TestReaderSplitsAtChunkBoundaries(t *testing.T) {
	const size = 8
	tests := []struct {
		length int
		sizes  []int
	}{
		{0, nil},
		{1, []int{1}},
		{size - 1, []int{size - 1}},
		{size, []int{size}},
		{size + 1, []int{size, 1}},
		{2 * size, []int{size, size}},
		{2*size + 3, []int{size, size, 3}},
	}
	for _, tt := range tests {
		data := bytes.Repeat([]byte("abcdefghijk"), 5)[:tt.length]
		// OneByteReader proves chunks are filled across short reads.
		chunks := collect(t, NewReader(iotest.OneByteReader(bytes.NewReader(data)), size))
		if len(chunks) != len(tt.sizes) {
			t.Fatalf("length %d: got %d chunks, want %d", tt.length, len(chunks), len(tt.sizes))
		}
		for i, c := range chunks {
			if len(c) != tt.sizes[i] {
				t.Errorf("length %d: chunk %d has %d bytes, want %d", tt.length, i, len(c), tt.sizes[i])
			}
		}
		if got := bytes.Join(chunks, nil); !bytes.Equal(got, data) {
			t.Errorf("length %d: reassembled data differs", tt.length)
		}
	}
}

// A body cut off mid-chunk must not look like a short final chunk.
func TestReaderReportsTruncatedStream(t *testing.T) {
	src := io.MultiReader(bytes.NewReader([]byte("abc")), iotest.ErrReader(io.ErrUnexpectedEOF))
	r := NewReader(src, 8)
	if _, err := r.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Next error = %v, want io.ErrUnexpectedEOF", err)
	}
	if _, err := r.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("second Next error = %v, want the same failure", err)
	}
}
