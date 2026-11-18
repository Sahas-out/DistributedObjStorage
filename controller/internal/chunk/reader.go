// Package chunk splits a stream of object data into fixed-size chunks.
package chunk

import "io"

// Chunk is one piece of an object.
type Chunk struct {
	SequenceNo int
	// Data is only valid until the next call to Reader.Next.
	Data []byte
}

// Reader cuts a stream into chunks using a single reusable buffer, so memory
// use stays at one chunk no matter how large the object is. Reading only when
// the previous chunk has been handled also passes backpressure on to the
// client through TCP flow control.
type Reader struct {
	src io.Reader
	buf []byte
	seq int
	err error
}

// NewReader returns a Reader producing chunks of size bytes. Only the last
// chunk may be smaller.
func NewReader(src io.Reader, size int) *Reader {
	return &Reader{src: src, buf: make([]byte, size)}
}

// Next returns the next chunk, or io.EOF once the stream has ended cleanly.
// Any other error means the stream failed and the object is incomplete.
//
// io.ReadFull is deliberately not used here. It reports a clean end of stream
// in the middle of a chunk as io.ErrUnexpectedEOF, which is also what an HTTP
// request body returns when the client disconnects early, so a truncated
// upload could not be told apart from a short final chunk.
func (r *Reader) Next() (Chunk, error) {
	if r.err != nil {
		return Chunk{}, r.err
	}
	n := 0
	for n < len(r.buf) {
		m, err := r.src.Read(r.buf[n:])
		n += m
		if err == io.EOF {
			r.err = io.EOF
			break
		}
		if err != nil {
			r.err = err
			return Chunk{}, err
		}
	}
	if n == 0 {
		return Chunk{}, io.EOF
	}
	c := Chunk{SequenceNo: r.seq, Data: r.buf[:n]}
	r.seq++
	return c, nil
}
