package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"sort"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/object"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

const (
	// maxMetadataField limits the "metadata" field of an upload form.
	maxMetadataField = 16 << 10
	// maxPatchBody limits the body of a metadata PATCH.
	maxPatchBody = 64 << 10
)

// requestError is a malformed request; its message is sent to the client
// with 400 invalid_request.
type requestError struct{ msg string }

func (e *requestError) Error() string { return e.msg }

func badRequest(format string, args ...any) error {
	return &requestError{msg: fmt.Sprintf(format, args...)}
}

// uploadForm reads a multipart/form-data upload: a required "file" part with
// the object's data and an optional "metadata" part with a JSON object of
// custom string metadata, in either order. Other parts are ignored.
type uploadForm struct {
	mr      *multipart.Reader
	custom  map[string]string
	sawMeta bool
}

// nextFile reads the parts before the file part and returns the file part.
func (f *uploadForm) nextFile() (*multipart.Part, error) {
	for {
		p, err := f.mr.NextPart()
		if errors.Is(err, io.EOF) {
			return nil, badRequest(`the form has no "file" field`)
		}
		if err != nil {
			return nil, badRequest("malformed multipart body: %v", err)
		}
		if p.FormName() == "file" {
			return p, nil
		}
		if err := f.readPart(p); err != nil {
			return nil, err
		}
	}
}

// finish reads the parts after the file part.
func (f *uploadForm) finish() error {
	for {
		p, err := f.mr.NextPart()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return badRequest("malformed multipart body: %v", err)
		}
		if p.FormName() == "file" {
			return badRequest(`the form has more than one "file" field`)
		}
		if err := f.readPart(p); err != nil {
			return err
		}
	}
}

// readPart handles a part other than the file.
func (f *uploadForm) readPart(p *multipart.Part) error {
	defer p.Close()
	if p.FormName() != "metadata" {
		_, err := io.Copy(io.Discard, p)
		return err
	}
	if f.sawMeta {
		return badRequest(`the form has more than one "metadata" field`)
	}
	f.sawMeta = true
	data, err := io.ReadAll(io.LimitReader(p, maxMetadataField+1))
	if err != nil {
		return err
	}
	if len(data) > maxMetadataField {
		return badRequest(`the "metadata" field is larger than %d bytes`, maxMetadataField)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return badRequest(`the "metadata" field must be a JSON object whose values are strings: %v`, err)
	}
	f.custom = m
	return nil
}

// fileContentType picks the content type of an uploaded file: the type its
// part declares, else one guessed from the file name's extension, else the
// default. A generic application/octet-stream also falls back to the guess.
func fileContentType(p *multipart.Part, name string) string {
	ct := p.Header.Get("Content-Type")
	if ct != "" && ct != object.DefaultContentType {
		return ct
	}
	if guess := mime.TypeByExtension(filepath.Ext(name)); guess != "" {
		return guess
	}
	return object.DefaultContentType
}

// handleGetMetadata responds with the object's system and custom metadata and
// where each copy of its chunks is stored, without touching the workers.
func (s *Server) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	if !object.ValidObjectID(id) {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidObjectID, "object id must be 32 lowercase hex characters")
		return
	}
	obj, locs, err := s.objects.Replicas(r.Context(), id)
	if err != nil {
		s.writeObjectError(w, r, id, err)
		return
	}
	writeJSON(w, http.StatusOK, objectMetadata(obj, locs))
}

// patchBody mirrors protocol.MetadataPatch, keeping name and content_type
// raw so an explicit null can be told apart from an omitted field.
type patchBody struct {
	Name           json.RawMessage    `json:"name"`
	ContentType    json.RawMessage    `json:"content_type"`
	CustomMetadata map[string]*string `json:"custom_metadata"`
}

// handlePatchMetadata changes an object's name, content type or custom
// metadata without touching its data, and responds like handleGetMetadata.
func (s *Server) handlePatchMetadata(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	if !object.ValidObjectID(id) {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidObjectID, "object id must be 32 lowercase hex characters")
		return
	}
	upd, err := decodePatch(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, err.Error())
		return
	}
	obj, err := s.objects.UpdateMetadata(r.Context(), id, upd)
	if err != nil {
		s.writeObjectError(w, r, id, err)
		return
	}
	writeJSON(w, http.StatusOK, objectMetadata(obj, s.objects.Locations(id)))
}

func decodePatch(w http.ResponseWriter, r *http.Request) (object.MetadataUpdate, error) {
	var upd object.MetadataUpdate
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (mt != "application/json" && mt != "application/merge-patch+json") {
		return upd, errors.New("the request body must be JSON (Content-Type: application/json)")
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxPatchBody))
	dec.DisallowUnknownFields()
	var body patchBody
	if err := dec.Decode(&body); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return upd, fmt.Errorf("the request body is larger than %d bytes", maxPatchBody)
		}
		return upd, fmt.Errorf(`the body must be a JSON object with optional "name", "content_type" and "custom_metadata" (an object of strings, or null to remove a key): %v`, err)
	}
	if dec.More() {
		return upd, errors.New("the request body must hold a single JSON object")
	}
	if upd.Name, err = optionalString("name", body.Name); err != nil {
		return upd, err
	}
	if upd.ContentType, err = optionalString("content_type", body.ContentType); err != nil {
		return upd, err
	}
	upd.Custom = body.CustomMetadata
	return upd, nil
}

// optionalString decodes a field that may be omitted but, when present, must
// be a string.
func optionalString(field string, raw json.RawMessage) (*string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if string(raw) == "null" {
		return nil, fmt.Errorf("%q cannot be removed; give a new value instead", field)
	}
	var v string
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("%q must be a string", field)
	}
	return &v, nil
}

// objectMetadata builds the metadata response from an object and the
// current locations of its replicas.
func objectMetadata(obj metadata.Object, locs []cluster.Location) protocol.ObjectMetadata {
	out := protocol.ObjectMetadata{
		ObjectInfo:     objectInfo(obj),
		CustomMetadata: obj.Custom,
		Chunks:         make([]protocol.ChunkInfo, len(obj.Chunks)),
	}
	if out.CustomMetadata == nil {
		out.CustomMetadata = map[string]string{}
	}
	for i, c := range obj.Chunks {
		out.Chunks[i] = protocol.ChunkInfo{SequenceNo: c.SequenceNo, Size: c.Size, Checksum: c.Checksum, Replicas: []protocol.ChunkLocation{}}
	}
	sort.Slice(locs, func(i, j int) bool { return locs[i].WorkerID < locs[j].WorkerID })
	for _, l := range locs {
		if l.SeqNo < 0 || l.SeqNo >= len(out.Chunks) {
			continue
		}
		out.Chunks[l.SeqNo].Replicas = append(out.Chunks[l.SeqNo].Replicas, protocol.ChunkLocation{
			ChunkID:    l.ChunkID,
			WorkerID:   l.WorkerID,
			WorkerAddr: l.Addr,
			Status:     string(l.Status),
		})
	}
	return out
}
