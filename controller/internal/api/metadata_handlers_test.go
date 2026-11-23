package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

func getMetadata(t *testing.T, base, id string) protocol.ObjectMetadata {
	t.Helper()
	resp := do(t, http.MethodGet, base+"/api/v1/objects/"+id+"/metadata", nil)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET metadata status = %d: %s", resp.StatusCode, body)
	}
	var m protocol.ObjectMetadata
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

func patch(t *testing.T, base, id, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPatch, base+"/api/v1/objects/"+id+"/metadata", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestUploadWithMetadata(t *testing.T) {
	env := newTestServer(t, 3)
	data := []byte(strings.Repeat("x", 2*testChunkSize+3))

	for _, order := range []string{"metadata first", "metadata last"} {
		t.Run(order, func(t *testing.T) {
			parts := []formPart{
				metaPart(`{"owner":"alice","tag":"demo"}`),
				{field: "file", filename: "photo.png", contentType: "image/png", body: string(data)},
				{field: "comment", body: "ignored"},
			}
			if order == "metadata last" {
				parts[0], parts[2] = parts[2], parts[0]
			}
			info := uploadParts(t, env.srv.URL, parts...)
			if info.Name != "photo.png" || info.ContentType != "image/png" || info.Size != int64(len(data)) ||
				info.ChunkCount != 3 || !info.LastModifiedAt.Equal(info.CreatedAt) {
				t.Fatalf("info = %+v", info)
			}

			m := getMetadata(t, env.srv.URL, info.ObjectID)
			if !reflect.DeepEqual(m.ObjectInfo, info) {
				t.Fatalf("metadata info = %+v, want %+v", m.ObjectInfo, info)
			}
			if !reflect.DeepEqual(m.CustomMetadata, map[string]string{"owner": "alice", "tag": "demo"}) {
				t.Fatalf("custom = %v", m.CustomMetadata)
			}
			if len(m.Chunks) != 3 {
				t.Fatalf("chunks = %+v", m.Chunks)
			}
			for i, c := range m.Chunks {
				if c.SequenceNo != i || c.Checksum == "" || len(c.Replicas) != 2 || c.Replicas[0].WorkerID == c.Replicas[1].WorkerID {
					t.Fatalf("chunk %d = %+v", i, c)
				}
				for _, r := range c.Replicas {
					if r.Status != string(cluster.ReplicaActive) || r.ChunkID == "" || r.WorkerAddr == "" {
						t.Fatalf("replica = %+v", r)
					}
				}
			}
		})
	}
}

func TestUploadGuessesContentTypeAndDefaultsName(t *testing.T) {
	env := newTestServer(t)
	info := upload(t, env.srv.URL, []byte(`{"a":1}`)) // data.bin, application/octet-stream
	if info.Name != "data.bin" || info.ContentType != "application/octet-stream" {
		t.Fatalf("info = %+v", info)
	}
	info = uploadParts(t, env.srv.URL, filePart("doc.json", []byte(`{}`)))
	if info.ContentType != "application/json" {
		t.Fatalf("content type from extension = %q", info.ContentType)
	}
	info = uploadParts(t, env.srv.URL, formPart{field: "file", body: "no filename"})
	if info.Name != info.ObjectID {
		t.Fatalf("name without filename = %q, want the object ID", info.Name)
	}
	m := getMetadata(t, env.srv.URL, info.ObjectID)
	if m.CustomMetadata == nil || len(m.CustomMetadata) != 0 {
		t.Fatalf("custom metadata = %#v, want empty object", m.CustomMetadata)
	}
}

func TestUploadFormErrors(t *testing.T) {
	env := newTestServer(t)
	cases := []struct {
		name  string
		parts []formPart
		code  string
	}{
		{"no file", []formPart{metaPart(`{}`)}, protocol.CodeInvalidRequest},
		{"two files", []formPart{filePart("a", []byte("a")), filePart("b", []byte("b"))}, protocol.CodeInvalidRequest},
		{"metadata not JSON", []formPart{metaPart(`owner=alice`), filePart("a", []byte("a"))}, protocol.CodeInvalidRequest},
		{"metadata not strings", []formPart{filePart("a", []byte("a")), metaPart(`{"n":1}`)}, protocol.CodeInvalidRequest},
		{"two metadata fields", []formPart{metaPart(`{}`), metaPart(`{}`), filePart("a", []byte("a"))}, protocol.CodeInvalidRequest},
		{"bad key", []formPart{metaPart(`{"bad key":"v"}`), filePart("a", []byte("a"))}, protocol.CodeInvalidMetadata},
		{"bad key after file", []formPart{filePart("a", []byte("a")), metaPart(`{"bad key":"v"}`)}, protocol.CodeInvalidMetadata},
		{"bad content type", []formPart{{field: "file", filename: "a", contentType: "garbage", body: "a"}}, protocol.CodeInvalidMetadata},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantError(t, postForm(t, env.srv.URL, c.parts...), http.StatusBadRequest, c.code)
		})
	}
	t.Run("not multipart", func(t *testing.T) {
		wantError(t, do(t, http.MethodPost, env.srv.URL+"/api/v1/objects", strings.NewReader("raw")), http.StatusBadRequest, protocol.CodeInvalidRequest)
	})
	if n := env.worker.count(); n != 0 {
		t.Fatalf("failed uploads left %d chunks on the workers", n)
	}
	resp := do(t, http.MethodGet, env.srv.URL+"/api/v1/objects", nil)
	var list []protocol.ObjectInfo
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil || len(list) != 0 {
		t.Fatalf("list after failed uploads = %v, %v", list, err)
	}
}

func TestDownloadHeadersUseMetadata(t *testing.T) {
	env := newTestServer(t)
	info := uploadParts(t, env.srv.URL, formPart{field: "file", filename: "résumé.pdf", contentType: "application/pdf", body: "%PDF"})
	resp := do(t, http.MethodGet, env.srv.URL+"/api/v1/objects/"+info.ObjectID, nil)
	h := resp.Header
	if h.Get("Content-Type") != "application/pdf" || h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Last-Modified") == "" {
		t.Fatalf("headers = %v", h)
	}
	if cd := h.Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment;") || !strings.Contains(cd, "r%C3%A9sum%C3%A9.pdf") {
		t.Fatalf("Content-Disposition = %q", cd)
	}
}

func TestPatchMetadata(t *testing.T) {
	env := newTestServer(t)
	info := uploadParts(t, env.srv.URL, metaPart(`{"owner":"alice","tag":"demo"}`), filePart("a.bin", []byte("payload")))

	resp := patch(t, env.srv.URL, info.ObjectID,
		`{"name":"renamed.txt","content_type":"text/plain","custom_metadata":{"tag":null,"reviewed":"yes","owner":"bob"}}`)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PATCH status = %d: %s", resp.StatusCode, body)
	}
	var got protocol.ObjectMetadata
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "renamed.txt" || got.ContentType != "text/plain" || !got.LastModifiedAt.After(info.LastModifiedAt) ||
		!got.CreatedAt.Equal(info.CreatedAt) || got.Checksum != info.Checksum || got.Size != info.Size || len(got.Chunks) != 1 {
		t.Fatalf("PATCH response = %+v", got)
	}
	if want := map[string]string{"owner": "bob", "reviewed": "yes"}; !reflect.DeepEqual(got.CustomMetadata, want) {
		t.Fatalf("custom = %v, want %v", got.CustomMetadata, want)
	}
	if again := getMetadata(t, env.srv.URL, info.ObjectID); !reflect.DeepEqual(again, got) {
		t.Fatalf("GET after PATCH = %+v\nwant %+v", again, got)
	}

	// The data is untouched and is now served under the new name and type.
	dl := do(t, http.MethodGet, env.srv.URL+"/api/v1/objects/"+info.ObjectID, nil)
	body, _ := io.ReadAll(dl.Body)
	if string(body) != "payload" || dl.Header.Get("Content-Type") != "text/plain" ||
		!strings.Contains(dl.Header.Get("Content-Disposition"), "renamed.txt") {
		t.Fatalf("download after PATCH: %q, %v", body, dl.Header)
	}

	// An empty patch changes nothing.
	resp = patch(t, env.srv.URL, info.ObjectID, `{}`)
	var same protocol.ObjectMetadata
	if err := json.NewDecoder(resp.Body).Decode(&same); err != nil || resp.StatusCode != http.StatusOK || !reflect.DeepEqual(same, got) {
		t.Fatalf("empty PATCH: status %d, %+v, %v", resp.StatusCode, same, err)
	}
}

func TestPatchMetadataErrors(t *testing.T) {
	env := newTestServer(t)
	info := upload(t, env.srv.URL, []byte("data"))
	many := map[string]string{}
	for i := range 65 {
		many[fmt.Sprintf("k%d", i)] = "v"
	}
	tooMany, _ := json.Marshal(map[string]any{"custom_metadata": many})

	cases := []struct {
		name, body string
		status     int
		code       string
	}{
		{"not JSON", `owner=bob`, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"array", `[]`, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"unknown field", `{"owner":"bob"}`, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"non-string value", `{"custom_metadata":{"n":1}}`, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"null name", `{"name":null}`, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"numeric name", `{"name":5}`, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"two objects", `{} {}`, http.StatusBadRequest, protocol.CodeInvalidRequest},
		{"empty name", `{"name":""}`, http.StatusBadRequest, protocol.CodeInvalidMetadata},
		{"bad content type", `{"content_type":"nope"}`, http.StatusBadRequest, protocol.CodeInvalidMetadata},
		{"bad key", `{"custom_metadata":{"a b":"c"}}`, http.StatusBadRequest, protocol.CodeInvalidMetadata},
		{"too many keys", string(tooMany), http.StatusBadRequest, protocol.CodeInvalidMetadata},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantError(t, patch(t, env.srv.URL, info.ObjectID, c.body), c.status, c.code)
		})
	}

	t.Run("missing object", func(t *testing.T) {
		wantError(t, patch(t, env.srv.URL, strings.Repeat("c", 32), `{"name":"x"}`), http.StatusNotFound, protocol.CodeObjectNotFound)
		wantError(t, do(t, http.MethodGet, env.srv.URL+"/api/v1/objects/"+strings.Repeat("c", 32)+"/metadata", nil), http.StatusNotFound, protocol.CodeObjectNotFound)
	})
	t.Run("malformed id", func(t *testing.T) {
		wantError(t, patch(t, env.srv.URL, "BAD", `{}`), http.StatusBadRequest, protocol.CodeInvalidObjectID)
	})
	t.Run("wrong content type", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPatch, env.srv.URL+"/api/v1/objects/"+info.ObjectID+"/metadata", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("Content-Type", "text/plain")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		wantError(t, resp, http.StatusBadRequest, protocol.CodeInvalidRequest)
	})
	t.Run("wrong method", func(t *testing.T) {
		wantError(t, do(t, http.MethodPut, env.srv.URL+"/api/v1/objects/"+info.ObjectID+"/metadata", nil), http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed)
	})

	// Nothing above changed the object.
	m := getMetadata(t, env.srv.URL, info.ObjectID)
	if m.Name != info.Name || len(m.CustomMetadata) != 0 || !m.LastModifiedAt.Equal(info.LastModifiedAt) {
		t.Fatalf("object changed by rejected PATCHes: %+v", m)
	}
}
