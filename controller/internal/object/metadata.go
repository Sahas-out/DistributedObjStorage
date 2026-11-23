package object

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/events"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/metadata"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// Limits on user-supplied metadata.
const (
	MaxNameBytes        = 255
	MaxContentTypeLen   = 255
	MaxCustomKeys       = 64
	MaxCustomKeyBytes   = 128
	MaxCustomValueBytes = 1024
)

// DefaultContentType is used when an upload doesn't say what it contains.
const DefaultContentType = "application/octet-stream"

// ErrInvalidMetadata means user-supplied metadata broke a validation rule.
// The error's message says which rule, and is safe to show to the client.
var ErrInvalidMetadata = errors.New("invalid metadata")

var customKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidMetadata, fmt.Sprintf(format, args...))
}

// ValidationMessage returns the client-facing part of an ErrInvalidMetadata
// error.
func ValidationMessage(err error) string {
	return strings.TrimPrefix(err.Error(), ErrInvalidMetadata.Error()+": ")
}

func validateName(name string) error {
	switch {
	case name == "":
		return invalid("name must not be empty")
	case len(name) > MaxNameBytes:
		return invalid("name is longer than %d bytes", MaxNameBytes)
	case !utf8.ValidString(name):
		return invalid("name is not valid UTF-8")
	case strings.ContainsFunc(name, unicode.IsControl):
		return invalid("name must not contain control characters")
	}
	return nil
}

// normalizeContentType checks that ct is a media type such as "text/plain;
// charset=utf-8" and returns it in canonical form.
func normalizeContentType(ct string) (string, error) {
	if len(ct) > MaxContentTypeLen {
		return "", invalid("content_type is longer than %d bytes", MaxContentTypeLen)
	}
	mt, params, err := mime.ParseMediaType(ct)
	if err != nil || !strings.Contains(mt, "/") {
		return "", invalid("content_type %q is not a valid media type such as \"image/png\"", ct)
	}
	out := mime.FormatMediaType(mt, params)
	if out == "" {
		return "", invalid("content_type %q is not a valid media type such as \"image/png\"", ct)
	}
	return out, nil
}

func validateCustomKey(k string) error {
	if len(k) == 0 || len(k) > MaxCustomKeyBytes {
		return invalid("custom metadata key %q must be 1 to %d bytes long", k, MaxCustomKeyBytes)
	}
	if !customKeyPattern.MatchString(k) {
		return invalid("custom metadata key %q may only contain letters, digits, '.', '_' and '-'", k)
	}
	return nil
}

func validateCustomValue(k, v string) error {
	if len(v) > MaxCustomValueBytes {
		return invalid("value of custom metadata key %q is longer than %d bytes", k, MaxCustomValueBytes)
	}
	if !utf8.ValidString(v) {
		return invalid("value of custom metadata key %q is not valid UTF-8", k)
	}
	return nil
}

// validateCustom checks a complete set of custom metadata.
func validateCustom(m map[string]string) error {
	if len(m) > MaxCustomKeys {
		return invalid("at most %d custom metadata keys are allowed, got %d", MaxCustomKeys, len(m))
	}
	for k, v := range m {
		if err := validateCustomKey(k); err != nil {
			return err
		}
		if err := validateCustomValue(k, v); err != nil {
			return err
		}
	}
	return nil
}

// UploadSpec carries the user-supplied metadata of a new object.
type UploadSpec struct {
	Name        string // defaults to the object ID when empty
	ContentType string // defaults to DefaultContentType when empty
	Custom      map[string]string
	// Finish, if set, runs after all of the object's data has been stored
	// and before the object is committed. It may still change the spec,
	// for example with metadata that arrived after the data. If it fails,
	// the upload fails and the stored data is removed.
	Finish func(*UploadSpec) error
}

// prepare validates the spec and fills in defaults.
func (u *UploadSpec) prepare(objectID string) error {
	if u.Name == "" {
		u.Name = objectID
	}
	if err := validateName(u.Name); err != nil {
		return err
	}
	if u.ContentType == "" {
		u.ContentType = DefaultContentType
	}
	ct, err := normalizeContentType(u.ContentType)
	if err != nil {
		return err
	}
	u.ContentType = ct
	return validateCustom(u.Custom)
}

// MetadataUpdate is a change to an object's user-editable metadata. Nil
// fields are left unchanged; a nil custom value removes that key.
type MetadataUpdate struct {
	Name        *string
	ContentType *string
	Custom      map[string]*string
}

// UpdateMetadata validates and applies a metadata change and returns the
// updated object. A change of any field updates the object's modification
// time; an empty change returns the object as it is.
func (s *Service) UpdateMetadata(ctx context.Context, objectID string, u MetadataUpdate) (metadata.Object, error) {
	upd := metadata.Update{Custom: u.Custom, MaxKeys: MaxCustomKeys, Now: time.Now().UTC()}
	if u.Name != nil {
		if err := validateName(*u.Name); err != nil {
			return metadata.Object{}, err
		}
		upd.Name = u.Name
	}
	if u.ContentType != nil {
		ct, err := normalizeContentType(*u.ContentType)
		if err != nil {
			return metadata.Object{}, err
		}
		upd.ContentType = &ct
	}
	for k, v := range u.Custom {
		if err := validateCustomKey(k); err != nil {
			return metadata.Object{}, err
		}
		if v != nil {
			if err := validateCustomValue(k, *v); err != nil {
				return metadata.Object{}, err
			}
		}
	}

	obj, err := s.meta.UpdateMetadata(ctx, objectID, upd)
	if errors.Is(err, metadata.ErrTooManyKeys) {
		return metadata.Object{}, invalid("an object may have at most %d custom metadata keys", MaxCustomKeys)
	}
	if err != nil {
		return metadata.Object{}, err
	}
	if !upd.Empty() {
		s.events.Record(protocol.EventMetadataUpdated, events.Info, "object metadata updated", objectID, "")
	}
	return obj, nil
}
