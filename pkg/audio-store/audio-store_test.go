package audio_store

import (
	"context"
	"errors"
	"testing"

	test_utils "summary-orchestrator/test-utils"
)

func TestPresignedUrl(t *testing.T) {
	binder := &test_utils.MockBinder{Data: []byte(`{"presignURL":"http://minio/recordings/a.ogg?sig=x"}`)}
	store := New("object-store", "6h", binder)

	url, err := store.PresignedUrl(context.Background(), "a.ogg")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if url != "http://minio/recordings/a.ogg?sig=x" {
		t.Fatalf("got %q", url)
	}

	call := binder.Calls[0]
	if call.Operation != "presign" {
		t.Fatalf("expected the presign operation, got %q", call.Operation)
	}
	if call.Metadata["key"] != "a.ogg" || call.Metadata["presignTTL"] != "6h" {
		t.Fatalf("unexpected metadata %+v", call.Metadata)
	}
}

// An empty URL would be handed to whisper and fail there instead of here.
func TestPresignedUrlRejectsEmptyResponse(t *testing.T) {
	store := New("object-store", "6h", &test_utils.MockBinder{Data: []byte(`{}`)})
	if _, err := store.PresignedUrl(context.Background(), "a.ogg"); err == nil {
		t.Fatal("expected an error for an empty presign response")
	}
}

func TestPresignedUrlPropagatesBindingErrors(t *testing.T) {
	store := New("object-store", "6h", &test_utils.MockBinder{Err: errors.New("no such key")})
	if _, err := store.PresignedUrl(context.Background(), "missing.ogg"); err == nil {
		t.Fatal("expected the binding error to surface")
	}
}
