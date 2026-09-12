package transcriber

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func serving(t *testing.T, status int, body string) *WhisperX {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "token", "fr", "", srv.Client())
}

func TestResultStillRunning(t *testing.T) {
	for _, state := range []string{"queued", "processing"} {
		w := serving(t, 200, `{"status":"`+state+`","result":null,"metadata":{},"error":null}`)
		_, err := w.Result(context.Background(), "abc")
		if !errors.Is(err, ErrStillRunning) {
			t.Fatalf("state %q: expected ErrStillRunning, got %v", state, err)
		}
	}
}

func TestResultCompleted(t *testing.T) {
	w := serving(t, 200, `{"status":"completed","result":{"segments":[
		{"start":0.0,"end":1.5,"speaker":"SPEAKER_00","text":" On entre dans la taverne."}
	]},"metadata":{},"error":null}`)

	segments, err := w.Result(context.Background(), "abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segments))
	}
	if segments[0].Speaker != "SPEAKER_00" {
		t.Fatalf("speaker not carried through: %q", segments[0].Speaker)
	}
}

// A failed task must never be mistaken for something worth retrying, or the
// workflow waits twelve hours for a transcription that will never arrive.
func TestResultFailedIsNotRetryable(t *testing.T) {
	w := serving(t, 200, `{"status":"failed","result":null,"metadata":{},"error":"unsupported codec"}`)
	_, err := w.Result(context.Background(), "abc")
	if err == nil || errors.Is(err, ErrStillRunning) {
		t.Fatalf("expected a terminal error, got %v", err)
	}
}

// "completed" with nothing in it is a failure, not an empty episode.
func TestResultCompletedButEmpty(t *testing.T) {
	w := serving(t, 200, `{"status":"completed","result":{"segments":[]},"metadata":{},"error":null}`)
	if _, err := w.Result(context.Background(), "abc"); err == nil {
		t.Fatal("expected an error for an empty transcription")
	}
}

func TestStartReturnsIdentifier(t *testing.T) {
	w := serving(t, 200, `{"identifier":"task-1","message":"Task queued"}`)
	id, err := w.Start(context.Background(), "http://minio/recordings/a.ogg")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "task-1" {
		t.Fatalf("expected task-1, got %q", id)
	}
}

func TestStartPropagatesHttpErrors(t *testing.T) {
	w := serving(t, 422, `{"detail":"bad url"}`)
	if _, err := w.Start(context.Background(), "nope"); err == nil {
		t.Fatal("expected an error on a 422")
	}
}
