package transcriber

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func serving(t *testing.T, status int, body string) *WhisperX {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return New(Config{
		BaseUrl:     srv.URL,
		Token:       "token",
		Language:    "fr",
		Model:       "big-model",
		Device:      "cpu",
		ComputeType: "int8",
	}, srv.Client())
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
	id, err := w.Start(context.Background(), "http://minio/recordings/a.ogg", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "task-1" {
		t.Fatalf("expected task-1, got %q", id)
	}
}

func TestStartPropagatesHttpErrors(t *testing.T) {
	w := serving(t, 422, `{"detail":"bad url"}`)
	if _, err := w.Start(context.Background(), "nope", 0); err == nil {
		t.Fatal("expected an error on a 422")
	}
}

// /speech-to-text-url takes the model, device and compute type from query
// parameters whose schema defaults are "tiny", "cuda" and "float16" — it does
// not read the server's own WHISPER_MODEL/DEVICE/COMPUTE_TYPE environment.
// Dropping any of them runs a job the deployment was never configured for,
// and on a CPU-only server the task dies with a CUDA driver error well after
// the request has already been answered 200.
func TestStartSendsEveryModelParameter(t *testing.T) {
	var query url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query()
		_, _ = w.Write([]byte(`{"identifier":"task-1"}`))
	}))
	t.Cleanup(srv.Close)

	w := New(Config{
		BaseUrl:     srv.URL,
		Language:    "fr",
		Model:       "big-model",
		Device:      "cpu",
		ComputeType: "int8",
	}, srv.Client())

	if _, err := w.Start(context.Background(), "http://minio/recordings/a.ogg", 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for param, want := range map[string]string{
		"language":     "fr",
		"model":        "big-model",
		"device":       "cpu",
		"compute_type": "int8",
		"diarize":      "true",
	} {
		if got := query.Get(param); got != want {
			t.Errorf("query %s = %q, want %q", param, got, want)
		}
	}
}

// Diarization left to itself over-segments: a seven-person session came back
// with eight speakers. The ceiling is only sent when it is actually known —
// a recording archived without its roster must still be transcribed, and
// max_speakers=0 would be a claim that nobody spoke.
func TestStartSendsSpeakerCeilingOnlyWhenKnown(t *testing.T) {
	var query url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query()
		_, _ = w.Write([]byte(`{"identifier":"task-1"}`))
	}))
	t.Cleanup(srv.Close)
	w := New(Config{BaseUrl: srv.URL, Language: "fr", Model: "m", Device: "cpu", ComputeType: "int8"}, srv.Client())

	if _, err := w.Start(context.Background(), "http://minio/a.ogg", 7); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := query.Get("max_speakers"); got != "7" {
		t.Errorf("max_speakers = %q, want 7", got)
	}
	// A ceiling is never a floor: the roster counts people who joined, and the
	// silent ones leave nothing to diarize.
	if query.Has("min_speakers") {
		t.Errorf("min_speakers must not be sent, got %q", query.Get("min_speakers"))
	}

	if _, err := w.Start(context.Background(), "http://minio/a.ogg", 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if query.Has("max_speakers") {
		t.Errorf("an unknown ceiling must be omitted, got %q", query.Get("max_speakers"))
	}
}
