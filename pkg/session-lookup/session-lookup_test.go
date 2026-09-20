package session_lookup

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	test_utils "summary-orchestrator/test-utils"
)

func TestSessionNumberFound(t *testing.T) {
	binder := &test_utils.MockBinder{Data: []byte(`[[4]]`)}
	lookup := New("db", binder)

	number, err := lookup.SessionNumber(context.Background(), 14, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if number != 4 {
		t.Fatalf("expected 4, got %d", number)
	}
	if len(binder.Calls) != 1 || binder.Calls[0].Operation != "query" {
		t.Fatalf("expected one query, got %+v", binder.Calls)
	}
}

// No row means the video has not been uploaded yet. This is the signal the
// workflow backs off on, so it must be distinguishable from a real failure.
func TestSessionNumberNotReady(t *testing.T) {
	lookup := New("db", &test_utils.MockBinder{Data: []byte(`[]`)})
	if _, err := lookup.SessionNumber(context.Background(), 14, 4); !errors.Is(err, ErrSessionNotReady) {
		t.Fatalf("expected ErrSessionNotReady, got %v", err)
	}
}

func TestSessionNumberBindingFailureIsNotNotReady(t *testing.T) {
	lookup := New("db", &test_utils.MockBinder{Err: errors.New("sidecar down")})
	_, err := lookup.SessionNumber(context.Background(), 14, 4)
	if err == nil || errors.Is(err, ErrSessionNotReady) {
		t.Fatalf("a database failure must not look like a missing row, got %v", err)
	}
}

func TestSanitizeFlattensSql(t *testing.T) {
	got := sanitize("SELECT a\n\tFROM b\n  WHERE c = $1")
	want := "SELECT a FROM b WHERE c = $1"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

// The binding renders a timestamp column as an RFC3339 string, unlike the
// session number which arrives as a JSON number.
func TestSessionDateParsesTheBindingsTimestamp(t *testing.T) {
	binder := &test_utils.MockBinder{Data: []byte(`[["2023-02-18T00:00:00Z"]]`)}
	lookup := New("db", binder)

	got, err := lookup.SessionDate(context.Background(), 22, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Date(2023, 2, 18, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}
	if len(binder.Calls) != 1 || !strings.Contains(binder.Calls[0].Metadata["sql"], "createdAt") {
		t.Fatalf("unexpected query: %+v", binder.Calls)
	}
}

// No row yet is the normal state while the video is still uploading, and it
// has to be distinguishable from a broken answer.
func TestSessionDateReportsAMissingRow(t *testing.T) {
	lookup := New("db", &test_utils.MockBinder{Data: []byte(`[]`)})

	if _, err := lookup.SessionDate(context.Background(), 22, 1); !errors.Is(err, ErrSessionNotReady) {
		t.Fatalf("expected ErrSessionNotReady, got %v", err)
	}
}

func TestSessionDateRejectsAnUnparseableTimestamp(t *testing.T) {
	lookup := New("db", &test_utils.MockBinder{Data: []byte(`[["18/02/2023"]]`)})

	if _, err := lookup.SessionDate(context.Background(), 22, 1); err == nil {
		t.Fatal("expected an error for a non-RFC3339 timestamp")
	}
}
