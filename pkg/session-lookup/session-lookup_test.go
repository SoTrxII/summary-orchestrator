package session_lookup

import (
	"context"
	"errors"
	"testing"

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
