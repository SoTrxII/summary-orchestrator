package audio_store

import (
	"context"
	"encoding/base64"
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

// Pandora's roster is not an object but the inside of one: it appends a line
// per user as they join, so the file has no enclosing braces. Parsing it as-is
// fails, and a failure here silently means "no ceiling" — which is exactly the
// unbounded diarization this is meant to fix.
func TestSpeakerCountReadsPandorasRoster(t *testing.T) {
	roster := `"0":{}
,"1":{"id":"152844030097620992","name":"asriel1727","discrim":"0"}
,"2":{"id":"155371393297416193","name":"frozegg","discrim":"0"}
,"3":{"id":"188626510901542912","name":"shrecki","discrim":"0"}
`
	binder := &test_utils.MockBinder{Data: []byte(roster)}
	store := New("object-store", "6h", binder)

	// Entry 0 is a placeholder carrying no id, not a fourth person.
	people, err := store.SpeakerCount(context.Background(), "a.ogg")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if people != 3 {
		t.Fatalf("expected 3 speakers, got %d", people)
	}
}

func TestSpeakerCountLooksBesideTheRecording(t *testing.T) {
	binder := &test_utils.MockBinder{Data: []byte(`"1":{"id":"1"}`)}
	store := New("object-store", "6h", binder)

	if _, err := store.SpeakerCount(context.Background(), "22/1.ogg"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(binder.Calls) != 1 {
		t.Fatalf("expected one call, got %d", len(binder.Calls))
	}
	if got := binder.Calls[0].Metadata["key"]; got != "22/1.ogg.users" {
		t.Fatalf("read %q, want the roster beside the recording", got)
	}
	if got := binder.Calls[0].Operation; got != "get" {
		t.Fatalf("operation = %q, want get", got)
	}
}

// Anything unreadable has to come back as "no idea" rather than as zero
// speakers: the caller turns 0 into an omitted ceiling, and any other number
// into a hard cap on how many voices may be found.
func TestSpeakerCountTreatsNonsenseAsUnknown(t *testing.T) {
	for name, body := range map[string]string{
		"empty":       "",
		"garbage":     "not json at all",
		"no ids":      `"0":{}`,
		"real object": `{"1":{"id":"1"},"2":{"id":"2"}}`,
	} {
		binder := &test_utils.MockBinder{Data: []byte(body)}
		people, err := New("object-store", "6h", binder).SpeakerCount(context.Background(), "a.ogg")
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", name, err)
		}
		want := 0
		if name == "real object" {
			want = 2 // a tidied-up format must still be read
		}
		if people != want {
			t.Errorf("%s: got %d speakers, want %d", name, people, want)
		}
	}
}

// The recordings binding sets encodeBase64, so a get hands back base64 rather
// than the bytes that were stored. Parsing that as a roster yields no ids and
// no ceiling — the bound would silently never be applied.
func TestSpeakerCountDecodesBase64Payloads(t *testing.T) {
	roster := `"0":{}
,"1":{"id":"1","name":"a"}
,"2":{"id":"2","name":"b"}
`
	encoded := base64.StdEncoding.EncodeToString([]byte(roster))
	binder := &test_utils.MockBinder{Data: []byte(encoded)}

	people, err := New("object-store", "6h", binder).SpeakerCount(context.Background(), "a.ogg")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if people != 2 {
		t.Fatalf("expected 2 speakers through base64, got %d", people)
	}
}
