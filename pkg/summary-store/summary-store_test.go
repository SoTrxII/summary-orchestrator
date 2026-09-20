package summary_store

import (
	"context"
	"encoding/json"
	"testing"

	"summary-orchestrator/pkg/storyteller"
	test_utils "summary-orchestrator/test-utils"
)

// The transcript is archived so an episode can be summarized again without
// paying whisper twice. That only works if the whisper task ids travel with
// it: they are what the workflow takes as transcriptionIds to skip straight
// to collecting.
func TestSaveTranscriptKeepsWhatMakesItReusable(t *testing.T) {
	binder := &test_utils.MockBinder{}
	store := New("object-store", binder)

	key, err := store.SaveTranscript(context.Background(), Transcript{
		CampaignId:       "22",
		EpisodeId:        "8",
		AudioKeys:        []string{"a.ogg", "b.ogg"},
		TranscriptionIds: []string{"task-a", "task-b"},
		Lines: []storyteller.Line{
			{Speaker: "SPEAKER_00", Text: "On entre dans la taverne."},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "campaigns/22/episodes/8/transcript.json" {
		t.Fatalf("stored at %q", key)
	}
	if len(binder.Calls) != 1 {
		t.Fatalf("expected one call, got %d", len(binder.Calls))
	}
	if got := binder.Calls[0].Metadata["key"]; got != key {
		t.Fatalf("wrote to %q but reported %q", got, key)
	}

	var stored Transcript
	if err := json.Unmarshal(binder.Calls[0].Data, &stored); err != nil {
		t.Fatalf("unreadable payload: %v", err)
	}
	if len(stored.TranscriptionIds) != 2 || stored.TranscriptionIds[0] != "task-a" {
		t.Errorf("task ids lost: %v", stored.TranscriptionIds)
	}
	if len(stored.AudioKeys) != 2 {
		t.Errorf("audio keys lost: %v", stored.AudioKeys)
	}
	if len(stored.Lines) != 1 || stored.Lines[0].Speaker != "SPEAKER_00" {
		t.Errorf("dialogue lost: %v", stored.Lines)
	}
}

// A transcript sits beside the summary written from it rather than in a
// bucket of its own, so everything known about an episode is under one prefix.
func TestTranscriptSitsBesideItsSummary(t *testing.T) {
	if got, want := TranscriptKey("22", "8"), "campaigns/22/episodes/8/transcript.json"; got != want {
		t.Fatalf("TranscriptKey = %q, want %q", got, want)
	}
	if got, want := EpisodeKey("22", "8"), "campaigns/22/episodes/8/summary.json"; got != want {
		t.Fatalf("EpisodeKey = %q, want %q", got, want)
	}
}

// A curated cast is what stops the summarizer inventing one. It is optional
// though, and every way of not having one has to cost an episode nothing: a
// campaign nobody has curated, a bucket that 404s, and a file somebody is
// halfway through editing by hand all have to read as "no cast".
func TestLoadCastDegradesRatherThanFailing(t *testing.T) {
	cast := `{"characters":[{"name":"Kit"}],"participants":["Lucas"]}`

	for _, tc := range []struct {
		name string
		data []byte
		err  error
		want string
	}{
		{"a curated campaign", []byte(cast), nil, cast},
		{"no cast file at all", nil, errNotFound{}, ""},
		{"a file broken mid-edit", []byte(`{"characters":[{"name":`), nil, ""},
		{"an empty object read back", []byte(""), nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := New("summaries", &test_utils.MockBinder{Data: tc.data, Err: tc.err})
			if got := store.LoadCast(context.Background(), "22"); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLoadCastReadsTheCampaignsOwnKey(t *testing.T) {
	binder := &test_utils.MockBinder{Data: []byte(`{}`)}
	New("summaries", binder).LoadCast(context.Background(), "22")

	if len(binder.Calls) != 1 {
		t.Fatalf("expected one call, got %d", len(binder.Calls))
	}
	if got := binder.Calls[0].Metadata["key"]; got != "campaigns/22/cast.json" {
		t.Fatalf("read %q", got)
	}
	if got := binder.Calls[0].Operation; got != "get" {
		t.Fatalf("operation %q", got)
	}
}

type errNotFound struct{}

func (errNotFound) Error() string { return "the key does not exist" }
