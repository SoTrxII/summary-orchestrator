package summary_store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"summary-orchestrator/pkg/storyteller"
	test_utils "summary-orchestrator/test-utils"

	"github.com/dapr/go-sdk/client"
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

// SaveCast is used for both a human's corrected file and the automatic draft;
// it just puts bytes at the campaign's own key either way.
func TestSaveCastWritesTheCampaignsOwnKey(t *testing.T) {
	binder := &test_utils.MockBinder{}
	key, err := New("summaries", binder).SaveCast(context.Background(), "28", `{"characters":[]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "campaigns/28/cast.json" {
		t.Fatalf("got %q", key)
	}
	if len(binder.Calls) != 1 || binder.Calls[0].Operation != "create" {
		t.Fatalf("expected one create call, got %+v", binder.Calls)
	}
	if got := binder.Calls[0].Metadata["key"]; got != key {
		t.Fatalf("wrote to %q", got)
	}
}

// sequencedBinder answers each call from a fixed script, in order -- unlike
// MockBinder, which always returns the same response. LoadTranscripts makes
// one "list" call followed by one "get" per transcript found, and each of
// those needs its own canned answer.
type sequencedBinder struct {
	responses []testBinderResponse
	calls     []*client.InvokeBindingRequest
}

type testBinderResponse struct {
	data []byte
	err  error
}

func (s *sequencedBinder) InvokeBinding(_ context.Context, in *client.InvokeBindingRequest) (*client.BindingEvent, error) {
	i := len(s.calls)
	s.calls = append(s.calls, in)
	if i >= len(s.responses) {
		return nil, fmt.Errorf("no scripted response for call %d", i)
	}
	r := s.responses[i]
	if r.err != nil {
		return nil, r.err
	}
	return &client.BindingEvent{Data: r.data}, nil
}

// Drafting a cast needs every episode's transcript, not just the one just
// written, because the drafter votes across episodes.
func TestLoadTranscriptsReadsEveryArchivedEpisode(t *testing.T) {
	list := `{"Contents":[
		{"Key":"campaigns/28/episodes/1/transcript.json"},
		{"Key":"campaigns/28/episodes/1/summary.json"},
		{"Key":"campaigns/28/episodes/2/transcript.json"}
	]}`
	t1 := `{"campaignId":"28","episodeId":"1","lines":[{"speaker":"A","text":"Bonjour."}]}`
	t2 := `{"campaignId":"28","episodeId":"2","lines":[{"speaker":"B","text":"Salut."}]}`

	binder := &sequencedBinder{responses: []testBinderResponse{
		{data: []byte(list)},
		{data: []byte(t1)},
		{data: []byte(t2)},
	}}
	transcripts, err := New("summaries", binder).LoadTranscripts(context.Background(), "28")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(transcripts) != 2 {
		t.Fatalf("expected 2 transcripts (summary.json excluded), got %d", len(transcripts))
	}
	if transcripts[0].EpisodeId != "1" || transcripts[1].EpisodeId != "2" {
		t.Fatalf("got %+v", transcripts)
	}
	if binder.calls[0].Operation != "list" {
		t.Fatalf("expected the first call to list, got %q", binder.calls[0].Operation)
	}
}

// One corrupt archive must not cost the draft every other episode's evidence.
func TestLoadTranscriptsSkipsAnUnreadableOne(t *testing.T) {
	list := `{"Contents":[
		{"Key":"campaigns/28/episodes/1/transcript.json"},
		{"Key":"campaigns/28/episodes/2/transcript.json"}
	]}`
	binder := &sequencedBinder{responses: []testBinderResponse{
		{data: []byte(list)},
		{err: errNotFound{}},
		{data: []byte(`{"campaignId":"28","episodeId":"2","lines":[]}`)},
	}}
	transcripts, err := New("summaries", binder).LoadTranscripts(context.Background(), "28")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(transcripts) != 1 || transcripts[0].EpisodeId != "2" {
		t.Fatalf("got %+v", transcripts)
	}
}
