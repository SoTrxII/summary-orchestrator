package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"summary-orchestrator/pkg/speakers"
	"summary-orchestrator/pkg/storyteller"
	"summary-orchestrator/pkg/transcriber"
	"testing"

	session_lookup "summary-orchestrator/pkg/session-lookup"
	test_utils "summary-orchestrator/test-utils"

	"github.com/dapr/durabletask-go/workflow"
)

// fakeActivityContext supplies the two methods an activity actually uses. The
// embedded interface covers the rest of the surface, which never gets called.
type fakeActivityContext struct {
	workflow.ActivityContext
	input any
}

func (f *fakeActivityContext) GetInput(into any) error {
	raw, err := json.Marshal(f.input)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
}

func (f *fakeActivityContext) Context() context.Context { return context.Background() }

// A retry decision is made on the far side of the runtime boundary, where the
// error has been flattened to its message. If the marker did not survive that
// round trip, every wait-and-see step would fail on its first attempt.
func TestRetryableSurvivesSerialization(t *testing.T) {
	original := retryable(errors.New("session row not created yet"))
	revived := errors.New(original.Error())

	if !isRetryable(revived) {
		t.Fatalf("marker lost through serialization: %q", revived.Error())
	}
	if isRetryable(errors.New("unsupported codec")) {
		t.Fatal("an ordinary failure must not be retried")
	}
	if isRetryable(nil) {
		t.Fatal("no error is not a retryable error")
	}
}

func withDeps(t *testing.T, sessions *test_utils.MockSessionLookup, publisher *test_utils.MockPublisher) {
	t.Helper()
	previous := deps
	deps = Deps{
		Sessions:   sessions,
		Publisher:  publisher,
		PubSubName: "pubsub",
		GistTopic:  "gists",
	}
	t.Cleanup(func() { deps = previous })
}

func publishInput() publishRequest {
	return publishRequest{
		CampaignId:  14,
		EpisodeId:   4,
		Summary:     "Les héros entrent dans la taverne.",
		EpisodeKey:  "campaigns/14/episodes/4/summary.json",
		CampaignKey: "campaigns/14/summary.json",
	}
}

// The summary is usually ready before the video finishes uploading, so the
// session row it attaches to does not exist yet. That has to read as "wait",
// not as "fail".
func TestPublishGistWaitsForTheSessionRow(t *testing.T) {
	sessions := &test_utils.MockSessionLookup{Err: session_lookup.ErrSessionNotReady}
	publisher := &test_utils.MockPublisher{}
	withDeps(t, sessions, publisher)

	_, err := PublishGist(&fakeActivityContext{input: publishInput()})
	if !isRetryable(err) {
		t.Fatalf("expected a retryable error, got %v", err)
	}
	if len(publisher.Events) != 0 {
		t.Fatal("nothing may be published before the session exists")
	}
}

// Velvet numbers episodes by counting existing sessions; the database numbers
// them by incrementing the highest. Delete a session and those disagree. The
// gist is keyed on the database's answer, so that is the one published.
func TestPublishGistUsesTheDatabaseSessionNumber(t *testing.T) {
	sessions := &test_utils.MockSessionLookup{Number: 3}
	publisher := &test_utils.MockPublisher{}
	withDeps(t, sessions, publisher)

	if _, err := PublishGist(&fakeActivityContext{input: publishInput()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(publisher.Events) != 1 {
		t.Fatalf("expected exactly one event, got %d", len(publisher.Events))
	}

	event := publisher.Events[0]
	if event.PubSub != "pubsub" || event.Topic != "gists" {
		t.Fatalf("published to %s/%s", event.PubSub, event.Topic)
	}
	gist, ok := event.Data.(Gist)
	if !ok {
		t.Fatalf("expected a Gist, got %T", event.Data)
	}
	if gist.EpisodeId != 3 {
		t.Fatalf("expected the database's session number 3, got %d", gist.EpisodeId)
	}
	if gist.CampaignId != 14 || gist.Summary == "" || gist.EpisodeKey == "" || gist.CampaignKey == "" {
		t.Fatalf("summary-updater requires every field, got %+v", gist)
	}
}

// A sidecar blip is worth waiting out too, otherwise a finished summary is
// thrown away over a momentary failure.
func TestPublishGistRetriesOnPublishFailure(t *testing.T) {
	withDeps(t, &test_utils.MockSessionLookup{Number: 4}, &test_utils.MockPublisher{Err: errors.New("sidecar down")})

	_, err := PublishGist(&fakeActivityContext{input: publishInput()})
	if !isRetryable(err) {
		t.Fatalf("expected a retryable error, got %v", err)
	}
}

// mockAudioStore stands in for the recordings bucket.
type mockAudioStore struct {
	people   int
	err      error
	timeline speakers.Timeline
	// Separate from err: a recording can have a roster and no timeline, which
	// is every recording cooked before cooking-server started writing one.
	timelineErr error
}

func (m *mockAudioStore) PresignedUrl(_ context.Context, key string) (string, error) {
	return "http://minio/" + key, nil
}

func (m *mockAudioStore) SpeakerCount(_ context.Context, _ string) (int, error) {
	return m.people, m.err
}

func (m *mockAudioStore) SpeakerTimeline(_ context.Context, _ string) (speakers.Timeline, error) {
	return m.timeline, m.timelineErr
}

// Recordings archived before Pandora wrote a roster have none, and older
// campaigns are full of them. Failing the step would cost the whole
// transcription over a hint; it degrades to an unbounded run instead.
func TestCountSpeakersSurvivesAMissingRoster(t *testing.T) {
	previous := deps
	deps = Deps{Audio: &mockAudioStore{err: errors.New("NoSuchKey")}}
	t.Cleanup(func() { deps = previous })

	got, err := CountSpeakers(&fakeActivityContext{input: presignRequest{Key: "22/1.ogg"}})
	if err != nil {
		t.Fatalf("a missing roster must not fail the workflow: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0 (unknown), got %v", got)
	}
}

func TestCountSpeakersReportsTheRoster(t *testing.T) {
	previous := deps
	deps = Deps{Audio: &mockAudioStore{people: 7}}
	t.Cleanup(func() { deps = previous })

	got, err := CountSpeakers(&fakeActivityContext{input: presignRequest{Key: "a.ogg"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 7 {
		t.Fatalf("expected 7, got %v", got)
	}
}

// mockTranscriber returns a fixed diarized transcript.
type mockTranscriber struct {
	segments []transcriber.Segment
}

func (m *mockTranscriber) Start(_ context.Context, _ string, _ int) (string, error) {
	return "task", nil
}

func (m *mockTranscriber) Result(_ context.Context, _ string) ([]transcriber.Segment, error) {
	return m.segments, nil
}

func diarized() []transcriber.Segment {
	return []transcriber.Segment{
		{Start: 1, End: 4, Speaker: "SPEAKER_00", Text: "On entre dans la crypte."},
		{Start: 12, End: 15, Speaker: "SPEAKER_01", Text: "Je dégaine."},
	}
}

// The whole point of the timeline: a diarized cluster is unusable downstream,
// a Discord account is not. The GM has to be marked, because that is what
// separates a player character from an NPC.
func TestFetchTranscriptionNamesSpeakersFromTheTimeline(t *testing.T) {
	previous := deps
	deps = Deps{
		Transcriber: &mockTranscriber{segments: diarized()},
		Audio: &mockAudioStore{timeline: speakers.Timeline{Tracks: []speakers.Track{
			{Track: 1, UserId: "188626510901542912", Username: "shrecki",
				Intervals: []speakers.Interval{{Start: 0, End: 5}}},
			{Track: 2, UserId: "152795063859150848", Username: "sotrx",
				Intervals: []speakers.Interval{{Start: 11, End: 16}}},
		}}},
	}
	t.Cleanup(func() { deps = previous })

	var lines []storyteller.Line
	got, err := FetchTranscription(&fakeActivityContext{
		input: transcriptionResult{
			TaskId:        "task",
			AudioKey:      "1.ogg",
			GameMasterIds: []string{"188626510901542912"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines = got.([]storyteller.Line)

	if lines[0].Speaker != "shrecki (MJ)" {
		t.Errorf("expected the game master to be marked, got %q", lines[0].Speaker)
	}
	if lines[1].Speaker != "sotrx" {
		t.Errorf("expected a named player, got %q", lines[1].Speaker)
	}
}

// Every recording cooked before the timeline existed has none, and the raw
// records it is derived from are long deleted. Those must still transcribe.
func TestFetchTranscriptionFallsBackToDiarization(t *testing.T) {
	previous := deps
	deps = Deps{
		Transcriber: &mockTranscriber{segments: diarized()},
		Audio:       &mockAudioStore{timelineErr: errors.New("NoSuchKey")},
	}
	t.Cleanup(func() { deps = previous })

	got, err := FetchTranscription(&fakeActivityContext{
		input: transcriptionResult{TaskId: "task", AudioKey: "22/1.ogg"},
	})
	if err != nil {
		t.Fatalf("a missing timeline must not fail the transcription: %v", err)
	}
	lines := got.([]storyteller.Line)

	if lines[0].Speaker != "SPEAKER_00" || lines[1].Speaker != "SPEAKER_01" {
		t.Errorf("expected the diarized labels to survive, got %q and %q",
			lines[0].Speaker, lines[1].Speaker)
	}
}
