package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"summary-orchestrator/pkg/speakers"
	"summary-orchestrator/pkg/storyteller"
	"summary-orchestrator/pkg/transcriber"
	"testing"
	"time"

	session_lookup "summary-orchestrator/pkg/session-lookup"
	summary_store "summary-orchestrator/pkg/summary-store"
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
	// Separate again: an object can be gone from the bucket while its
	// transcript and its whisper task both survive, which is the normal state
	// of every campaign old enough to have been pruned.
	recordedAt    time.Time
	recordedAtErr error
	dated         []string
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

func (m *mockAudioStore) RecordedAt(_ context.Context, key string) (time.Time, error) {
	m.dated = append(m.dated, key)
	return m.recordedAt, m.recordedAtErr
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

// mockAgent records what reached the A2A client.
type mockAgent struct {
	recordedAt string
}

func (m *mockAgent) SummarizeEpisode(_ context.Context, _, _ string, _ []storyteller.Line, _, recordedAt string) (string, error) {
	m.recordedAt = recordedAt
	return "Chapitre.", nil
}

func (m *mockAgent) SummarizeCampaign(_ context.Context, _, _ string) (string, error) {
	return "Récit.", nil
}

func (m *mockAgent) Gist(_ context.Context, _, _, _ string) (string, error) { return "Gist.", nil }

// mockSummaries supplies the cast the activity fetches at the point of use.
type mockSummaries struct{ summary_store.SummaryStore }

func (m *mockSummaries) LoadCast(_ context.Context, _ string) string { return "" }

// An episode dated from its recording is an episode the knowledge graph can
// order. Without this every fact carries the moment of ingestion instead, and
// a campaign rebuilt today has eight episodes stamped minutes apart.
func TestWriteEpisodeSummaryDatesTheEpisodeFromItsRecording(t *testing.T) {
	previous := deps
	agent := &mockAgent{}
	audio := &mockAudioStore{recordedAt: time.Date(2026, 1, 5, 22, 5, 18, 0, time.UTC)}
	deps = Deps{Audio: audio, Agent: agent, Summaries: &mockSummaries{},
		Sessions: &test_utils.MockSessionLookup{}}
	t.Cleanup(func() { deps = previous })

	_, err := WriteEpisodeSummary(&fakeActivityContext{input: episodeSummaryRequest{
		CampaignId: "22",
		EpisodeId:  "4",
		AudioKeys:  []string{"155111112.ogg", "155111113.ogg"},
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent.recordedAt != "2026-01-05T22:05:18Z" {
		t.Fatalf("got %q", agent.recordedAt)
	}
	// The first chunk, not the last: a session that dropped and resumed belongs
	// at the moment it started.
	if len(audio.dated) != 1 || audio.dated[0] != "155111112.ogg" {
		t.Fatalf("dated %v", audio.dated)
	}
}

// The object is gone, or was never there because the caller submitted task ids
// only. This step runs under noRetry, so failing here would cost the episode
// its chapter over a field that every previously stored episode also lacks.
func TestWriteEpisodeSummarySurvivesAnUndatableRecording(t *testing.T) {
	previous := deps
	agent := &mockAgent{}
	deps = Deps{
		Audio:     &mockAudioStore{recordedAtErr: errors.New("not in the bucket")},
		Agent:     agent,
		Summaries: &mockSummaries{},
		Sessions:  &test_utils.MockSessionLookup{DateErr: session_lookup.ErrSessionNotReady},
	}
	t.Cleanup(func() { deps = previous })

	got, err := WriteEpisodeSummary(&fakeActivityContext{input: episodeSummaryRequest{
		CampaignId: "22", EpisodeId: "4", AudioKeys: []string{"gone.ogg"},
	}})
	if err != nil {
		t.Fatalf("an undatable recording must not fail the chapter: %v", err)
	}
	if got != "Chapitre." {
		t.Fatalf("got %v", got)
	}
	if agent.recordedAt != "" {
		t.Fatalf("expected no date, got %q", agent.recordedAt)
	}
}

func TestWriteEpisodeSummaryWithNoKeysDoesNotAskForADate(t *testing.T) {
	previous := deps
	audio := &mockAudioStore{}
	deps = Deps{Audio: audio, Agent: &mockAgent{}, Summaries: &mockSummaries{},
		Sessions: &test_utils.MockSessionLookup{DateErr: session_lookup.ErrSessionNotReady}}
	t.Cleanup(func() { deps = previous })

	if _, err := WriteEpisodeSummary(&fakeActivityContext{input: episodeSummaryRequest{
		CampaignId: "22", EpisodeId: "4",
	}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(audio.dated) != 0 {
		t.Fatalf("asked to date %v with no keys", audio.dated)
	}
}

// Campaign 22's first two episodes: recorded in 2023, summarized now, and their
// audio long since pruned from the bucket. Undated they would be stamped at
// ingestion -- i.e. today -- which puts the campaign's oldest sessions after its
// newest ones and hands every contradiction to the wrong episode. The session
// row still carries the play date someone typed during the backfill.
func TestWriteEpisodeSummaryFallsBackToTheSessionRow(t *testing.T) {
	previous := deps
	agent := &mockAgent{}
	sessions := &test_utils.MockSessionLookup{
		Date: time.Date(2023, 2, 18, 0, 0, 0, 0, time.UTC),
	}
	deps = Deps{
		Audio:     &mockAudioStore{recordedAtErr: errors.New("not in the bucket")},
		Agent:     agent,
		Summaries: &mockSummaries{},
		Sessions:  sessions,
	}
	t.Cleanup(func() { deps = previous })

	if _, err := WriteEpisodeSummary(&fakeActivityContext{input: episodeSummaryRequest{
		CampaignId: "22", EpisodeId: "1", AudioKeys: []string{"1.ogg"},
	}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent.recordedAt != "2023-02-18T00:00:00Z" {
		t.Fatalf("got %q", agent.recordedAt)
	}
	if len(sessions.DateArgs) != 1 || sessions.DateArgs[0] != [2]int{22, 1} {
		t.Fatalf("looked up %v", sessions.DateArgs)
	}
}

// The bucket is the trustworthy source when it can answer, so the database is
// not consulted at all -- createdAt trails the recording and for a live episode
// the row may not even exist yet.
func TestWriteEpisodeSummaryPrefersTheBucketOverTheDatabase(t *testing.T) {
	previous := deps
	agent := &mockAgent{}
	sessions := &test_utils.MockSessionLookup{
		Date: time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	deps = Deps{
		Audio:     &mockAudioStore{recordedAt: time.Date(2026, 1, 5, 22, 5, 18, 0, time.UTC)},
		Agent:     agent,
		Summaries: &mockSummaries{},
		Sessions:  sessions,
	}
	t.Cleanup(func() { deps = previous })

	if _, err := WriteEpisodeSummary(&fakeActivityContext{input: episodeSummaryRequest{
		CampaignId: "22", EpisodeId: "4", AudioKeys: []string{"155111112.ogg"},
	}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent.recordedAt != "2026-01-05T22:05:18Z" {
		t.Fatalf("got %q", agent.recordedAt)
	}
	if len(sessions.DateArgs) != 0 {
		t.Fatalf("asked the database despite having the object: %v", sessions.DateArgs)
	}
}

// The hand-set override is for the case where the operator knows something
// neither lookup does, so it beats both without consulting either.
func TestWriteEpisodeSummaryHonoursTheCallerSuppliedDate(t *testing.T) {
	previous := deps
	agent := &mockAgent{}
	audio := &mockAudioStore{recordedAt: time.Date(2026, 1, 5, 22, 5, 18, 0, time.UTC)}
	sessions := &test_utils.MockSessionLookup{}
	deps = Deps{Audio: audio, Agent: agent, Summaries: &mockSummaries{}, Sessions: sessions}
	t.Cleanup(func() { deps = previous })

	if _, err := WriteEpisodeSummary(&fakeActivityContext{input: episodeSummaryRequest{
		CampaignId: "22", EpisodeId: "1",
		AudioKeys:  []string{"155111112.ogg"},
		RecordedAt: "2023-02-18T00:00:00Z",
	}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent.recordedAt != "2023-02-18T00:00:00Z" {
		t.Fatalf("got %q", agent.recordedAt)
	}
	if len(audio.dated) != 0 || len(sessions.DateArgs) != 0 {
		t.Fatalf("consulted a lookup despite being told the date")
	}
}
