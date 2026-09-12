package workflows

import (
	"context"
	"encoding/json"
	"errors"
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
