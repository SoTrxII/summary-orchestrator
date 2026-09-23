package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dapr/durabletask-go/workflow"
	"github.com/gin-gonic/gin"
)

// mockScheduler records what the handler tried to schedule.
type mockScheduler struct {
	scheduled bool
	input     map[string]any
}

func (m *mockScheduler) ScheduleWorkflow(_ context.Context, _ string, opts ...workflow.NewWorkflowOptions) (string, error) {
	m.scheduled = true
	// The options are opaque, so the input is read back off the built options
	// rather than inspected directly; only that a run was scheduled matters here.
	return "id", nil
}

func (m *mockScheduler) FetchWorkflowMetadata(_ context.Context, _ string, _ ...workflow.FetchWorkflowMetadataOptions) (*workflow.WorkflowMetadata, error) {
	return nil, nil
}

func post(t *testing.T, body string) (*httptest.ResponseRecorder, *mockScheduler) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	sched := &mockScheduler{}
	r := gin.New()
	NewController(sched).Register(r)

	req := httptest.NewRequest(http.MethodPost, "/workflows/episode", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w, sched
}

// An episode summarized from an archive names no recording -- and for the
// legacy episodes whose audio has been pruned from the bucket there is no
// recording left to name. Rejecting those here would make the transcriptKey
// path unreachable, which is how this was first caught: the workflow accepted
// it and the HTTP layer in front of it did not.
func TestStartEpisodeAcceptsATranscriptKeyWithoutAudio(t *testing.T) {
	w, sched := post(t, `{"jobId":"j1","campaignId":28,"episodeId":7,
		"transcriptKey":"28/7/transcript.json"}`)

	if w.Code != http.StatusAccepted {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}
	if !sched.scheduled {
		t.Fatal("expected a workflow to be scheduled")
	}
}

func TestStartEpisodeStillAcceptsAudio(t *testing.T) {
	w, sched := post(t, `{"jobId":"j2","campaignId":28,"episodeId":7,"audioKey":"28/7.ogg"}`)

	if w.Code != http.StatusAccepted {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}
	if !sched.scheduled {
		t.Fatal("expected a workflow to be scheduled")
	}
}

// Neither a recording nor a transcript means there is nothing to summarize,
// and scheduling it would burn the job id on a run that cannot succeed.
func TestStartEpisodeRejectsARequestWithNoSource(t *testing.T) {
	w, sched := post(t, `{"jobId":"j3","campaignId":28,"episodeId":7}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}
	if sched.scheduled {
		t.Fatal("nothing may be scheduled without a source")
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unreadable error body: %v", err)
	}
	if !strings.Contains(body["error"], "transcriptKey") {
		t.Errorf("the error should name the option the caller is missing, got %q", body["error"])
	}
}
