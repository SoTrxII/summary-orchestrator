package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	summary_store "summary-orchestrator/pkg/summary-store"

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

// mockSummaries serves one stored summary, or one error, and records which
// one it was asked for.
type mockSummaries struct {
	stored summary_store.Summary
	err    error
	asked  string
}

func (m *mockSummaries) LoadEpisode(_ context.Context, campaignId, episodeId string) (summary_store.Summary, error) {
	m.asked = campaignId + "/" + episodeId
	return m.stored, m.err
}

func (m *mockSummaries) LoadCampaign(_ context.Context, campaignId string) (summary_store.Summary, error) {
	m.asked = campaignId
	return m.stored, m.err
}

func get(t *testing.T, path string, summaries *mockSummaries) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	NewController(&mockScheduler{}, summaries).Register(r)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func post(t *testing.T, body string) (*httptest.ResponseRecorder, *mockScheduler) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	sched := &mockScheduler{}
	r := gin.New()
	NewController(sched, &mockSummaries{}).Register(r)

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

// The body is the stored object as is, so that what a reader of the bucket
// and a caller of the route see cannot drift apart.
func TestSummaryRoutesServeWhatIsStored(t *testing.T) {
	for _, tc := range []struct {
		path   string
		stored summary_store.Summary
		asked  string
		body   string
	}{
		{"/campaigns/20/episodes/3/summary",
			summary_store.Summary{CampaignId: "20", EpisodeId: "3", Summary: "Le convoi quitte la ville."},
			"20/3", `{"campaignId":"20","episodeId":"3","summary":"Le convoi quitte la ville."}`},
		{"/campaigns/20/summary",
			summary_store.Summary{CampaignId: "20", Summary: "Tout commence à Port-Réal."},
			"20", `{"campaignId":"20","summary":"Tout commence à Port-Réal."}`},
	} {
		summaries := &mockSummaries{stored: tc.stored}
		w := get(t, tc.path, summaries)

		if w.Code != http.StatusOK {
			t.Fatalf("%s: got %d: %s", tc.path, w.Code, w.Body.String())
		}
		if summaries.asked != tc.asked {
			t.Errorf("%s: read %q, want %q", tc.path, summaries.asked, tc.asked)
		}
		if got := w.Body.String(); got != tc.body {
			t.Errorf("%s: got %s", tc.path, got)
		}
	}
}

// Velvet turns these two into different messages: "not summarized yet" must
// not read as an outage, nor an outage as "nothing here".
func TestSummaryRoutesTellNotWrittenFromNotReadable(t *testing.T) {
	missing := fmt.Errorf("campaigns/7/episodes/74/summary.json: %w", summary_store.ErrNoSummary)
	if w := get(t, "/campaigns/7/episodes/74/summary", &mockSummaries{err: missing}); w.Code != http.StatusNotFound {
		t.Fatalf("a summary never written: got %d", w.Code)
	}
	down := errors.New("reading campaigns/20/summary.json: connection refused")
	if w := get(t, "/campaigns/20/summary", &mockSummaries{err: down}); w.Code != http.StatusBadGateway {
		t.Fatalf("an unreadable bucket: got %d", w.Code)
	}
}

// The ids are spliced into a bucket key. Anything that is not a number stops
// here, and a number is read in its plain form so it names the key it was
// written under.
func TestSummaryRoutesOnlyTakeNumbers(t *testing.T) {
	for _, path := range []string{
		"/campaigns/abc/summary",
		"/campaigns/20/episodes/3.json/summary",
		"/campaigns/-1/summary",
	} {
		summaries := &mockSummaries{}
		if w := get(t, path, summaries); w.Code != http.StatusBadRequest {
			t.Errorf("%s: got %d", path, w.Code)
		}
		if summaries.asked != "" {
			t.Errorf("%s: reached the bucket as %q", path, summaries.asked)
		}
	}

	// An encoded slash never gets as far as the check: the router decodes it
	// and finds no route of that shape.
	escaping := &mockSummaries{}
	if w := get(t, "/campaigns/20/episodes/..%2Fcast.json/summary", escaping); w.Code != http.StatusNotFound || escaping.asked != "" {
		t.Errorf("an encoded slash: got %d, reached the bucket as %q", w.Code, escaping.asked)
	}

	summaries := &mockSummaries{stored: summary_store.Summary{Summary: "x"}}
	get(t, "/campaigns/020/episodes/03/summary", summaries)
	if summaries.asked != "20/3" {
		t.Errorf("read %q, want 20/3", summaries.asked)
	}
}
