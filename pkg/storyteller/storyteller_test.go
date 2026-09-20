package storyteller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// agentServing stands in for the agent answering JSON-RPC directly, which is
// how it is reached in the cluster. It records the last request so tests can
// inspect the envelope sent.
func agentServing(t *testing.T, rpcBody string) (*A2A, *http.Request, *string) {
	t.Helper()
	var lastBody string
	lastReq := &http.Request{Header: http.Header{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		lastBody = string(raw)
		lastReq.Header = r.Header.Clone()
		_, _ = w.Write([]byte(rpcBody))
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "key", "Storyteller", false, srv.Client()), lastReq, &lastBody
}

func TestReadsArtifactText(t *testing.T) {
	agent, _, _ := agentServing(t, `{"jsonrpc":"2.0","id":"1","result":{"task":{
		"status":{"state":"TASK_STATE_COMPLETED"},
		"artifacts":[{"parts":[{"text":"Chapitre premier."}]}]}}}`)

	got, err := agent.SummarizeEpisode(context.Background(), "14", "4", []Line{{Speaker: "Alice", Text: "Bonjour"}}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "Chapitre premier." {
		t.Fatalf("got %q", got)
	}
}

// The agent may answer with a bare message rather than a task artifact.
func TestFallsBackToMessageParts(t *testing.T) {
	agent, _, _ := agentServing(t, `{"jsonrpc":"2.0","id":"1","result":{"message":{
		"role":"ROLE_AGENT","parts":[{"text":"Un résumé."}]}}}`)

	got, err := agent.SummarizeCampaign(context.Background(), "14", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "Un résumé." {
		t.Fatalf("got %q", got)
	}
}

// A failed task carries its reason in the status message, not in an artifact.
// An empty answer must never be stored as a summary, and the reason is the
// only place the real cause shows up, so it has to reach the caller.
func TestFailedTaskReasonIsSurfaced(t *testing.T) {
	agent, _, _ := agentServing(t, `{"jsonrpc":"2.0","id":"1","result":{"task":{
		"status":{"state":"TASK_STATE_FAILED","message":{"parts":[{"text":"Erreur : 401 unauthorized"}]}},
		"artifacts":[]}}}`)

	_, err := agent.SummarizeCampaign(context.Background(), "14", "")
	if err == nil {
		t.Fatal("expected an error when the agent returns nothing")
	}
	if !strings.Contains(err.Error(), "401 unauthorized") {
		t.Fatalf("expected the agent's own reason, got %v", err)
	}
}

// A task that fails without saying why still has to be an error.
func TestEmptyAnswerIsAnError(t *testing.T) {
	agent, _, _ := agentServing(t, `{"jsonrpc":"2.0","id":"1","result":{"task":{
		"status":{"state":"TASK_STATE_FAILED"},"artifacts":[]}}}`)

	if _, err := agent.SummarizeCampaign(context.Background(), "14", ""); err == nil {
		t.Fatal("expected an error when the agent returns nothing")
	}
}

func TestJsonRpcErrorIsSurfaced(t *testing.T) {
	agent, _, _ := agentServing(t, `{"jsonrpc":"2.0","id":"1","error":{"code":-32603,"message":"boom"}}`)

	if _, err := agent.SummarizeCampaign(context.Background(), "14", ""); err == nil ||
		!strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected the agent's own error, got %v", err)
	}
}

// A2A 1.0 names methods after the gRPC service and carries the version in a
// header. Getting either wrong is rejected outright: the 0.3 spelling answers
// "Method not found", and a missing header answers "version 0.3 not supported".
func TestSpeaksProtocolV1(t *testing.T) {
	agent, req, body := agentServing(t, `{"result":{"task":{"artifacts":[{"parts":[{"text":"ok"}]}]}}}`)

	if _, err := agent.SummarizeCampaign(context.Background(), "14", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := req.Header.Get("A2A-Version"); got != "1.0" {
		t.Fatalf("A2A-Version header is %q", got)
	}
	for _, want := range []string{`"method":"SendMessage"`, `"role":"ROLE_USER"`} {
		if !strings.Contains(*body, want) {
			t.Fatalf("envelope is missing %s, got %s", want, *body)
		}
	}
	// Protojson has no part discriminator; sending one fails to parse.
	if strings.Contains(*body, `"kind"`) {
		t.Fatalf("envelope should carry no kind discriminator, got %s", *body)
	}
}

// The task header is how the agent picks a skill, so the envelope has to be
// exactly what its parser expects.
func TestEpisodeEnvelope(t *testing.T) {
	agent, _, body := agentServing(t, `{"jsonrpc":"2.0","id":"1","result":{"task":{
		"artifacts":[{"parts":[{"text":"ok"}]}]}}}`)

	if _, err := agent.SummarizeEpisode(context.Background(), "14", "04", []Line{{Speaker: "Alice", Text: "Bonjour"}}, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{
		`Tâche: episode`,
		`Campagne: 14`,
		`Episode: 04`,
		`[CONTENU]`,
		`{\"speaker\":\"Alice\",\"text\":\"Bonjour\"}`,
	} {
		if !strings.Contains(*body, want) {
			t.Fatalf("envelope is missing %q, got %s", want, *body)
		}
	}
}

// paths records every path the client touches, so a test can assert on what
// it did *not* call as well as what it did.
func recordingAgent(t *testing.T, viaGateway bool) (*A2A, *[]string) {
	t.Helper()
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if strings.HasSuffix(r.URL.Path, "/v1/agents") {
			_, _ = w.Write([]byte(`[{"agent_id":"uuid-1","agent_name":"Storyteller"}]`))
			return
		}
		_, _ = w.Write([]byte(`{"result":{"task":{"artifacts":[{"parts":[{"text":"ok"}]}]}}}`))
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "key", "Storyteller", viaGateway, srv.Client()), &paths
}

// In the cluster the agent is reached directly: it serves JSON-RPC on "/",
// and there is no proxy to ask for an id.
func TestDirectModePostsToRoot(t *testing.T) {
	agent, paths := recordingAgent(t, false)
	if _, err := agent.SummarizeCampaign(context.Background(), "14", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(*paths) != 1 || (*paths)[0] != "/" {
		t.Fatalf("expected a single POST to /, got %v", *paths)
	}
}

// Through the proxy, LiteLLM routes on the uuid it assigned rather than the
// agent name, so the name has to be resolved first.
func TestGatewayModeResolvesAgentId(t *testing.T) {
	agent, paths := recordingAgent(t, true)
	if _, err := agent.SummarizeCampaign(context.Background(), "14", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(*paths) != 2 || (*paths)[0] != "/v1/agents" || (*paths)[1] != "/a2a/uuid-1" {
		t.Fatalf("expected a lookup then /a2a/uuid-1, got %v", *paths)
	}
}

// The cast travels in its own section ahead of [CONTENU], because the agent
// reads the content as "everything after that marker". Putting it anywhere else
// would fold the cast into the transcript.
func TestCastRidesAheadOfTheTranscript(t *testing.T) {
	agent, _, body := agentServing(t, okTask)
	cast := `{"characters":[{"name":"Kit"}],"participants":["Lucas"]}`

	if _, err := agent.SummarizeEpisode(context.Background(), "22", "1",
		[]Line{{Speaker: "Alice", Text: "Bonjour"}}, cast); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sent := *body
	castAt := strings.Index(sent, "[CASTING]")
	contentAt := strings.Index(sent, "[CONTENU]")
	if castAt < 0 {
		t.Fatalf("no cast section in %q", sent)
	}
	if castAt > contentAt {
		t.Fatalf("cast section came after the transcript")
	}
	if !strings.Contains(sent, `participants`) {
		t.Errorf("cast body not sent: %q", sent)
	}
}

// Every campaign that nobody has curated goes down this path, so it has to
// leave the message exactly as it was before the cast existed.
func TestNoCastLeavesTheMessageUnchanged(t *testing.T) {
	agent, _, body := agentServing(t, okTask)

	if _, err := agent.SummarizeEpisode(context.Background(), "22", "1",
		[]Line{{Speaker: "Alice", Text: "Bonjour"}}, "   "); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(*body, "CASTING") {
		t.Fatalf("emitted an empty cast section: %q", *body)
	}
}

func TestCampaignSummaryCarriesTheCastToo(t *testing.T) {
	agent, _, body := agentServing(t, okTask)

	if _, err := agent.SummarizeCampaign(context.Background(), "22",
		`{"participants":["Lucas"]}`); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(*body, "[CASTING]") {
		t.Fatalf("campaign message lost the cast: %q", *body)
	}
}

const okTask = `{"jsonrpc":"2.0","id":"1","result":{"task":{
	"status":{"state":"TASK_STATE_COMPLETED"},
	"artifacts":[{"parts":[{"text":"Chapitre premier."}]}]}}}`
