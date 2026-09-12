package storyteller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// A2A is a Storyteller client speaking A2A JSON-RPC.
//
// Two ways to reach the agent. In-cluster it answers JSON-RPC directly on
// "/", which is the default and needs no discovery. The LiteLLM proxy is the
// other route, for callers outside the cluster; it is opt-in because it also
// requires the agent to be registered in the proxy's agent_list.
type A2A struct {
	endpoint   string
	apiKey     string
	agent      string
	viaGateway bool
	client     *http.Client

	// LiteLLM routes A2A on the uuid it assigned when registering the agent,
	// not on the agent name, and that uuid is regenerated whenever its
	// database is. Resolve it once per process rather than hardcoding it.
	resolveOnce sync.Once
	agentId     string
}

func New(endpoint, apiKey, agent string, viaGateway bool, client *http.Client) *A2A {
	if client == nil {
		client = http.DefaultClient
	}
	return &A2A{
		endpoint:   strings.TrimSuffix(endpoint, "/"),
		apiKey:     apiKey,
		agent:      agent,
		viaGateway: viaGateway,
		client:     client,
	}
}

func (a *A2A) SummarizeEpisode(ctx context.Context, campaignId, episodeId string, lines []Line) (string, error) {
	content, err := json.Marshal(lines)
	if err != nil {
		return "", err
	}
	return a.send(ctx, fmt.Sprintf(
		"Tâche: episode\nCampagne: %s\nEpisode: %s\n[CONTENU]\n%s",
		campaignId, episodeId, content))
}

func (a *A2A) SummarizeCampaign(ctx context.Context, campaignId string) (string, error) {
	return a.send(ctx, fmt.Sprintf("Tâche: campagne\nCampagne: %s", campaignId))
}

// Gist asks for the short form of an already-written episode summary.
//
// NOTE: the agent does not implement a "gist" task yet. Until it does, this
// header falls through to its default branch and returns another full chapter.
func (a *A2A) Gist(ctx context.Context, campaignId, episodeId, summary string) (string, error) {
	return a.send(ctx, fmt.Sprintf(
		"Tâche: gist\nCampagne: %s\nEpisode: %s\n[CONTENU]\n%s",
		campaignId, episodeId, summary))
}

type agentEntry struct {
	AgentId   string `json:"agent_id"`
	AgentName string `json:"agent_name"`
}

// rpcUrl is where message/send is posted.
//
// Talking to the agent directly needs no lookup. Through the proxy, the agent
// name has to be resolved to the id LiteLLM routes on; a failure to look it
// up is not fatal, since the configured value may already be an id.
func (a *A2A) rpcUrl(ctx context.Context) string {
	if !a.viaGateway {
		return a.endpoint + "/"
	}

	a.resolveOnce.Do(func() {
		a.agentId = a.agent

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.endpoint+"/v1/agents", nil)
		if err != nil {
			return
		}
		a.authorize(req)

		res, err := a.client.Do(req)
		if err != nil {
			slog.Warn(fmt.Sprintf("[Storyteller] :: could not list agents, using %q as an id: %s", a.agent, err))
			return
		}
		defer res.Body.Close()

		var entries []agentEntry
		if err := json.NewDecoder(res.Body).Decode(&entries); err != nil {
			slog.Warn(fmt.Sprintf("[Storyteller] :: unreadable agent list, using %q as an id: %s", a.agent, err))
			return
		}
		wanted := strings.ToLower(a.agent)
		for _, e := range entries {
			if wanted == strings.ToLower(e.AgentId) || wanted == strings.ToLower(e.AgentName) {
				a.agentId = e.AgentId
				slog.Info(fmt.Sprintf("[Storyteller] :: resolved agent %q -> %s", a.agent, e.AgentId))
				return
			}
		}
		slog.Warn(fmt.Sprintf("[Storyteller] :: agent %q not found in /v1/agents, using it as an id", a.agent))
	})
	return fmt.Sprintf("%s/a2a/%s", a.endpoint, a.agentId)
}

// A2A 1.0 is protobuf-derived: methods are named after the gRPC service
// methods rather than the "message/send" strings of 0.3, and the version
// travels in a header. Sending the 0.3 spelling to a 1.0 agent gets
// "Method not found"; sending 1.0 method names without the header gets
// "A2A version '0.3' is not supported".
const (
	sendMethod    = "SendMessage"
	versionHeader = "A2A-Version"
	protoVersion  = "1.0"
)

// Protojson, so no "kind" discriminator: the field that is set is the kind.
type part struct {
	Text string `json:"text"`
}

type rpcRequest struct {
	JsonRpc string         `json:"jsonrpc"`
	Id      string         `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

type message struct {
	Parts []part `json:"parts"`
}

type task struct {
	Artifacts []struct {
		Parts []part `json:"parts"`
	} `json:"artifacts"`
	Status struct {
		State string `json:"state"`
		// A failed task explains itself here rather than in an artifact.
		Message *message `json:"message"`
	} `json:"status"`
}

type rpcResponse struct {
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Result struct {
		Task    *task    `json:"task"`
		Message *message `json:"message"`
	} `json:"result"`
}

// send makes one non-streaming SendMessage call. Streaming is deliberately
// not used: the LiteLLM A2A proxy serves it as application/x-ndjson rather
// than text/event-stream, which strict SSE clients reject.
func (a *A2A) send(ctx context.Context, text string) (string, error) {
	body, err := json.Marshal(rpcRequest{
		JsonRpc: "2.0",
		Id:      uuid.NewString(),
		Method:  sendMethod,
		Params: map[string]any{
			"message": map[string]any{
				"role":      "ROLE_USER",
				"messageId": uuid.NewString(),
				"parts":     []part{{Text: text}},
			},
		},
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.rpcUrl(ctx), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(versionHeader, protoVersion)
	a.authorize(req)

	res, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	payload, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("agent returned %d: %s", res.StatusCode, strings.TrimSpace(string(payload)))
	}

	var parsed rpcResponse
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return "", fmt.Errorf("unreadable agent response: %w", err)
	}
	if parsed.Error != nil {
		return "", fmt.Errorf("agent error %d: %s", parsed.Error.Code, parsed.Error.Message)
	}

	var sb strings.Builder
	if t := parsed.Result.Task; t != nil {
		for _, artifact := range t.Artifacts {
			appendParts(&sb, artifact.Parts)
		}
	}
	if sb.Len() == 0 && parsed.Result.Message != nil {
		appendParts(&sb, parsed.Result.Message.Parts)
	}

	out := strings.TrimSpace(sb.String())
	if out == "" {
		// An empty answer is an error, never a summary. A failed task puts the
		// reason in its status message, which is the only place the real cause
		// (a model 401, a Graphiti timeout) ever appears — quote it verbatim.
		return "", fmt.Errorf("agent returned no text: %s", taskFailure(parsed.Result.Task))
	}
	return out, nil
}

// taskFailure renders whatever the agent said about why a task produced nothing.
func taskFailure(t *task) string {
	if t == nil {
		return "no task in response"
	}
	var sb strings.Builder
	if t.Status.Message != nil {
		appendParts(&sb, t.Status.Message.Parts)
	}
	if sb.Len() == 0 {
		return fmt.Sprintf("task state %q", t.Status.State)
	}
	return fmt.Sprintf("task state %q: %s", t.Status.State, strings.TrimSpace(sb.String()))
}

func appendParts(sb *strings.Builder, parts []part) {
	for _, p := range parts {
		if p.Text == "" {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(p.Text)
	}
}

func (a *A2A) authorize(req *http.Request) {
	if a.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
}
