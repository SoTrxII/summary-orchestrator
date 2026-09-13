package transcriber

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Config describes the whisperX deployment and how to run a job on it.
//
// Everything here has to be sent on every request. /speech-to-text-url reads
// these from query parameters whose defaults are hardcoded in the API schema
// — "en", "tiny", "cuda", "float16" — and does *not* fall back to the server's
// own DEFAULT_LANG/WHISPER_MODEL/DEVICE/COMPUTE_TYPE environment. Omitting any
// of them silently runs a job the deployment was never configured for: on a
// CPU-only server, omitting Device fails the task outright with "CUDA driver
// version is insufficient for CUDA runtime version".
type Config struct {
	BaseUrl  string
	Token    string
	Language string
	Model    string
	// "cpu" or "cuda".
	Device string
	// Must be "int8" when Device is "cpu"; float16 is GPU-only.
	ComputeType string
}

// WhisperX talks to a whisperX-FastAPI deployment.
type WhisperX struct {
	cfg    Config
	client *http.Client
}

func New(cfg Config, client *http.Client) *WhisperX {
	if client == nil {
		client = http.DefaultClient
	}
	cfg.BaseUrl = strings.TrimSuffix(cfg.BaseUrl, "/")
	return &WhisperX{cfg: cfg, client: client}
}

type startResponse struct {
	Identifier string `json:"identifier"`
	Message    string `json:"message"`
}

func (w *WhisperX) Start(ctx context.Context, audioUrl string) (string, error) {
	// The endpoint takes the audio location as a form field and everything
	// else as query parameters. See Config: none of these may be omitted.
	q := url.Values{}
	q.Set("language", w.cfg.Language)
	q.Set("diarize", "true")
	q.Set("model", w.cfg.Model)
	q.Set("device", w.cfg.Device)
	q.Set("compute_type", w.cfg.ComputeType)

	body := url.Values{}
	body.Set("url", audioUrl)

	endpoint := fmt.Sprintf("%s/speech-to-text-url?%s", w.cfg.BaseUrl, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w.authorize(req)

	var parsed startResponse
	if err := w.do(req, &parsed); err != nil {
		return "", fmt.Errorf("queueing transcription: %w", err)
	}
	if parsed.Identifier == "" {
		return "", fmt.Errorf("queueing transcription: no identifier returned")
	}
	return parsed.Identifier, nil
}

type taskResult struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Result struct {
		Segments []Segment `json:"segments"`
	} `json:"result"`
}

func (w *WhisperX) Result(ctx context.Context, taskId string) ([]Segment, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/task/%s", w.cfg.BaseUrl, taskId), nil)
	if err != nil {
		return nil, err
	}
	w.authorize(req)

	var parsed taskResult
	if err := w.do(req, &parsed); err != nil {
		return nil, fmt.Errorf("polling task %s: %w", taskId, err)
	}

	switch parsed.Status {
	case "completed":
		if len(parsed.Result.Segments) == 0 {
			return nil, fmt.Errorf("task %s completed with an empty transcription", taskId)
		}
		return parsed.Result.Segments, nil
	case "failed":
		return nil, fmt.Errorf("task %s failed: %s", taskId, parsed.Error)
	case "queued", "processing":
		return nil, ErrStillRunning
	default:
		return nil, fmt.Errorf("task %s in unknown state %q", taskId, parsed.Status)
	}
}

func (w *WhisperX) authorize(req *http.Request) {
	if w.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+w.cfg.Token)
	}
}

func (w *WhisperX) do(req *http.Request, into any) error {
	res, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	payload, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("whisper returned %d: %s", res.StatusCode, strings.TrimSpace(string(payload)))
	}
	return json.Unmarshal(payload, into)
}
