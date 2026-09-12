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

// WhisperX talks to a whisperX-FastAPI deployment.
type WhisperX struct {
	baseUrl string
	token   string
	// Language the audio is in. Passed explicitly: the API's own default is
	// "en" regardless of the server's DEFAULT_LANG.
	language string
	// Optional model override. Empty means "whatever the server is
	// configured with", which is the normal case.
	model  string
	client *http.Client
}

func New(baseUrl, token, language, model string, client *http.Client) *WhisperX {
	if client == nil {
		client = http.DefaultClient
	}
	return &WhisperX{
		baseUrl:  strings.TrimSuffix(baseUrl, "/"),
		token:    token,
		language: language,
		model:    model,
		client:   client,
	}
}

type startResponse struct {
	Identifier string `json:"identifier"`
	Message    string `json:"message"`
}

func (w *WhisperX) Start(ctx context.Context, audioUrl string) (string, error) {
	// The endpoint takes the audio location as a form field and everything
	// else as query parameters.
	q := url.Values{}
	q.Set("language", w.language)
	q.Set("diarize", "true")
	if w.model != "" {
		q.Set("model", w.model)
	}

	body := url.Values{}
	body.Set("url", audioUrl)

	endpoint := fmt.Sprintf("%s/speech-to-text-url?%s", w.baseUrl, q.Encode())
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/task/%s", w.baseUrl, taskId), nil)
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
	if w.token != "" {
		req.Header.Set("Authorization", "Bearer "+w.token)
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
