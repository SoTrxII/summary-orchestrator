package audio_store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"summary-orchestrator/internal/utils"

	"github.com/dapr/go-sdk/client"
)

// DaprAudioStore resolves keys in the recordings bucket to presigned URLs,
// using the S3 binding's "presign" operation.
//
// Presigning rather than fetching is deliberate: whisper downloads the audio
// itself, so a multi-hundred-megabyte recording never travels through this
// process nor through the Dapr sidecar's gRPC channel (which caps message
// size well below a full session).
type DaprAudioStore struct {
	componentName string
	ttl           string
	client        utils.Binder
}

func New(componentName, ttl string, client utils.Binder) *DaprAudioStore {
	return &DaprAudioStore{componentName, ttl, client}
}

type presignResponse struct {
	PresignUrl string `json:"presignURL"`
}

func (a *DaprAudioStore) PresignedUrl(ctx context.Context, key string) (string, error) {
	res, err := a.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      a.componentName,
		Operation: "presign",
		Metadata: map[string]string{
			"key":        key,
			"presignTTL": a.ttl,
		},
	})
	if err != nil {
		return "", fmt.Errorf("presigning %s: %w", key, err)
	}

	var parsed presignResponse
	if err := json.Unmarshal(res.Data, &parsed); err != nil {
		return "", fmt.Errorf("presigning %s: unreadable response: %w", key, err)
	}
	if parsed.PresignUrl == "" {
		return "", fmt.Errorf("presigning %s: empty url in response", key)
	}
	return parsed.PresignUrl, nil
}

// usersSuffix is what Pandora names the roster it writes beside a recording.
const usersSuffix = ".users"

// SpeakerCount counts the people Pandora saw in the voice channel while a
// recording was being made. That is an upper bound on how many voices the
// audio can contain, which is exactly what diarization wants: left to guess,
// it over-segments and invents speakers that were never there.
//
// It is only ever an upper bound. The roster counts everyone who joined,
// including those who never said a word, so it must not be used as a minimum.
//
// A missing or unreadable roster is not a failure. Recordings archived before
// Pandora wrote one, or from another source entirely, simply have none; this
// reports 0, meaning "no idea", and the caller transcribes without a bound.
func (a *DaprAudioStore) SpeakerCount(ctx context.Context, key string) (int, error) {
	res, err := a.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      a.componentName,
		Operation: "get",
		Metadata:  map[string]string{"key": key + usersSuffix},
	})
	if err != nil {
		return 0, fmt.Errorf("reading the roster of %s: %w", key, err)
	}
	return countRoster(res.Data), nil
}

// parseRoster counts the people in Pandora's roster format.
//
// The file is not an object but the inside of one: Pandora appends a line per
// user as they join, so it reads `"0":{}\n,"1":{"id":…}` with no enclosing
// braces and no way to know in advance where it ends. Wrapping it is what
// makes it parseable. Index 0 is a placeholder carrying no id and is not a
// person. Anything unparseable counts as no information rather than as zero
// speakers, since the two mean very different things downstream.
// countRoster reads Pandora's roster, base64-wrapped or not.
//
// The recordings binding is configured with encodeBase64, so a get comes back
// base64-encoded rather than as the bytes that were stored. Rather than
// depending on that staying true, this tries the content as-is first and falls
// back to decoding it: the roster is never valid base64 by accident, and
// base64 is never a valid roster.
func countRoster(raw []byte) int {
	if people := parseRoster(raw); people > 0 {
		return people
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0
	}
	return parseRoster(decoded)
}

func parseRoster(raw []byte) int {
	body := strings.TrimSpace(string(raw))
	if body == "" {
		return 0
	}
	// Tolerate a real object too, in case the format is ever tidied up.
	if !strings.HasPrefix(body, "{") {
		body = "{" + body + "}"
	}

	var roster map[string]struct {
		Id string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &roster); err != nil {
		return 0
	}

	people := 0
	for _, entry := range roster {
		if entry.Id != "" {
			people++
		}
	}
	return people
}
