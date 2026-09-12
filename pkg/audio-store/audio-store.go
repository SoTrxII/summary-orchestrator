package audio_store

import (
	"context"
	"encoding/json"
	"fmt"

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
