package audio_store

import "context"

// AudioStore hands out URLs to the recordings bucket.
type AudioStore interface {
	// PresignedUrl returns a time-limited, directly downloadable URL for a
	// cooked audio key.
	PresignedUrl(ctx context.Context, key string) (string, error)
}
