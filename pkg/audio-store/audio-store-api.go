package audio_store

import "context"

// AudioStore hands out URLs to the recordings bucket.
type AudioStore interface {
	// PresignedUrl returns a time-limited, directly downloadable URL for a
	// cooked audio key.
	PresignedUrl(ctx context.Context, key string) (string, error)
	// SpeakerCount reports how many people Pandora had in the voice channel
	// for a recording, or 0 when there is no way to tell.
	SpeakerCount(ctx context.Context, key string) (int, error)
}
