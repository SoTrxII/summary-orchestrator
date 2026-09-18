package audio_store

import (
	"context"

	"summary-orchestrator/pkg/speakers"
)

// AudioStore hands out URLs to the recordings bucket.
type AudioStore interface {
	// PresignedUrl returns a time-limited, directly downloadable URL for a
	// cooked audio key.
	PresignedUrl(ctx context.Context, key string) (string, error)
	// SpeakerCount reports how many people Pandora had in the voice channel
	// for a recording, or 0 when there is no way to tell.
	SpeakerCount(ctx context.Context, key string) (int, error)

	// SpeakerTimeline reports who was transmitting when, as cooking-server
	// recorded it beside the recording. Absent for anything cooked before that
	// existed, and for anything whose raw record was already deleted -- the
	// caller must degrade to diarization's anonymous labels rather than fail.
	SpeakerTimeline(ctx context.Context, key string) (speakers.Timeline, error)
}
