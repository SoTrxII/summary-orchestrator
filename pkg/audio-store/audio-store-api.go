package audio_store

import (
	"context"
	"time"

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

	// RecordedAt reports when a recording was written to the bucket, which is
	// the closest thing we have to when the session was played.
	//
	// Nothing else records it: rpg-db's session row carries only createdat,
	// which is when the video finished uploading and is therefore the date of
	// the backfill rather than of the game for every episode imported after the
	// fact. The object's own last-modified is written by cooking-server minutes
	// after the table breaks up. Spot-checked across campaign 22 it is
	// monotonic and plausible: 2025-12-15, 2026-01-05, 2026-01-19, 2026-02-10,
	// 2026-05-05, 2026-09-14.
	RecordedAt(ctx context.Context, key string) (time.Time, error)
}
