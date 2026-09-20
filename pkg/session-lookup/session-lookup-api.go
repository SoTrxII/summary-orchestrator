package session_lookup

import (
	"context"
	"errors"
	"time"
)

// ErrSessionNotReady means the episode has no row in the session table yet.
//
// This is the expected state for a while: Velvet only inserts the session once
// the video has finished uploading, which can be long after summarization
// starts. It is the one error the gist step waits out.
var ErrSessionNotReady = errors.New("session row not created yet")

// SessionLookup resolves an episode to the session number the database
// actually assigned it.
type SessionLookup interface {
	SessionNumber(ctx context.Context, campaignId, episodeIndex int) (int, error)

	// SessionDate reports when a session was recorded, as the database has it.
	//
	// This is the fallback for dating an episode whose recording has been
	// pruned from the bucket, and it is a better answer than it looks. For an
	// episode that went through the pipeline normally, createdAt trails the
	// recording by minutes -- campaign 22 episode 3 was cooked at 22:57 and the
	// row written at 23:08. For one backfilled by hand it is the play date
	// someone typed, which is exactly what is wanted: campaign 22's first two
	// sessions carry 2023-02-18 and 2023-04-29 at midnight sharp, three years
	// before the rest of the campaign and long since deleted from the bucket.
	//
	// Not a substitute for the object's own date in the normal case, though:
	// the row does not exist until the video has finished uploading, which is
	// usually after summarization starts. That is what ErrSessionNotReady
	// means here too.
	SessionDate(ctx context.Context, campaignId, episodeIndex int) (time.Time, error)
}
