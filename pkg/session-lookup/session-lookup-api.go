package session_lookup

import (
	"context"
	"errors"
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

	// GameMasters returns the Discord ids running the campaign.
	//
	// This is the only distinction the summary needs in order to separate
	// player characters from NPCs: a character voiced by anyone else is a PC,
	// and a character voiced by these is not. Empty when the campaign has no
	// rows, which must degrade to "nobody is marked" rather than fail.
	GameMasters(ctx context.Context, campaignId int) (map[string]bool, error)
}
