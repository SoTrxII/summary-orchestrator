package summary_store

import (
	"context"
	"errors"

	"summary-orchestrator/pkg/storyteller"
)

// Transcript is the raw material a summary was written from.
//
// It is archived for its own sake: transcription costs hours of CPU where
// everything after it costs minutes, so keeping it turns "summarize this
// campaign again with a different model" into a cheap operation instead of
// another two days of whisper.
type Transcript struct {
	CampaignId string `json:"campaignId"`
	EpisodeId  string `json:"episodeId"`
	// Recordings this came from, in order.
	AudioKeys []string `json:"audioKeys"`
	// The whisper tasks that produced it. Handed back to the workflow as
	// transcriptionIds, these re-run it without transcribing anything again —
	// for as long as whisper still has them, which is the other reason this
	// file exists.
	TranscriptionIds []string `json:"transcriptionIds"`
	// The diarized dialogue, in the order it was spoken.
	Lines []storyteller.Line `json:"lines"`
}

// Summary is a narrative as it sits in the bucket. EpisodeId is empty for the
// summary of a whole campaign.
//
// The field names are a contract twice over: they are what is already stored,
// and what the read routes hand to callers as is.
type Summary struct {
	CampaignId string `json:"campaignId"`
	EpisodeId  string `json:"episodeId,omitempty"`
	Summary    string `json:"summary"`
}

// ErrNoSummary means nothing has been written at that key. It is the expected
// answer for an episode that was never summarized -- a one-shot collection, or
// a session whose transcript is missing -- and is kept apart from a bucket
// that cannot be read so a caller can say "not yet" rather than "broken".
var ErrNoSummary = errors.New("no summary stored")

// SummaryStore persists the generated summaries as objects, so the keys
// published alongside a gist point at something real.
type SummaryStore interface {
	// SaveEpisode stores an episode narrative and returns its key.
	SaveEpisode(ctx context.Context, campaignId, episodeId, summary string) (string, error)
	// SaveCampaign stores a whole-campaign narrative and returns its key.
	SaveCampaign(ctx context.Context, campaignId, summary string) (string, error)
	// SaveTranscript archives the transcript an episode was written from and
	// returns its key.
	SaveTranscript(ctx context.Context, t Transcript) (string, error)
	// LoadTranscripts returns every transcript archived for a campaign.
	// Drafting a cast draws on cross-episode evidence -- a speaker label means
	// nothing outside the episode it came from -- so it needs all of them, not
	// just the one just written.
	LoadTranscripts(ctx context.Context, campaignId string) ([]Transcript, error)
	// LoadTranscriptLines reads the dialogue of a transcript stored at an
	// arbitrary key, in either of the two shapes the bucket holds (see
	// ParseTranscript). It is what lets an episode be summarized from a
	// transcript somebody already paid for instead of transcribing again.
	LoadTranscriptLines(ctx context.Context, key string) ([]storyteller.Line, error)
	// CampaignKey is where a campaign summary lives, whether or not one has
	// been written yet.
	CampaignKey(campaignId string) string
	// LoadCast returns the campaign's curated cast list as raw JSON, or "" when
	// it has none. Never fails: a cast improves a summary, it is not required
	// for one.
	LoadCast(ctx context.Context, campaignId string) string
	// SaveCast stores a campaign's cast list -- drafted or corrected by hand --
	// and returns its key.
	SaveCast(ctx context.Context, campaignId, cast string) (string, error)
}
