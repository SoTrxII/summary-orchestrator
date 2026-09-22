package summary_store

import (
	"context"

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
