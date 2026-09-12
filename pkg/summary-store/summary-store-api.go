package summary_store

import "context"

// SummaryStore persists the generated summaries as objects, so the keys
// published alongside a gist point at something real.
type SummaryStore interface {
	// SaveEpisode stores an episode narrative and returns its key.
	SaveEpisode(ctx context.Context, campaignId, episodeId, summary string) (string, error)
	// SaveCampaign stores a whole-campaign narrative and returns its key.
	SaveCampaign(ctx context.Context, campaignId, summary string) (string, error)
	// CampaignKey is where a campaign summary lives, whether or not one has
	// been written yet.
	CampaignKey(campaignId string) string
}
