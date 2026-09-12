package storyteller

import "context"

// Line is one turn of dialogue, in the shape the agent expects as episode
// content.
type Line struct {
	Speaker string `json:"speaker"`
	Text    string `json:"text"`
}

// Storyteller is the Marlin A2A agent: it writes the narrative summaries and
// owns every prompt. This service never talks to a model directly.
type Storyteller interface {
	// SummarizeEpisode writes the narrative chapter for one episode. The
	// agent persists it to Graphiti itself, so this call is NOT idempotent:
	// running it twice adds two episodes to the knowledge graph.
	SummarizeEpisode(ctx context.Context, campaignId, episodeId string, lines []Line) (string, error)
	// SummarizeCampaign rebuilds the whole-campaign narrative from the
	// episodes already stored in Graphiti. Re-runnable at any time.
	SummarizeCampaign(ctx context.Context, campaignId string) (string, error)
	// Gist condenses an episode summary into the short form shown in Discord.
	Gist(ctx context.Context, campaignId, episodeId, summary string) (string, error)
}
