package workflows

import (
	"errors"
	"fmt"
	"strings"

	session_lookup "summary-orchestrator/pkg/session-lookup"
	"summary-orchestrator/pkg/storyteller"
	"summary-orchestrator/pkg/transcriber"

	"github.com/dapr/durabletask-go/workflow"
)

type transcriptionRequest struct {
	AudioUrl string `json:"audioUrl"`
}

type transcriptionResult struct {
	TaskId string `json:"taskId"`
}

type episodeSummaryRequest struct {
	CampaignId string             `json:"campaignId"`
	EpisodeId  string             `json:"episodeId"`
	Lines      []storyteller.Line `json:"lines"`
}

type campaignSummaryRequest struct {
	CampaignId string `json:"campaignId"`
}

type storeRequest struct {
	CampaignId string `json:"campaignId"`
	// Empty for a campaign summary.
	EpisodeId string `json:"episodeId"`
	Summary   string `json:"summary"`
}

type gistRequest struct {
	CampaignId string `json:"campaignId"`
	EpisodeId  string `json:"episodeId"`
	Summary    string `json:"summary"`
}

type publishRequest struct {
	CampaignId  int    `json:"campaignId"`
	EpisodeId   int    `json:"episodeId"`
	Summary     string `json:"summary"`
	EpisodeKey  string `json:"episodeKey"`
	CampaignKey string `json:"campaignKey"`
}

// PresignAudio turns a cooked audio key into a URL whisper can fetch itself.
func PresignAudio(ctx workflow.ActivityContext) (any, error) {
	var in EpisodeInput
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Audio.PresignedUrl(ctx.Context(), in.AudioKey)
}

// StartTranscription queues the work and returns whisper's task id.
func StartTranscription(ctx workflow.ActivityContext) (any, error) {
	var in transcriptionRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Transcriber.Start(ctx.Context(), in.AudioUrl)
}

// FetchTranscription collects the diarized transcript, or reports that it is
// not ready yet so the workflow's retry policy waits.
func FetchTranscription(ctx workflow.ActivityContext) (any, error) {
	var in transcriptionResult
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}

	segments, err := deps.Transcriber.Result(ctx.Context(), in.TaskId)
	if err != nil {
		if errors.Is(err, transcriber.ErrStillRunning) {
			return nil, retryable(err)
		}
		return nil, err
	}

	lines := make([]storyteller.Line, 0, len(segments))
	for _, s := range segments {
		text := strings.TrimSpace(s.Text)
		if text == "" {
			continue
		}
		speaker := s.Speaker
		if speaker == "" {
			speaker = "UNKNOWN"
		}
		lines = append(lines, storyteller.Line{Speaker: speaker, Text: text})
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("transcription %s produced no usable dialogue", in.TaskId)
	}
	return lines, nil
}

// WriteEpisodeSummary asks the agent for the chapter. Not idempotent: the
// agent stores what it writes in the knowledge graph.
func WriteEpisodeSummary(ctx workflow.ActivityContext) (any, error) {
	var in episodeSummaryRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Agent.SummarizeEpisode(ctx.Context(), in.CampaignId, in.EpisodeId, in.Lines)
}

// WriteCampaignSummary rebuilds the campaign narrative from the graph. Safe to
// repeat: it reads every stored episode and regenerates in one pass.
func WriteCampaignSummary(ctx workflow.ActivityContext) (any, error) {
	var in campaignSummaryRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Agent.SummarizeCampaign(ctx.Context(), in.CampaignId)
}

func StoreEpisodeSummary(ctx workflow.ActivityContext) (any, error) {
	var in storeRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Summaries.SaveEpisode(ctx.Context(), in.CampaignId, in.EpisodeId, in.Summary)
}

func StoreCampaignSummary(ctx workflow.ActivityContext) (any, error) {
	var in storeRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Summaries.SaveCampaign(ctx.Context(), in.CampaignId, in.Summary)
}

func WriteGist(ctx workflow.ActivityContext) (any, error) {
	var in gistRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Agent.Gist(ctx.Context(), in.CampaignId, in.EpisodeId, in.Summary)
}

// PublishGist resolves the episode's real session number, then hands the gist
// to summary-updater.
//
// The lookup is the point of the step: a gist is keyed on the session row, and
// that row is only written once the video has finished uploading. Reporting
// "not ready" is what makes the workflow's backoff wait for the rest of the
// pipeline instead of racing it.
func PublishGist(ctx workflow.ActivityContext) (any, error) {
	var in publishRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}

	sessionNumber, err := deps.Sessions.SessionNumber(ctx.Context(), in.CampaignId, in.EpisodeId)
	if err != nil {
		// Both "no row yet" and a database blip are worth waiting out; only
		// a malformed response is not.
		if errors.Is(err, session_lookup.ErrSessionNotReady) {
			return nil, retryable(fmt.Errorf("campaign %d episode %d: %w", in.CampaignId, in.EpisodeId, err))
		}
		return nil, retryable(err)
	}

	gist := Gist{
		CampaignId:  in.CampaignId,
		EpisodeId:   sessionNumber,
		Summary:     in.Summary,
		EpisodeKey:  in.EpisodeKey,
		CampaignKey: in.CampaignKey,
	}
	if err := deps.Publisher.PublishEvent(ctx.Context(), deps.PubSubName, deps.GistTopic, gist); err != nil {
		return nil, retryable(err)
	}
	return sessionNumber, nil
}
