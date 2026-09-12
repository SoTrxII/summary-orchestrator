package workflows

import (
	"fmt"
	"log/slog"

	"summary-orchestrator/pkg/storyteller"

	"github.com/dapr/durabletask-go/workflow"
)

// SummarizeEpisode turns a freshly cooked recording into a narrative summary,
// a knowledge-graph entry and a gist, without ever holding the audio itself.
//
// It runs alongside the rest of processing rather than inside it: the video is
// encoded, uploaded and handed back to the user while this is still working.
func SummarizeEpisode(ctx *workflow.WorkflowContext) (any, error) {
	var in EpisodeInput
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	log := func(msg string) {
		if !ctx.IsReplaying() {
			slog.Info(fmt.Sprintf("[SummarizeEpisode] :: job %s :: %s", in.JobId, msg))
		}
	}
	log(fmt.Sprintf("starting for campaign %d, episode %d", in.CampaignId, in.EpisodeId))

	// 1. Hand whisper a URL rather than the bytes.
	var audioUrl string
	if err := ctx.CallActivity(PresignAudio,
		workflow.WithActivityInput(in),
		workflow.WithActivityRetryPolicy(transientRetry),
	).Await(&audioUrl); err != nil {
		return nil, fmt.Errorf("presigning audio: %w", err)
	}

	// 2. Queue the transcription, then wait it out. Whisper is asynchronous,
	// so this is two steps: one to enqueue, one to collect.
	var taskId string
	if err := ctx.CallActivity(StartTranscription,
		workflow.WithActivityInput(transcriptionRequest{AudioUrl: audioUrl}),
		workflow.WithActivityRetryPolicy(transientRetry),
	).Await(&taskId); err != nil {
		return nil, fmt.Errorf("queueing transcription: %w", err)
	}
	log("transcription queued as " + taskId)

	var lines []storyteller.Line
	if err := ctx.CallActivity(FetchTranscription,
		workflow.WithActivityInput(transcriptionResult{TaskId: taskId}),
		workflow.WithActivityRetryPolicy(transcriptionRetry),
	).Await(&lines); err != nil {
		return nil, fmt.Errorf("transcribing: %w", err)
	}
	log(fmt.Sprintf("transcribed into %d lines", len(lines)))

	// 3. The agent writes the chapter and stores it in the knowledge graph.
	var episodeSummary string
	if err := ctx.CallActivity(WriteEpisodeSummary,
		workflow.WithActivityInput(episodeSummaryRequest{
			CampaignId: in.campaign(),
			EpisodeId:  in.episode(),
			Lines:      lines,
		}),
		workflow.WithActivityRetryPolicy(noRetry),
	).Await(&episodeSummary); err != nil {
		return nil, fmt.Errorf("summarizing episode: %w", err)
	}

	var episodeKey string
	if err := ctx.CallActivity(StoreEpisodeSummary,
		workflow.WithActivityInput(storeRequest{
			CampaignId: in.campaign(),
			EpisodeId:  in.episode(),
			Summary:    episodeSummary,
		}),
		workflow.WithActivityRetryPolicy(transientRetry),
	).Await(&episodeKey); err != nil {
		return nil, fmt.Errorf("storing episode summary: %w", err)
	}

	// 4. A one-shot is standalone, so there is no campaign narrative to
	// rebuild. The key is still reported: it is where a campaign summary
	// would live, and the consumer requires the field either way.
	campaignKey := deps.Summaries.CampaignKey(in.campaign())
	if !in.IsOneShot {
		var campaignSummary string
		if err := ctx.CallActivity(WriteCampaignSummary,
			workflow.WithActivityInput(campaignSummaryRequest{CampaignId: in.campaign()}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(&campaignSummary); err != nil {
			return nil, fmt.Errorf("summarizing campaign: %w", err)
		}
		if err := ctx.CallActivity(StoreCampaignSummary,
			workflow.WithActivityInput(storeRequest{
				CampaignId: in.campaign(),
				Summary:    campaignSummary,
			}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(&campaignKey); err != nil {
			return nil, fmt.Errorf("storing campaign summary: %w", err)
		}
	} else {
		log("one-shot, skipping the campaign summary")
	}

	// 5. Condense into the short form Discord shows.
	var gist string
	if err := ctx.CallActivity(WriteGist,
		workflow.WithActivityInput(gistRequest{
			CampaignId: in.campaign(),
			EpisodeId:  in.episode(),
			Summary:    episodeSummary,
		}),
		workflow.WithActivityRetryPolicy(transientRetry),
	).Await(&gist); err != nil {
		return nil, fmt.Errorf("writing gist: %w", err)
	}

	// 6. Publish it, once the episode actually exists in the database.
	if err := ctx.CallActivity(PublishGist,
		workflow.WithActivityInput(publishRequest{
			CampaignId:  in.CampaignId,
			EpisodeId:   in.EpisodeId,
			Summary:     gist,
			EpisodeKey:  episodeKey,
			CampaignKey: campaignKey,
		}),
		workflow.WithActivityRetryPolicy(gistRetry),
	).Await(nil); err != nil {
		return nil, fmt.Errorf("publishing gist: %w", err)
	}

	log("done")
	return episodeKey, nil
}
