package workflows

import (
	"fmt"
	"log/slog"
	"strings"

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

	// 1-2. Queue the transcription, then wait it out. Whisper is asynchronous,
	// so this is two steps: one to enqueue, one to collect. Transcribing a
	// session takes hours, so a caller holding the ids of tasks that already
	// ran can skip straight to collecting them — that is the difference
	// between minutes and half a day when a later step has to be re-run.
	//
	// A recording that dropped and resumed arrives as several chunks. Each is
	// a separate whisper job; they are all queued before any is collected, so
	// they sit in whisper's queue back to back instead of waiting on a round
	// trip through here between them.
	//
	// Unless the caller already has a transcript, in which case none of that
	// happens at all -- see the TranscriptKey branch below.
	keys := in.Keys()
	taskIds := in.Tasks()
	if in.TranscriptKey == "" && len(keys) == 0 {
		return nil, fmt.Errorf("no audio to summarize")
	}
	var lines []storyteller.Line
	if in.TranscriptKey != "" {
		// Nothing to transcribe: the dialogue already exists. For an episode
		// whose recording has been pruned this is the only path that leads to
		// a summary at all.
		if err := ctx.CallActivity(LoadTranscript,
			workflow.WithActivityInput(loadTranscriptRequest{Key: in.TranscriptKey}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(&lines); err != nil {
			return nil, fmt.Errorf("loading transcript %s: %w", in.TranscriptKey, err)
		}
		log(fmt.Sprintf("loaded %d lines from %s, skipping transcription", len(lines), in.TranscriptKey))
	} else {
		if len(taskIds) == 0 {
			for _, key := range keys {
				// Hand whisper a URL rather than the bytes.
				var audioUrl string
				if err := ctx.CallActivity(PresignAudio,
					workflow.WithActivityInput(presignRequest{Key: key}),
					workflow.WithActivityRetryPolicy(transientRetry),
				).Await(&audioUrl); err != nil {
					return nil, fmt.Errorf("presigning %s: %w", key, err)
				}

				// Tell diarization how many voices it may find. Unbounded, it
				// invents speakers that were never in the room, and a chapter
				// written from a transcript with a phantom in it reads like one.
				var maxSpeakers int
				if err := ctx.CallActivity(CountSpeakers,
					workflow.WithActivityInput(presignRequest{Key: key}),
					workflow.WithActivityRetryPolicy(transientRetry),
				).Await(&maxSpeakers); err != nil {
					return nil, fmt.Errorf("counting speakers of %s: %w", key, err)
				}

				var taskId string
				if err := ctx.CallActivity(StartTranscription,
					workflow.WithActivityInput(transcriptionRequest{
						AudioUrl:    audioUrl,
						MaxSpeakers: maxSpeakers,
					}),
					workflow.WithActivityRetryPolicy(transientRetry),
				).Await(&taskId); err != nil {
					return nil, fmt.Errorf("queueing transcription of %s: %w", key, err)
				}
				taskIds = append(taskIds, taskId)
				log(fmt.Sprintf("%s queued for transcription as %s (at most %d speakers)",
					key, taskId, maxSpeakers))
			}
		} else {
			log(fmt.Sprintf("reusing %d existing transcription(s): %s", len(taskIds), strings.Join(taskIds, ", ")))
		}

		// Collected in order: the chunks are consecutive in time, so the
		// concatenated transcript has to follow the same order to read as one
		// session rather than a shuffled one.
		for i, taskId := range taskIds {
			// The recording each task came from, so its speaker timeline can be
			// found. taskIds is built by walking keys in order, and a caller
			// supplying transcriptionIds is documented to list them in the same
			// order as the keys they came from -- but a caller that supplies only
			// task ids and no keys is still valid, so this must not index blindly.
			var audioKey string
			if i < len(keys) {
				audioKey = keys[i]
			}

			var chunk []storyteller.Line
			if err := ctx.CallActivity(FetchTranscription,
				workflow.WithActivityInput(transcriptionResult{
					TaskId:        taskId,
					AudioKey:      audioKey,
					GameMasterIds: in.GameMasterIds,
				}),
				workflow.WithActivityRetryPolicy(transcriptionRetry),
			).Await(&chunk); err != nil {
				return nil, fmt.Errorf("transcribing task %s: %w", taskId, err)
			}
			lines = append(lines, chunk...)
		}
		log(fmt.Sprintf("transcribed into %d lines", len(lines)))
	}

	// Archive the transcript before anything is written from it.
	//
	// This is the expensive half of the workflow — hours of CPU against
	// minutes for every step after it — and whisper's own task table was until
	// now the only copy, on one node's disk, reachable only through its API.
	// Kept here, re-summarizing an episode with a different prompt or model
	// costs minutes.
	//
	// A failure to archive is logged and swallowed: the transcript is already
	// in hand, and the summary is worth more than the copy of its source.
	//
	// It runs on the TranscriptKey path too, where it is a copy rather than a
	// rescue: it republishes the transcript under the current layout so the
	// episode looks like any other to everything downstream -- LoadTranscripts
	// for cast drafting in particular, which only walks campaigns/<c>/episodes/.
	// keys and taskIds are empty there, which is precisely how the campaign-10
	// and campaign-14 episodes migrated by hand already read.
	var transcriptKey string
	if err := ctx.CallActivity(StoreTranscript,
		workflow.WithActivityInput(transcriptRequest{
			CampaignId:       in.campaign(),
			EpisodeId:        in.episode(),
			AudioKeys:        keys,
			TranscriptionIds: taskIds,
			Lines:            lines,
		}),
		workflow.WithActivityRetryPolicy(transientRetry),
	).Await(&transcriptKey); err != nil {
		log(fmt.Sprintf("could not archive the transcript: %s", err))
	} else {
		log(fmt.Sprintf("transcript archived at %s", transcriptKey))
	}

	// Once there is enough evidence, (re)draft the campaign's cast so this
	// episode's own chapter -- and, below, the campaign narrative -- already
	// benefit from it. Skipped by the activity itself once a human has frozen
	// the file or written one by hand; a failure here costs a nice-to-have,
	// never the chapter, so it is logged and swallowed like the archive above.
	var castKey string
	if err := ctx.CallActivity(DraftCast,
		workflow.WithActivityInput(draftCastRequest{CampaignId: in.campaign()}),
		workflow.WithActivityRetryPolicy(transientRetry),
	).Await(&castKey); err != nil {
		log(fmt.Sprintf("could not draft the cast: %s", err))
	} else if castKey != "" {
		log(fmt.Sprintf("cast drafted at %s", castKey))
	}

	// 3. The agent writes the chapter and stores it in the knowledge graph.
	var episodeSummary string
	if err := ctx.CallActivity(WriteEpisodeSummary,
		workflow.WithActivityInput(episodeSummaryRequest{
			CampaignId: in.campaign(),
			EpisodeId:  in.episode(),
			Lines:      lines,
			AudioKeys:  keys,
			RecordedAt: in.RecordedAt,
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
			workflow.WithActivityRetryPolicy(campaignRetry),
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
