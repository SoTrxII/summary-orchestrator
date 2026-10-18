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

	out, err := summarize(ctx, in, log)
	if err != nil {
		// Only logged if it can't be told: the workflow fails either way
		if pubErr := ctx.CallActivity(PublishFailure,
			workflow.WithActivityInput(Failure{
				CampaignId: in.CampaignId,
				EpisodeId:  in.EpisodeId,
				JobId:      in.JobId,
				Error:      err.Error(),
			}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(nil); pubErr != nil {
			log(fmt.Sprintf("could not report the failure: %s", pubErr))
		}
	}
	return out, err
}

func summarize(ctx *workflow.WorkflowContext, in EpisodeInput, log func(string)) (any, error) {
	log(fmt.Sprintf("starting for campaign %d, episode %d", in.CampaignId, in.EpisodeId))

	// 1-2. Transcribe. Whisper is asynchronous, so each job is two steps: one to
	// start it, one to collect it. Transcribing a session takes hours, so a
	// caller holding the ids of tasks that already ran can skip straight to
	// collecting them — that is the difference between minutes and half a day
	// when a later step has to be re-run.
	//
	// Fresh transcriptions go through the whisper lease, one episode at a
	// time: whisper runs every request it is handed at once, and two of them
	// get it OOM-killed (see whisper_lease.Lease). A recording that dropped and
	// resumed arrives as several chunks, and those run one after the other
	// too, under the same lease. They used to all be queued before any was
	// collected, on the belief that whisper would line them up back to back;
	// it runs them side by side instead.
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
	} else if len(taskIds) > 0 {
		log(fmt.Sprintf("reusing %d existing transcription(s): %s", len(taskIds), strings.Join(taskIds, ", ")))
		// Collected in order: the chunks are consecutive in time, so the
		// concatenated transcript has to follow the same order to read as one
		// session rather than a shuffled one.
		for i, taskId := range taskIds {
			// The recording each task came from, so its speaker timeline can be
			// found. A caller supplying transcriptionIds is documented to list
			// them in the same order as the keys they came from -- but a caller
			// that supplies only task ids and no keys is still valid, so this
			// must not index blindly.
			var audioKey string
			if i < len(keys) {
				audioKey = keys[i]
			}
			chunk, err := collectTranscription(ctx, taskId, audioKey, in.GameMasterIds)
			if err != nil {
				return nil, err
			}
			lines = append(lines, chunk...)
		}
		log(fmt.Sprintf("transcribed into %d lines", len(lines)))
	} else {
		lease := whisperLeaseRequest{Holder: in.JobId}
		if err := ctx.CallActivity(AcquireWhisper,
			workflow.WithActivityInput(lease),
			workflow.WithActivityRetryPolicy(whisperQueueRetry),
		).Await(nil); err != nil {
			return nil, fmt.Errorf("waiting for whisper: %w", err)
		}
		log("whisper is ours")

		var err error
		lines, taskIds, err = transcribeChunks(ctx, in, keys, log)

		// Given back whether that worked or not. A failure to release is only
		// logged: the lease frees itself once whisper reports the task done
		// (see whisper_lease.Gate.Acquire), so it delays the next episode by a
		// poll at most, while failing here would throw the transcript away.
		if relErr := ctx.CallActivity(ReleaseWhisper,
			workflow.WithActivityInput(lease),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(nil); relErr != nil {
			log(fmt.Sprintf("could not release whisper: %s", relErr))
		}
		if err != nil {
			return nil, err
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

	if in.TranscribeOnly {
		if transcriptKey == "" {
			// The archive was the whole point of this run. The task ids are
			// named so a re-run can collect them instead of transcribing again.
			return nil, fmt.Errorf("transcribe-only run could not archive its transcript (whisper tasks %s)",
				strings.Join(taskIds, ", "))
		}
		log("transcribe-only, stopping here")
		return transcriptKey, nil
	}

	// Once there is enough evidence, (re)draft the campaign's cast so this
	// episode's own chapter -- and, below, the campaign narrative -- already
	// benefit from it. Skipped by the activity itself once a human has frozen
	// the file or written one by hand; a failure here costs a nice-to-have,
	// never the chapter, so it is logged and swallowed like the archive above.
	//
	// Not for a one-shot: the draft is a vote across episodes for one recurring
	// party, and a campaign made of unrelated sessions has none to find -- the
	// result is a plausible-looking cast of whoever dominated one episode.
	if !in.IsOneShot {
		var castKey string
		if err := ctx.CallActivity(DraftCast,
			workflow.WithActivityInput(draftCastRequest{CampaignId: in.campaign(), Characters: in.Characters}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(&castKey); err != nil {
			log(fmt.Sprintf("could not draft the cast: %s", err))
		} else if castKey != "" {
			log(fmt.Sprintf("cast drafted at %s", castKey))
		}
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
			OneShot:    in.IsOneShot,
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

	// 6. Publish it.
	if err := ctx.CallActivity(PublishGist,
		workflow.WithActivityInput(publishRequest{
			CampaignId:  in.CampaignId,
			EpisodeId:   in.EpisodeId,
			Summary:     gist,
			EpisodeKey:  episodeKey,
			CampaignKey: campaignKey,
		}),
		workflow.WithActivityRetryPolicy(transientRetry),
	).Await(nil); err != nil {
		return nil, fmt.Errorf("publishing gist: %w", err)
	}

	log("done")
	return episodeKey, nil
}

// transcribeChunks runs each recording through whisper in turn, starting the
// next only once the last is collected. The caller must hold the whisper lease.
func transcribeChunks(ctx *workflow.WorkflowContext, in EpisodeInput, keys []string, log func(string)) ([]storyteller.Line, []string, error) {
	var lines []storyteller.Line
	var taskIds []string
	for _, key := range keys {
		// Presigned only now, not before waiting for the lease: the URL lives
		// for PresignTTL, and the wait for whisper can be longer than that.
		var audioUrl string
		if err := ctx.CallActivity(PresignAudio,
			workflow.WithActivityInput(presignRequest{Key: key}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(&audioUrl); err != nil {
			return nil, taskIds, fmt.Errorf("presigning %s: %w", key, err)
		}

		// Tell diarization how many voices it may find. Unbounded, it invents
		// speakers that were never in the room, and a chapter written from a
		// transcript with a phantom in it reads like one.
		var maxSpeakers int
		if err := ctx.CallActivity(CountSpeakers,
			workflow.WithActivityInput(presignRequest{Key: key}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(&maxSpeakers); err != nil {
			return nil, taskIds, fmt.Errorf("counting speakers of %s: %w", key, err)
		}

		var taskId string
		if err := ctx.CallActivity(StartTranscription,
			workflow.WithActivityInput(transcriptionRequest{
				AudioUrl:    audioUrl,
				MaxSpeakers: maxSpeakers,
			}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(&taskId); err != nil {
			return nil, taskIds, fmt.Errorf("queueing transcription of %s: %w", key, err)
		}
		taskIds = append(taskIds, taskId)
		log(fmt.Sprintf("%s transcribing as %s (at most %d speakers)", key, taskId, maxSpeakers))

		// Only logged on failure. The task is already running, so failing now
		// would abandon it without freeing whisper; left unattached, the lease
		// still frees itself after StartGrace.
		if err := ctx.CallActivity(AttachWhisper,
			workflow.WithActivityInput(whisperLeaseRequest{Holder: in.JobId, TaskId: taskId}),
			workflow.WithActivityRetryPolicy(transientRetry),
		).Await(nil); err != nil {
			log(fmt.Sprintf("could not record %s on the whisper lease: %s", taskId, err))
		}

		chunk, err := collectTranscription(ctx, taskId, key, in.GameMasterIds)
		if err != nil {
			return nil, taskIds, err
		}
		lines = append(lines, chunk...)
	}
	return lines, taskIds, nil
}

// collectTranscription waits for one whisper task and returns its lines.
func collectTranscription(ctx *workflow.WorkflowContext, taskId, audioKey string, gameMasterIds []string) ([]storyteller.Line, error) {
	var chunk []storyteller.Line
	if err := ctx.CallActivity(FetchTranscription,
		workflow.WithActivityInput(transcriptionResult{
			TaskId:        taskId,
			AudioKey:      audioKey,
			GameMasterIds: gameMasterIds,
		}),
		workflow.WithActivityRetryPolicy(transcriptionRetry),
	).Await(&chunk); err != nil {
		return nil, fmt.Errorf("transcribing task %s: %w", taskId, err)
	}
	return chunk, nil
}
