package workflows

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	session_lookup "summary-orchestrator/pkg/session-lookup"
	"summary-orchestrator/pkg/storyteller"
	summary_store "summary-orchestrator/pkg/summary-store"
	"summary-orchestrator/pkg/transcriber"

	"github.com/dapr/durabletask-go/workflow"
)

type presignRequest struct {
	Key string `json:"key"`
}

type transcriptionRequest struct {
	AudioUrl string `json:"audioUrl"`
	// 0 when the recording came without a roster to count.
	MaxSpeakers int `json:"maxSpeakers"`
}

type transcriptRequest struct {
	CampaignId       string             `json:"campaignId"`
	EpisodeId        string             `json:"episodeId"`
	AudioKeys        []string           `json:"audioKeys"`
	TranscriptionIds []string           `json:"transcriptionIds"`
	Lines            []storyteller.Line `json:"lines"`
}

type transcriptionResult struct {
	TaskId string `json:"taskId"`
	// Recording this task transcribed. Needed to find the speaker timeline,
	// which is stored beside the audio rather than against the whisper task.
	AudioKey string `json:"audioKey"`
	// Discord ids of whoever runs this campaign, as the caller supplied them.
	GameMasterIds []string `json:"gameMasterIds"`
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
	var in presignRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Audio.PresignedUrl(ctx.Context(), in.Key)
}

// CountSpeakers reads how many people were in the voice channel, so that
// diarization can be told what it is looking at.
//
// It never fails the workflow. A recording with no roster — anything archived
// before Pandora wrote one — is transcribed unbounded, exactly as it was
// before; a worse guess is not worth losing hours of transcription over.
func CountSpeakers(ctx workflow.ActivityContext) (any, error) {
	var in presignRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}

	people, err := deps.Audio.SpeakerCount(ctx.Context(), in.Key)
	if err != nil {
		slog.Info(fmt.Sprintf("[CountSpeakers] :: no roster for %s, diarizing unbounded: %s", in.Key, err))
		return 0, nil
	}
	return people, nil
}

// StartTranscription queues the work and returns whisper's task id.
func StartTranscription(ctx workflow.ActivityContext) (any, error) {
	var in transcriptionRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Transcriber.Start(ctx.Context(), in.AudioUrl, in.MaxSpeakers)
}

// StoreTranscript archives the transcript the summary will be written from.
func StoreTranscript(ctx workflow.ActivityContext) (any, error) {
	var in transcriptRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Summaries.SaveTranscript(ctx.Context(), summary_store.Transcript{
		CampaignId:       in.CampaignId,
		EpisodeId:        in.EpisodeId,
		AudioKeys:        in.AudioKeys,
		TranscriptionIds: in.TranscriptionIds,
		Lines:            in.Lines,
	})
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

	// Replace diarization's anonymous clusters with the people who actually
	// spoke, when the recording was cooked recently enough to have a timeline.
	// Both lookups degrade rather than fail: a transcript with SPEAKER_00 in it
	// is worth far more than no transcript, and this is the step that already
	// cost hours of CPU.
	timeline, err := deps.Audio.SpeakerTimeline(ctx.Context(), in.AudioKey)
	if err != nil {
		slog.Info(fmt.Sprintf("[FetchTranscription] :: no speaker timeline for %s (%s), falling back to diarization labels", in.AudioKey, err))
	}
	gms := make(map[string]bool, len(in.GameMasterIds))
	for _, id := range in.GameMasterIds {
		gms[id] = true
	}

	lines := make([]storyteller.Line, 0, len(segments))
	named := 0
	for _, s := range segments {
		text := strings.TrimSpace(s.Text)
		if text == "" {
			continue
		}
		speaker := s.Speaker
		if speaker == "" {
			speaker = "UNKNOWN"
		}
		if track := timeline.Speaker(s.Start, s.End); track != nil {
			speaker = track.Label(gms[track.UserId])
			named++
		}
		lines = append(lines, storyteller.Line{Speaker: speaker, Text: text})
	}
	if named > 0 {
		slog.Info(fmt.Sprintf("[FetchTranscription] :: %d/%d lines attributed to a named participant", named, len(lines)))
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
