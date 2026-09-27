package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	session_lookup "summary-orchestrator/pkg/session-lookup"
	"summary-orchestrator/pkg/storyteller"
	summary_store "summary-orchestrator/pkg/summary-store"
	"summary-orchestrator/pkg/transcriber"

	"github.com/dapr/durabletask-go/workflow"
)

// minCastEpisodes mirrors marlin's MIN_CAST_EPISODES: the drafter scales its
// own PC/NPC confidence bar down to however many episodes actually exist, so
// one is enough for a real verdict -- otherwise a one-shot's cast.json would
// never get a second episode to clear a higher bar and would sit unclassified
// forever.
const minCastEpisodes = 1

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
	// Recordings this episode was transcribed from, in order. Carried only so
	// the activity can date the session from the first one.
	AudioKeys []string `json:"audioKeys"`
	// Caller-supplied play date, RFC3339. Wins over both lookups below when
	// set; see EpisodeInput.RecordedAt for why it exists.
	RecordedAt string `json:"recordedAt"`
	// The session stands alone, so the agent skips the knowledge graph.
	OneShot bool `json:"oneShot"`
}

type campaignSummaryRequest struct {
	CampaignId string `json:"campaignId"`
}

type draftCastRequest struct {
	CampaignId string `json:"campaignId"`
}

type loadTranscriptRequest struct {
	Key string `json:"key"`
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

// LoadTranscript reads an existing transcript instead of making one.
//
// The expensive half of this workflow is transcription, and a transcript the
// bucket already holds cost somebody those hours once already. Measured on the
// eight campaign-22 and campaign-28 episodes that exist in both forms,
// re-transcribing produced a median of 0.91x the text of the archive it
// replaced -- so for an episode that already has one, this is not a degraded
// shortcut, it is the same material for none of the CPU.
func LoadTranscript(ctx workflow.ActivityContext) (any, error) {
	var in loadTranscriptRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return deps.Summaries.LoadTranscriptLines(ctx.Context(), in.Key)
}

// DraftCast counts a first-pass cast off the campaign's archived transcripts
// and stores it, so a human corrects a draft instead of writing one from
// scratch.
//
// It only ever touches a cast it wrote itself. A file with no "generated"
// flag was curated by hand and one with "generated":false has been frozen by
// whoever corrected the draft -- both mean "leave this alone" here, which is
// why an unreadable existing file also takes that branch rather than being
// treated as absent: overwriting a half-edited cast on a parse failure would
// be worse than skipping a redraft.
func DraftCast(ctx workflow.ActivityContext) (any, error) {
	var in draftCastRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}

	current := deps.Summaries.LoadCast(ctx.Context(), in.CampaignId)
	if current != "" {
		var flag struct {
			Generated *bool `json:"generated"`
		}
		if err := json.Unmarshal([]byte(current), &flag); err != nil || flag.Generated == nil || !*flag.Generated {
			return "", nil
		}
	}

	transcripts, err := deps.Summaries.LoadTranscripts(ctx.Context(), in.CampaignId)
	if err != nil {
		return nil, fmt.Errorf("listing transcripts: %w", err)
	}
	if len(transcripts) < minCastEpisodes {
		return "", nil
	}

	raw := make([]json.RawMessage, 0, len(transcripts))
	for _, t := range transcripts {
		b, err := json.Marshal(t)
		if err != nil {
			continue
		}
		raw = append(raw, b)
	}

	// The draft being replaced goes along: while it is still "generated", a
	// human may already have added what the transcripts cannot say (who is at
	// the table, which names are the real player characters, which are one
	// person under two identities), and a redraft that did not carry those
	// over would erase them on the next episode.
	draft, err := deps.Agent.DraftCast(ctx.Context(), in.CampaignId, raw, current)
	if err != nil {
		return nil, fmt.Errorf("drafting cast: %w", err)
	}
	return deps.Summaries.SaveCast(ctx.Context(), in.CampaignId, draft)
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
//
// The campaign's cast list is fetched here rather than passed in through the
// workflow input: it is a property of the campaign, it can be corrected between
// two episodes of the same run, and reading it at the point of use means the
// next episode picks up the correction without restarting anything.
func WriteEpisodeSummary(ctx workflow.ActivityContext) (any, error) {
	var in episodeSummaryRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	cast := deps.Summaries.LoadCast(ctx.Context(), in.CampaignId)
	return deps.Agent.SummarizeEpisode(ctx.Context(), in.CampaignId, in.EpisodeId,
		in.Lines, cast, recordedAt(ctx, in), in.OneShot)
}

// recordedAt establishes when the session was played, or returns "" when
// nothing can answer.
//
// Three sources, most trustworthy first:
//
//  1. What the caller said. Only ever set by hand, and only when the two below
//     are known to be unable to answer, so it wins outright.
//  2. The recording's own last-modified date. The normal path: cooking-server
//     writes the object minutes after the table breaks up. The first key, not
//     the last -- a session that dropped and resumed comes back as consecutive
//     chunks, and the episode belongs at the moment it started.
//  3. The session row's createdAt. For a normal episode this is the same
//     evening, minutes later; for one backfilled by hand it is the play date
//     someone typed. It is third rather than unused because the object can be
//     pruned while the row survives -- campaign 22's first two episodes are
//     exactly that, dated 2023 with no audio left in the bucket.
//
// Never fails the activity. WriteEpisodeSummary runs under noRetry because it
// writes to the knowledge graph, so an error here would cost the episode its
// chapter outright -- for a field whose absence only restores the behaviour
// every previously stored episode already has.
func recordedAt(ctx workflow.ActivityContext, in episodeSummaryRequest) string {
	if supplied := strings.TrimSpace(in.RecordedAt); supplied != "" {
		return supplied
	}

	if len(in.AudioKeys) > 0 {
		when, err := deps.Audio.RecordedAt(ctx.Context(), in.AudioKeys[0])
		if err == nil {
			return when.UTC().Format(time.RFC3339)
		}
		slog.Info(fmt.Sprintf("[WriteEpisodeSummary] :: cannot date %s from the bucket (%s), "+
			"falling back to the session row", in.AudioKeys[0], err))
	}

	// Numeric ids everywhere else in this workflow; the strings here are only
	// what the agent's wire format wants. A non-numeric one cannot key the
	// session table, so there is nothing to look up.
	campaignId, campaignErr := strconv.Atoi(in.CampaignId)
	episodeId, episodeErr := strconv.Atoi(in.EpisodeId)
	if campaignErr != nil || episodeErr != nil {
		return ""
	}

	when, err := deps.Sessions.SessionDate(ctx.Context(), campaignId, episodeId)
	if err != nil {
		slog.Info(fmt.Sprintf("[WriteEpisodeSummary] :: no date for campaign %d episode %d (%s), "+
			"the episode will be stamped at ingestion", campaignId, episodeId, err))
		return ""
	}
	return when.UTC().Format(time.RFC3339)
}

// WriteCampaignSummary rebuilds the campaign narrative from the graph. Safe to
// repeat: it reads every stored episode and regenerates in one pass.
func WriteCampaignSummary(ctx workflow.ActivityContext) (any, error) {
	var in campaignSummaryRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	cast := deps.Summaries.LoadCast(ctx.Context(), in.CampaignId)
	return deps.Agent.SummarizeCampaign(ctx.Context(), in.CampaignId, cast)
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
