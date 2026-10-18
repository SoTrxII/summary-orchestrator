package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
	// See EpisodeInput.Characters.
	Characters []Character `json:"characters"`
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

type whisperLeaseRequest struct {
	// The workflow instance id: unique, and stable across replays.
	Holder string `json:"holder"`
	TaskId string `json:"taskId"`
}

// AcquireWhisper waits for whisper to be free. See whisper_lease.Lease.
//
// Every failure is retryable. Busy, losing a race for the lease, a state
// store blip and a whisper that cannot be asked all mean the same thing here:
// not yet, ask again later.
func AcquireWhisper(ctx workflow.ActivityContext) (any, error) {
	var in whisperLeaseRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	if err := deps.Whisper.Acquire(ctx.Context(), in.Holder); err != nil {
		slog.Info(fmt.Sprintf("[AcquireWhisper] :: job %s :: waiting: %s", in.Holder, err))
		return nil, retryable(err)
	}
	return nil, nil
}

// AttachWhisper records the task this holder just started on the lease.
func AttachWhisper(ctx workflow.ActivityContext) (any, error) {
	var in whisperLeaseRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return nil, deps.Whisper.Attach(ctx.Context(), in.Holder, in.TaskId)
}

// ReleaseWhisper gives whisper back.
func ReleaseWhisper(ctx workflow.ActivityContext) (any, error) {
	var in whisperLeaseRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return nil, deps.Whisper.Release(ctx.Context(), in.Holder)
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
	draft, err := deps.Agent.DraftCast(ctx.Context(), in.CampaignId, raw, withRoster(current, in.Characters))
	if err != nil {
		return nil, fmt.Errorf("drafting cast: %w", err)
	}
	return deps.Summaries.SaveCast(ctx.Context(), in.CampaignId, draft)
}

// withRoster adds the declared characters to the cast handed to the drafter,
// under a "roster" key the agent reads and does not write back.
//
// Only that copy gets them, never the stored file: Velvet sends the list again
// with every episode, so a character removed there is gone from the next draft
// without anyone having to delete it here too. And it rides in the cast because
// the cast already travels in this call -- the agent's protocol is unchanged.
//
// A cast that will not parse goes through as it is. The caller only gets this
// far with an empty one or one it has just read "generated" from, so that
// branch costs a declared list, never a draft.
func withRoster(cast string, roster []Character) string {
	if len(roster) == 0 {
		return cast
	}
	fields := map[string]json.RawMessage{}
	if cast != "" {
		if err := json.Unmarshal([]byte(cast), &fields); err != nil {
			return cast
		}
	}
	// Neither Marshal can fail: one is given strings, the other raw values
	// that were parsed a moment ago.
	fields["roster"], _ = json.Marshal(roster)
	out, _ := json.Marshal(fields)
	return string(out)
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
// Two sources, most trustworthy first:
//
//  1. What the caller said. Only ever set by hand, and only when the bucket is
//     known to be unable to answer, so it wins outright.
//  2. The recording's own last-modified date. The normal path: cooking-server
//     writes the object minutes after the table breaks up. The first key, not
//     the last -- a session that dropped and resumed comes back as consecutive
//     chunks, and the episode belongs at the moment it started.
//
// The session row's createdAt used to come third, for episodes whose audio was
// pruned while the row survived (campaign 22's first two, played in 2023). It
// was the only read of rpg-db left, a database Velvet owns: such an episode is
// only ever replayed by hand, and whoever replays it passes RecordedAt.
//
// Never fails the activity. WriteEpisodeSummary runs under noRetry because it
// writes to the knowledge graph, so an error here would cost the episode its
// chapter outright -- for a field whose absence only restores the behaviour
// every previously stored episode already has.
func recordedAt(ctx workflow.ActivityContext, in episodeSummaryRequest) string {
	if supplied := strings.TrimSpace(in.RecordedAt); supplied != "" {
		return supplied
	}
	if len(in.AudioKeys) == 0 {
		return ""
	}
	when, err := deps.Audio.RecordedAt(ctx.Context(), in.AudioKeys[0])
	if err != nil {
		slog.Info(fmt.Sprintf("[WriteEpisodeSummary] :: cannot date %s from the bucket (%s), "+
			"the episode will be stamped at ingestion", in.AudioKeys[0], err))
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

// PublishGist hands the gist to Velvet, which keeps a copy beside the session.
//
// The session number is the one the job came with: Velvet numbers the row at
// /session start, hours before any gist, so there is nothing left to wait for
// or to confirm. A gist for a session that does not exist is Velvet's to
// refuse.
func PublishGist(ctx workflow.ActivityContext) (any, error) {
	var in publishRequest
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}

	gist := Gist{
		CampaignId:  in.CampaignId,
		EpisodeId:   in.EpisodeId,
		Summary:     in.Summary,
		EpisodeKey:  in.EpisodeKey,
		CampaignKey: in.CampaignKey,
	}
	if err := deps.Publisher.PublishEvent(ctx.Context(), deps.PubSubName, deps.GistTopic, gist); err != nil {
		return nil, retryable(err)
	}
	return nil, nil
}

// PublishFailure tells Velvet the episode will get no summary. Without it a
// workflow that dies after hours of whisper leaves the session's channel
// waiting for a gist that never comes.
func PublishFailure(ctx workflow.ActivityContext) (any, error) {
	var in Failure
	if err := ctx.GetInput(&in); err != nil {
		return nil, err
	}
	return nil, deps.Publisher.PublishEvent(ctx.Context(), deps.PubSubName, deps.FailureTopic, in)
}
