package workflows

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"summary-orchestrator/internal/utils"
	audio_store "summary-orchestrator/pkg/audio-store"
	session_lookup "summary-orchestrator/pkg/session-lookup"
	"summary-orchestrator/pkg/storyteller"
	summary_store "summary-orchestrator/pkg/summary-store"
	"summary-orchestrator/pkg/transcriber"
	whisper_lease "summary-orchestrator/pkg/whisper-lease"

	"github.com/dapr/durabletask-go/workflow"
)

// WorkflowName is what the API schedules. Kept as a constant because the
// runtime addresses workflows by name, not by reference.
const WorkflowName = "SummarizeEpisode"

// EpisodeInput starts the whole thing. It is what processing-orchestrator
// posts once cooking is done.
type EpisodeInput struct {
	// Processing job this recording belongs to. Doubles as the workflow
	// instance id, which makes a re-submission of the same job a no-op.
	JobId string `json:"jobId"`
	// Campaign this episode belongs to.
	CampaignId int `json:"campaignId" binding:"required"`
	// Episode number within the campaign, as Velvet counted it.
	EpisodeId int `json:"episodeId" binding:"required"`
	// One-shots have no campaign narrative to update.
	IsOneShot bool `json:"isOneShot"`
	// Discord ids of whoever runs this campaign, supplied by the caller.
	//
	// Velvet owns the rpg database and reads playercampaign.isgm there, so the
	// answer travels on the job rather than this service opening its own
	// connection to a schema it does not own. Empty is normal -- it is every
	// recording made before this, and every hand-submitted re-run -- and means
	// nobody is marked, not that nobody runs the game.
	GameMasterIds []string `json:"gameMasterIds"`
	// Ids of whisper tasks that have already run, in the same order as the
	// keys they came from. Optional: set them to skip transcribing again and
	// collect those tasks' results instead, which is what makes re-running a
	// failed late step affordable rather than another half-day of CPU.
	TranscriptionIds []string `json:"transcriptionIds"`
	// Single-task form of TranscriptionIds, for the common one-chunk case.
	TranscriptionId string `json:"transcriptionId"`
	// When the session was played, RFC3339. Optional, and normally left empty:
	// the recording's own date is more trustworthy than anything a caller
	// types, and it is what the pipeline uses.
	//
	// It exists for the case the pipeline cannot answer -- re-summarizing an
	// archived episode whose recording has been pruned from the bucket. Without
	// it such an episode is stamped at ingestion, which puts the campaign's
	// oldest sessions *after* its newest ones in the graph's chronology and
	// hands every contradiction to the wrong episode.
	RecordedAt string `json:"recordedAt"`
	// Key of a transcript that already exists in the summaries bucket. Set it
	// to summarize from that instead of transcribing anything at all.
	//
	// This is how a campaign predating the current pipeline gets summarized.
	// The bucket holds 252 transcripts the pre-2026 summarizer wrote under
	// <campaign>/<episode>/transcript.json, and for 191 of them the recording
	// has since been pruned -- so this is not an optimisation for those, it is
	// the only way they can ever be summarized. Both shapes are accepted; see
	// summary_store.ParseTranscript.
	//
	// Wins over AudioKeys and TranscriptionIds: it is only ever set by hand,
	// and only when the caller knows the transcript is the one they want.
	TranscriptKey string `json:"transcriptKey"`
	// Keys of the cooked audio in the recordings bucket, in recording order.
	//
	// There is usually one, but not always: Pandora opens a new record every
	// time it reconnects to the voice channel, so a session that dropped
	// comes back as consecutive chunks. They are transcribed in turn and the
	// transcripts concatenated, which is why the order matters.
	AudioKeys []string `json:"audioKeys"`
	// Single-key form of AudioKeys.
	AudioKey string `json:"audioKey"`
	// Stop once the transcript is archived: no cast draft, no chapter, no
	// graph write, no gist.
	//
	// For recovering an episode of a campaign that is not being summarized
	// yet. Before this existed, the only way to transcribe without writing a
	// chapter was to call whisper directly -- past the whisper lease, so any
	// real episode ending meanwhile would run beside it. The archived
	// transcript is then replayed with TranscriptKey, in episode order, when
	// the campaign's turn comes.
	TranscribeOnly bool `json:"transcribeOnly"`
}

// Keys is the recording, whichever form the caller expressed it in. The
// singular field comes last so that a caller setting both gets the plural
// one first, which is the one that can describe a split recording.
func (e EpisodeInput) Keys() []string {
	return oneOrMany(e.AudioKeys, e.AudioKey)
}

// Tasks are the whisper tasks to collect instead of starting new ones. Empty
// means transcribe from scratch.
func (e EpisodeInput) Tasks() []string {
	return oneOrMany(e.TranscriptionIds, e.TranscriptionId)
}

func oneOrMany(many []string, one string) []string {
	out := make([]string, 0, len(many)+1)
	for _, v := range many {
		if v != "" {
			out = append(out, v)
		}
	}
	if one != "" {
		out = append(out, one)
	}
	return out
}

func (e EpisodeInput) campaign() string { return strconv.Itoa(e.CampaignId) }
func (e EpisodeInput) episode() string  { return strconv.Itoa(e.EpisodeId) }

// Gist is the payload summary-updater expects on the gists topic. Every field
// is required on its side, including the two keys it does not currently read.
type Gist struct {
	CampaignId  int    `json:"campaignId"`
	EpisodeId   int    `json:"episodeId"`
	Summary     string `json:"summary"`
	EpisodeKey  string `json:"episodeKey"`
	CampaignKey string `json:"campaignKey"`
}

// Deps are the services the activities run against. Activities are plain
// functions the runtime discovers by name, so they cannot be closures over a
// constructor's arguments; the wiring lives here instead.
type Deps struct {
	Audio       audio_store.AudioStore
	Transcriber transcriber.Transcriber
	Whisper     whisper_lease.Lease
	Agent       storyteller.Storyteller
	Summaries   summary_store.SummaryStore
	Sessions    session_lookup.SessionLookup
	Publisher   utils.Publisher
	PubSubName  string
	GistTopic   string
}

var deps Deps

// Register wires the dependencies and registers everything with the runtime.
func Register(r *workflow.Registry, d Deps) error {
	deps = d

	if err := r.AddWorkflow(SummarizeEpisode); err != nil {
		return err
	}
	activities := []workflow.Activity{
		PresignAudio,
		CountSpeakers,
		AcquireWhisper,
		AttachWhisper,
		ReleaseWhisper,
		StartTranscription,
		FetchTranscription,
		StoreTranscript,
		LoadTranscript,
		DraftCast,
		WriteEpisodeSummary,
		StoreEpisodeSummary,
		WriteCampaignSummary,
		StoreCampaignSummary,
		WriteGist,
		PublishGist,
	}
	for _, a := range activities {
		if err := r.AddActivity(a); err != nil {
			return err
		}
	}
	return nil
}

// retryableMarker tags a failure the workflow should wait out rather than give
// up on.
//
// Retry decisions cannot use errors.Is: an activity's error crosses the
// runtime boundary as a serialized string, so by the time a RetryPolicy's
// Handle sees it, the sentinel identity is gone and only the message survives.
const retryableMarker = "[retryable]"

func retryable(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s %w", retryableMarker, err)
}

func isRetryable(err error) bool {
	return err != nil && strings.Contains(err.Error(), retryableMarker)
}

// transientRetry covers ordinary blips: a sidecar restarting, a connection
// reset. Short and cheap.
var transientRetry = &workflow.RetryPolicy{
	MaxAttempts:          4,
	InitialRetryInterval: 5 * time.Second,
	BackoffCoefficient:   2,
	MaxRetryInterval:     1 * time.Minute,
}

// transcriptionRetry waits out the transcription itself.
//
// Whisper runs on CPU with int8 weights, and measured against real sessions it
// is roughly three times slower than real time: a 3h03m recording took between
// 8h28m and 10h46m. The ceiling is set for a long session plus room to spare,
// because giving up early throws away hours of work that cannot be resumed.
var transcriptionRetry = &workflow.RetryPolicy{
	MaxAttempts:          600,
	InitialRetryInterval: 1 * time.Minute,
	BackoffCoefficient:   1.5,
	MaxRetryInterval:     10 * time.Minute,
	RetryTimeout:         48 * time.Hour,
	Handle:               isRetryable,
}

// whisperQueueRetry waits for another episode to finish with whisper.
//
// The wait is a queue, so its length is other people's transcriptions: each
// runs 1.7x to 3.5x its session's length, 3-10h for a normal one. Three tables
// ending the same evening leaves the last waiting on two of those; a backlog
// submitted by hand can be longer still. 72h covers that without letting a
// wedged lease hold an episode forever -- the lease itself frees an abandoned
// holder well before this runs out (see whisper_lease.MaxHold).
//
// Polling every 5 minutes at most costs a state read and one whisper status
// call, and leaves whisper idle for at most 5 minutes between jobs that take
// hours.
var whisperQueueRetry = &workflow.RetryPolicy{
	MaxAttempts:          1000,
	InitialRetryInterval: 1 * time.Minute,
	BackoffCoefficient:   1.5,
	MaxRetryInterval:     5 * time.Minute,
	RetryTimeout:         72 * time.Hour,
	Handle:               isRetryable,
}

// campaignRetry waits for Graphiti to finish swallowing the episode.
//
// The campaign summary is rebuilt from the graph, so it cannot be written
// until the episode just stored is queryable — and add_episode is
// fire-and-forget on the server side. Two episodes of near-identical length
// took 48s and 5m41s, so the honest answer to "how long does ingestion take"
// is "minutes, and you cannot predict which".
//
// On a campaign's first episode this is the difference between a summary and
// a failure rather than between a fresh brief and a stale one: with nothing
// else in the group, the agent finds no episodes at all and refuses to write.
var campaignRetry = &workflow.RetryPolicy{
	MaxAttempts:          15,
	InitialRetryInterval: 20 * time.Second,
	BackoffCoefficient:   1.5,
	MaxRetryInterval:     2 * time.Minute,
	RetryTimeout:         20 * time.Minute,
}

// gistRetry waits for the rest of the pipeline to catch up. The session row
// this gist attaches to is only written once the video has finished
// uploading, which can happen well after the summary is ready.
var gistRetry = &workflow.RetryPolicy{
	MaxAttempts:          12,
	InitialRetryInterval: 30 * time.Second,
	BackoffCoefficient:   2,
	MaxRetryInterval:     10 * time.Minute,
	RetryTimeout:         2 * time.Hour,
	Handle:               isRetryable,
}

// noRetry is for the one step that must not run twice: writing the episode
// summary also stores it in the knowledge graph, and Graphiti will happily
// accept the same episode a second time.
//
// InitialRetryInterval is set even though MaxAttempts is 1 and no second
// attempt can happen: the runtime validates the whole policy before running
// the activity and rejects a zero interval outright, which fails the step for
// a reason that has nothing to do with its work.
var noRetry = &workflow.RetryPolicy{MaxAttempts: 1, InitialRetryInterval: 1 * time.Second}
