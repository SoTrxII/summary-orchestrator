package transcriber

import (
	"context"
	"errors"
)

// ErrStillRunning is returned by Result while whisper is still working on the
// task. It is the signal the workflow's retry policy waits on.
var ErrStillRunning = errors.New("transcription still running")

// Segment is one diarized chunk of speech.
type Segment struct {
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
	Speaker string  `json:"speaker"`
	Text    string  `json:"text"`
}

// Transcriber drives a whisperX-FastAPI instance.
//
// Transcription is asynchronous on the server side: Start queues a task and
// returns immediately, Result reports on it.
type Transcriber interface {
	// Start queues the transcription of an audio file reachable at audioUrl.
	//
	// maxSpeakers caps how many voices diarization is allowed to find. Pass 0
	// when that is unknown and it will be left to guess.
	Start(ctx context.Context, audioUrl string, maxSpeakers int) (taskId string, err error)
	// Result returns the diarized segments, or ErrStillRunning.
	Result(ctx context.Context, taskId string) ([]Segment, error)
	// AudioDuration is the length of the audio a task transcribed, in
	// seconds. A session recorded in several parts becomes one video with the
	// parts end to end, so this is what shifts a later part's times onto the
	// video's clock. The last segment's end is not it: a part can end on
	// minutes of silence.
	AudioDuration(ctx context.Context, taskId string) (float64, error)
	// Status reports whether a task is still occupying whisper: "queued",
	// "processing", "completed", "failed", or StatusMissing when whisper has
	// no record of it. An error means whisper could not be asked, which is
	// not the same as the task being gone.
	Status(ctx context.Context, taskId string) (string, error)
}

// StatusMissing is what Status reports for a task id whisper does not know.
const StatusMissing = "missing"

// Running reports whether a task in this status is still using whisper.
func Running(status string) bool {
	return status == "queued" || status == "processing"
}
