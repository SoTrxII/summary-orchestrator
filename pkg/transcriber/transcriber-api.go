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
}
