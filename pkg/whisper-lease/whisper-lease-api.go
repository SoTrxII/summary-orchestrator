package whisper_lease

import (
	"context"
	"errors"
)

// ErrBusy means another episode holds whisper and is still using it. It is
// the signal the workflow's queueing retry policy waits on.
var ErrBusy = errors.New("whisper is busy")

// ErrLost means the lease was reclaimed from this holder, which only happens
// once it looked abandoned (see Gate.Acquire).
var ErrLost = errors.New("whisper lease lost")

// Lease lets one episode at a time use whisper.
//
// whisper will not do this itself: /speech-to-text-url hands each request to
// FastAPI's BackgroundTasks on a single worker with no lock anywhere, so two
// requests become two transcriptions in one process. Each peaks at 10-11 GiB
// against a 24Gi limit, so two of them get the pod OOM-killed along with
// hours of work on both. Two tables ending their sessions on the same evening
// is all it takes.
type Lease interface {
	// Acquire makes holder the lease's owner, or returns ErrBusy while the
	// current owner is still using whisper. Acquiring a lease the holder
	// already owns succeeds, so a retried call is harmless.
	Acquire(ctx context.Context, holder string) error
	// Attach records the whisper task the holder just started. It is what
	// lets anyone else tell a live holder from a dead one.
	Attach(ctx context.Context, holder, taskId string) error
	// Release gives the lease back. Releasing a lease someone else holds is a
	// no-op rather than an error: it was already reclaimed.
	Release(ctx context.Context, holder string) error
}
