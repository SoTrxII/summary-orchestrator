package whisper_lease

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"summary-orchestrator/internal/utils"
	"summary-orchestrator/pkg/transcriber"

	"github.com/dapr/go-sdk/client"
)

// Key of the single lease record in the state store. Dapr prefixes it with
// the app id, so it is private to this service.
const Key = "whisper-lease"

// StartGrace is how long a holder may own the lease without having attached
// a whisper task before it counts as abandoned.
//
// Between Acquire and Attach the workflow presigns the audio, counts the
// roster and queues the task. Queueing is the long one: whisper downloads and
// decodes the whole recording before it answers, which is why its client
// timeout is 15 minutes, and transientRetry allows 4 attempts with at most a
// minute between them -- about 63 minutes in the worst case. 90 leaves room
// without letting a workflow that died right after acquiring block every
// other episode for long.
const StartGrace = 90 * time.Minute

// MaxHold is how long a task may stay "processing" before it counts as
// orphaned rather than slow.
//
// It has to exist because whisper cannot tell the two apart: its task table
// survives a pod restart on a hostPath, so a task interrupted by one sits in
// "processing" forever, and several such rows are there today. Measured runs
// took 1.7x to 3.5x the audio's length -- a 3h03m session took up to 10h46m
// -- so 16h covers any session this table has played, with room to spare.
const MaxHold = 16 * time.Hour

// Record is what the state store holds. A zero Holder means free.
type Record struct {
	Holder string `json:"holder"`
	// The whisper task the holder is waiting on, or "" before it has one.
	TaskId string `json:"taskId"`
	// When the holder acquired the lease, then when it attached its latest
	// task: an episode that arrived as several chunks holds the lease across
	// all of them, and each chunk gets the full MaxHold.
	Since time.Time `json:"since"`
}

// StatusFunc asks whisper about a task. See transcriber.Transcriber.Status.
type StatusFunc func(ctx context.Context, taskId string) (string, error)

// Gate is a Lease kept in a Dapr state store.
//
// Every write is conditional on the version that was read (first-write
// concurrency with an ETag), so two workflows reading the same free lease
// cannot both take it: the second write is refused. With no ETag, a first-write
// save only succeeds if the key does not exist yet, which is what makes the
// very first acquisition safe too. Both behaviours were checked against the
// Redis store this runs on (2026-09-27): a no-ETag write to an existing key was
// refused with a 500, a stale ETag with a 409.
//
// Nothing is ever deleted. Release writes an empty record instead, so the key
// always has a version to compare against after its first use.
type Gate struct {
	storeName string
	store     utils.StateStore
	status    StatusFunc
	now       func() time.Time
}

func New(storeName string, store utils.StateStore, status StatusFunc) *Gate {
	return &Gate{storeName: storeName, store: store, status: status, now: time.Now}
}

func (g *Gate) Acquire(ctx context.Context, holder string) error {
	current, version, err := g.read(ctx)
	if err != nil {
		return err
	}
	switch current.Holder {
	case "":
	case holder:
		return nil
	default:
		abandoned, why, err := g.abandoned(ctx, current)
		if err != nil {
			return err
		}
		if !abandoned {
			return fmt.Errorf("%w: held by %s (task %q) since %s", ErrBusy,
				current.Holder, current.TaskId, current.Since.UTC().Format(time.RFC3339))
		}
		slog.Warn(fmt.Sprintf("[WhisperLease] :: reclaiming the lease from %s: %s", current.Holder, why))
	}
	if err := g.write(ctx, Record{Holder: holder, Since: g.now()}, version); err != nil {
		// Someone else wrote between the read and this write. They hold it now.
		return fmt.Errorf("%w: lost the race to another holder (%v)", ErrBusy, err)
	}
	return nil
}

func (g *Gate) Attach(ctx context.Context, holder, taskId string) error {
	current, version, err := g.read(ctx)
	if err != nil {
		return err
	}
	if current.Holder != holder {
		return fmt.Errorf("%w: now held by %q", ErrLost, current.Holder)
	}
	return g.write(ctx, Record{Holder: holder, TaskId: taskId, Since: g.now()}, version)
}

func (g *Gate) Release(ctx context.Context, holder string) error {
	current, version, err := g.read(ctx)
	if err != nil {
		return err
	}
	if current.Holder != holder {
		return nil
	}
	return g.write(ctx, Record{}, version)
}

// abandoned decides whether the current holder has stopped using whisper
// without releasing -- a workflow that failed between two activities, or a
// pod that died. Whisper's own answer is the evidence; a whisper that cannot be
// asked is an error, never a licence to reclaim, because guessing wrong there
// is exactly the concurrent run this exists to prevent.
func (g *Gate) abandoned(ctx context.Context, current Record) (bool, string, error) {
	held := g.now().Sub(current.Since)
	if current.TaskId == "" {
		if held > StartGrace {
			return true, fmt.Sprintf("no task attached after %s", held.Round(time.Minute)), nil
		}
		return false, "", nil
	}
	status, err := g.status(ctx, current.TaskId)
	if err != nil {
		return false, "", fmt.Errorf("asking whisper about task %s: %w", current.TaskId, err)
	}
	if !transcriber.Running(status) {
		return true, fmt.Sprintf("task %s is %s", current.TaskId, status), nil
	}
	if held > MaxHold {
		return true, fmt.Sprintf("task %s still %s after %s, orphaned by a whisper restart",
			current.TaskId, status, held.Round(time.Minute)), nil
	}
	return false, "", nil
}

func (g *Gate) read(ctx context.Context) (Record, string, error) {
	item, err := g.store.GetState(ctx, g.storeName, Key, nil)
	if err != nil {
		return Record{}, "", fmt.Errorf("reading the whisper lease: %w", err)
	}
	var current Record
	if item == nil || len(item.Value) == 0 {
		return current, "", nil
	}
	if err := json.Unmarshal(item.Value, &current); err != nil {
		return Record{}, "", fmt.Errorf("reading the whisper lease: %w", err)
	}
	return current, item.Etag, nil
}

func (g *Gate) write(ctx context.Context, next Record, version string) error {
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	return g.store.SaveStateWithETag(ctx, g.storeName, Key, raw, version, nil,
		client.WithConcurrency(client.StateConcurrencyFirstWrite))
}
