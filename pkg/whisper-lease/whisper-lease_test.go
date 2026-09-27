package whisper_lease

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"summary-orchestrator/pkg/transcriber"

	"github.com/dapr/go-sdk/client"
)

// fakeStore behaves like the Redis state store as measured on 2026-09-27: a
// first-write save with no ETag only succeeds on a missing key, and with an
// ETag only when it matches the current version.
type fakeStore struct {
	value   []byte
	version int
	// Runs just before a save is judged, to let a test slip a competing
	// write in between another holder's read and its write.
	beforeSave func()
	readErr    error
}

func (f *fakeStore) GetState(_ context.Context, _, _ string, _ map[string]string) (*client.StateItem, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	if f.value == nil {
		return &client.StateItem{}, nil
	}
	return &client.StateItem{Value: f.value, Etag: strconv.Itoa(f.version)}, nil
}

func (f *fakeStore) SaveStateWithETag(_ context.Context, _, _ string, data []byte, etag string, _ map[string]string, _ ...client.StateOption) error {
	if hook := f.beforeSave; hook != nil {
		f.beforeSave = nil
		hook()
	}
	if etag == "" && f.value != nil {
		return errors.New("rpc error: code = Unknown desc = failed saving state: key already exists")
	}
	if etag != "" && etag != strconv.Itoa(f.version) {
		return errors.New("rpc error: code = Aborted desc = possible etag mismatch")
	}
	f.value = data
	f.version++
	return nil
}

func (f *fakeStore) record(t *testing.T) Record {
	t.Helper()
	var r Record
	if f.value != nil {
		if err := json.Unmarshal(f.value, &r); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// whisperSays answers every status question with the same thing.
func whisperSays(status string, err error) StatusFunc {
	return func(context.Context, string) (string, error) { return status, err }
}

func newGate(store *fakeStore, status StatusFunc, now time.Time) *Gate {
	g := New("statestore", store, status)
	g.now = func() time.Time { return now }
	return g
}

var noon = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

// The case this exists for: two tables end their sessions the same evening.
// The second must wait, not start a second transcription beside the first.
func TestSecondEpisodeWaitsWhileTheFirstTranscribes(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("processing", nil), noon)

	if err := g.Acquire(context.Background(), "c22-ep9"); err != nil {
		t.Fatalf("a free lease must be taken: %v", err)
	}
	if err := g.Attach(context.Background(), "c22-ep9", "task-1"); err != nil {
		t.Fatal(err)
	}
	if err := g.Acquire(context.Background(), "c29-ep11"); !errors.Is(err, ErrBusy) {
		t.Fatalf("expected ErrBusy while task-1 runs, got %v", err)
	}
	if got := store.record(t); got.Holder != "c22-ep9" || got.TaskId != "task-1" {
		t.Fatalf("the holder must be untouched, got %+v", got)
	}
}

func TestReleaseLetsTheNextEpisodeIn(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("processing", nil), noon)
	_ = g.Acquire(context.Background(), "a")

	if err := g.Release(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := g.Acquire(context.Background(), "b"); err != nil {
		t.Fatalf("a released lease must be free: %v", err)
	}
}

// An activity's reply can be lost after its write landed, and the runtime
// then runs it again. That retry must not find itself locked out by itself.
func TestAcquiringTwiceIsHarmless(t *testing.T) {
	g := newGate(&fakeStore{}, whisperSays("processing", nil), noon)
	_ = g.Acquire(context.Background(), "a")

	if err := g.Acquire(context.Background(), "a"); err != nil {
		t.Fatalf("re-acquiring one's own lease must succeed: %v", err)
	}
}

// A holder that finished but never released -- its workflow failed between
// FetchTranscription and Release -- must not block whisper forever.
func TestAFinishedTaskFreesTheLease(t *testing.T) {
	for _, status := range []string{"completed", "failed", transcriber.StatusMissing} {
		t.Run(status, func(t *testing.T) {
			store := &fakeStore{}
			g := newGate(store, whisperSays(status, nil), noon)
			_ = g.Acquire(context.Background(), "a")
			_ = g.Attach(context.Background(), "a", "task-1")

			if err := g.Acquire(context.Background(), "b"); err != nil {
				t.Fatalf("a %s task must free the lease: %v", status, err)
			}
			if got := store.record(t); got.Holder != "b" || got.TaskId != "" {
				t.Fatalf("expected b holding with no task yet, got %+v", got)
			}
		})
	}
}

// Whisper's table keeps a task interrupted by a restart in "processing"
// forever, so "processing" alone cannot hold the lease indefinitely.
func TestAnOrphanedTaskFreesTheLeaseAfterMaxHold(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("processing", nil), noon)
	_ = g.Acquire(context.Background(), "a")
	_ = g.Attach(context.Background(), "a", "task-1")

	g.now = func() time.Time { return noon.Add(MaxHold - time.Minute) }
	if err := g.Acquire(context.Background(), "b"); !errors.Is(err, ErrBusy) {
		t.Fatalf("a long job is not an orphan yet, got %v", err)
	}
	g.now = func() time.Time { return noon.Add(MaxHold + time.Minute) }
	if err := g.Acquire(context.Background(), "b"); err != nil {
		t.Fatalf("expected the orphan to be reclaimed, got %v", err)
	}
}

// A workflow that died between Acquire and Attach leaves a lease with no task
// to ask whisper about. Only time can tell that apart from a slow upload.
func TestALeaseWithNoTaskIsReclaimedAfterTheGrace(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("", errors.New("must not be asked")), noon)
	_ = g.Acquire(context.Background(), "a")

	g.now = func() time.Time { return noon.Add(StartGrace - time.Minute) }
	if err := g.Acquire(context.Background(), "b"); !errors.Is(err, ErrBusy) {
		t.Fatalf("still within the grace, got %v", err)
	}
	g.now = func() time.Time { return noon.Add(StartGrace + time.Minute) }
	if err := g.Acquire(context.Background(), "b"); err != nil {
		t.Fatalf("expected the abandoned lease to be reclaimed, got %v", err)
	}
}

// Not knowing is not the same as the task being gone. Reclaiming on a failed
// status call would start the very concurrent run this prevents.
func TestAnUnreachableWhisperNeverFreesTheLease(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("", errors.New("connection refused")), noon)
	_ = g.Acquire(context.Background(), "a")
	_ = g.Attach(context.Background(), "a", "task-1")

	if err := g.Acquire(context.Background(), "b"); err == nil {
		t.Fatal("expected an error while whisper cannot be asked")
	}
	if got := store.record(t); got.Holder != "a" {
		t.Fatalf("the lease must stay with a, got %+v", got)
	}
}

// Two workflows read the same free lease at the same moment. Only one of the
// two conditional writes can land.
func TestOnlyOneOfTwoSimultaneousAcquiresWins(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("processing", nil), noon)
	// Used and released once, so both contenders compare against a version.
	_ = g.Acquire(context.Background(), "earlier")
	_ = g.Release(context.Background(), "earlier")
	store.beforeSave = func() {
		if err := g.Acquire(context.Background(), "b"); err != nil {
			t.Fatalf("b got in first and must win: %v", err)
		}
	}

	if err := g.Acquire(context.Background(), "a"); !errors.Is(err, ErrBusy) {
		t.Fatalf("a read the lease before b took it and must lose, got %v", err)
	}
	if got := store.record(t); got.Holder != "b" {
		t.Fatalf("expected b holding, got %+v", got)
	}
}

// The same race on the very first use, when the key does not exist yet and
// there is no version to compare: it must still come down to one winner.
func TestTheFirstEverAcquireIsRaceSafeToo(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("processing", nil), noon)
	store.beforeSave = func() { _ = g.Acquire(context.Background(), "b") }

	if err := g.Acquire(context.Background(), "a"); !errors.Is(err, ErrBusy) {
		t.Fatalf("expected a to lose the insert, got %v", err)
	}
}

func TestAttachAfterBeingReclaimedReportsTheLoss(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("processing", nil), noon)
	_ = g.Acquire(context.Background(), "a")
	g.now = func() time.Time { return noon.Add(StartGrace + time.Minute) }
	_ = g.Acquire(context.Background(), "b")

	if err := g.Attach(context.Background(), "a", "task-1"); !errors.Is(err, ErrLost) {
		t.Fatalf("expected ErrLost, got %v", err)
	}
}

// A late Release from a holder that was already reclaimed must not free the
// lease out from under whoever holds it now.
func TestReleaseByAFormerHolderLeavesTheLeaseAlone(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("completed", nil), noon)
	_ = g.Acquire(context.Background(), "a")
	_ = g.Attach(context.Background(), "a", "task-1")
	_ = g.Acquire(context.Background(), "b")

	if err := g.Release(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if got := store.record(t); got.Holder != "b" {
		t.Fatalf("b's lease was released by a: %+v", got)
	}
}

// Each chunk of a split session is its own whisper task, and each gets the
// full MaxHold rather than sharing one clock with the chunks before it.
func TestAttachingTheNextChunkRestartsTheClock(t *testing.T) {
	store := &fakeStore{}
	g := newGate(store, whisperSays("processing", nil), noon)
	_ = g.Acquire(context.Background(), "a")
	_ = g.Attach(context.Background(), "a", "chunk-1")

	later := noon.Add(10 * time.Hour)
	g.now = func() time.Time { return later }
	_ = g.Attach(context.Background(), "a", "chunk-2")

	g.now = func() time.Time { return later.Add(MaxHold - time.Minute) }
	if err := g.Acquire(context.Background(), "b"); !errors.Is(err, ErrBusy) {
		t.Fatalf("chunk 2 is within its own MaxHold, got %v", err)
	}
}
