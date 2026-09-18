package speakers

import (
	"encoding/base64"
	"testing"
)

func timeline() Timeline {
	return Timeline{
		RecordId: 119472335,
		Tracks: []Track{
			{Track: 1, UserId: "188626510901542912", Username: "shrecki",
				Intervals: []Interval{{Start: 0, End: 10}, {Start: 30, End: 40}}},
			{Track: 2, UserId: "152795063859150848", Username: "sotrx",
				Intervals: []Interval{{Start: 8, End: 20}}},
		},
	}
}

func TestSpeakerPicksWhoeverHeldTheFloor(t *testing.T) {
	// People talk over each other constantly, so overlapping tracks are the
	// normal case, not an edge case. 12-18 is 6s of sotrx against 0s of
	// shrecki, whose interval ended at 10.
	got := timeline().Speaker(12, 18)
	if got == nil || got.Username != "sotrx" {
		t.Fatalf("expected sotrx to hold the floor, got %v", got)
	}
}

func TestSpeakerPrefersTheLargerOverlapWhenBothSpoke(t *testing.T) {
	// 5-12: shrecki covers 5-10 (5s), sotrx covers 8-12 (4s).
	got := timeline().Speaker(5, 12)
	if got == nil || got.Username != "shrecki" {
		t.Fatalf("expected shrecki, got %v", got)
	}
}

func TestSpeakerIsUnknownWhenNobodyTransmitted(t *testing.T) {
	// whisper regularly emits text over stretches where no one was speaking --
	// music, room noise, or an outright hallucination. Those must not be
	// attributed to whoever happens to be first in the list.
	if got := timeline().Speaker(21, 29); got != nil {
		t.Fatalf("expected no speaker in a silent stretch, got %v", got)
	}
}

func TestSpeakerHandlesAnEmptyTimeline(t *testing.T) {
	if got := (Timeline{}).Speaker(0, 10); got != nil {
		t.Fatalf("expected no speaker, got %v", got)
	}
}

func TestLabelMarksTheGameMaster(t *testing.T) {
	track := Track{Track: 1, Username: "shrecki"}
	if got := track.Label(true); got != "shrecki (MJ)" {
		t.Errorf("got %q", got)
	}
	if got := track.Label(false); got != "shrecki" {
		t.Errorf("got %q", got)
	}
}

func TestLabelNamesAnUnrosteredTrack(t *testing.T) {
	// Recordings made before Pandora wrote a roster still have distinct
	// streams; they are separate people even when we cannot name them.
	if got := (Track{Track: 3}).Label(false); got != "Participant 3" {
		t.Errorf("got %q", got)
	}
}

func TestParseAcceptsPlainJson(t *testing.T) {
	raw := []byte(`{"recordId":1,"tracks":[{"track":1,"username":"a","intervals":[]}]}`)
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.RecordId != 1 || len(got.Tracks) != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestParseAcceptsBase64(t *testing.T) {
	// The recordings binding sets encodeBase64, so this is what a get actually
	// returns. Getting it wrong degrades to "no timeline", which is silent.
	raw := []byte(`{"recordId":7,"tracks":[]}`)
	encoded := []byte(base64.StdEncoding.EncodeToString(raw))

	got, err := Parse(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.RecordId != 7 {
		t.Fatalf("got %+v", got)
	}
}

func TestParseRejectsNonsense(t *testing.T) {
	if _, err := Parse([]byte("not a timeline")); err == nil {
		t.Fatal("expected an error")
	}
}
