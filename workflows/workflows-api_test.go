package workflows

import (
	"reflect"
	"testing"
)

// A recording that dropped and resumed comes back as several keys, and the
// order they are given in is the order they were recorded in. Losing it would
// hand the agent a shuffled session.
func TestKeysKeepRecordingOrder(t *testing.T) {
	cases := []struct {
		name string
		in   EpisodeInput
		want []string
	}{
		{
			name: "single key",
			in:   EpisodeInput{AudioKey: "1234.ogg"},
			want: []string{"1234.ogg"},
		},
		{
			name: "several chunks",
			in:   EpisodeInput{AudioKeys: []string{"a.ogg", "b.ogg", "c.ogg"}},
			want: []string{"a.ogg", "b.ogg", "c.ogg"},
		},
		{
			name: "both forms, plural first",
			in:   EpisodeInput{AudioKeys: []string{"a.ogg"}, AudioKey: "b.ogg"},
			want: []string{"a.ogg", "b.ogg"},
		},
		{
			name: "empty entries are dropped",
			in:   EpisodeInput{AudioKeys: []string{"a.ogg", ""}},
			want: []string{"a.ogg"},
		},
		{
			name: "nothing at all",
			in:   EpisodeInput{},
			want: []string{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.in.Keys(); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("Keys() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestTasksMirrorKeys(t *testing.T) {
	in := EpisodeInput{TranscriptionIds: []string{"t1", "t2"}, TranscriptionId: "t3"}
	want := []string{"t1", "t2", "t3"}
	if got := in.Tasks(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Tasks() = %v, want %v", got, want)
	}
	if got := (EpisodeInput{}).Tasks(); len(got) != 0 {
		t.Fatalf("Tasks() on a fresh input = %v, want empty", got)
	}
}
