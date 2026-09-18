// Package speakers turns "SPEAKER_03" into a person.
//
// whisper's diarization only ever produces anonymous clusters, because the
// audio it is given is a mix: every participant flattened into one track. The
// identity is not lost in the audio, it is lost at the cooking step -- Discord
// delivers one stream per speaker, and cooking-server now records who was
// transmitting when, beside the recording, while the raw record still exists.
//
// Matching whisper's segments against that timeline replaces a guess with a
// Discord account, which rpg-db can then join to isgm. That is the whole point:
// a summary that knows who the game master is can tell a player character from
// an NPC, and a summary that knows a name belongs to a real person at the table
// can keep them out of the fiction.
package speakers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// Interval is a stretch of time, in seconds from the start of the recording,
// during which one person was transmitting.
type Interval struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

// Track is one participant's side of the recording.
type Track struct {
	Track int `json:"track"`
	// Discord snowflake. Empty for a track the roster did not name, which
	// happens for recordings made before Pandora wrote one.
	UserId          string     `json:"userId"`
	Username        string     `json:"username"`
	Intervals       []Interval `json:"intervals"`
	SpeakingSeconds float64    `json:"speakingSeconds"`
}

// Timeline is what cooking-server stored beside the recording.
type Timeline struct {
	RecordId int64   `json:"recordId"`
	Tracks   []Track `json:"tracks"`
}

// GameMasterSuffix marks the game master in a speaker label.
//
// The GM is the one distinction the summary genuinely needs -- every character
// voiced by anyone else is a player character, and every character voiced by
// the GM is an NPC -- and carrying it in the label avoids widening the Line
// contract shared with the agent for a single boolean.
const GameMasterSuffix = " (MJ)"

// Label is how this person should appear in the transcript handed to the agent.
func (t Track) Label(isGM bool) string {
	name := strings.TrimSpace(t.Username)
	if name == "" {
		// A track with no roster entry is still a distinct person, and saying
		// so beats collapsing them into whatever diarization guessed.
		name = fmt.Sprintf("Participant %d", t.Track)
	}
	if isGM {
		return name + GameMasterSuffix
	}
	return name
}

// Parse reads a timeline sidecar, base64-wrapped or not.
//
// The recordings binding is configured with encodeBase64, so a get returns
// base64 rather than the stored bytes. Trying the content as-is first and
// falling back to decoding keeps this correct either way: the sidecar is never
// valid base64 by accident, and base64 is never valid JSON.
func Parse(raw []byte) (Timeline, error) {
	timeline, err := unmarshal(raw)
	if err == nil {
		return timeline, nil
	}
	decoded, decodeErr := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if decodeErr != nil {
		return Timeline{}, err
	}
	return unmarshal(decoded)
}

func unmarshal(raw []byte) (Timeline, error) {
	var timeline Timeline
	if err := json.Unmarshal(raw, &timeline); err != nil {
		return Timeline{}, fmt.Errorf("parsing the speaker timeline: %w", err)
	}
	return timeline, nil
}

// Speaker returns whoever was talking for most of [start, end], or nil when the
// timeline cannot say.
//
// Most, rather than any: people talk over each other constantly at a table, so
// several tracks routinely overlap one whisper segment. Picking the largest
// overlap attributes the segment to whoever actually held the floor for it,
// which is also whose words whisper most likely transcribed.
func (t Timeline) Speaker(start, end float64) *Track {
	var best *Track
	var bestOverlap float64

	for i := range t.Tracks {
		overlap := t.Tracks[i].overlap(start, end)
		if overlap > bestOverlap {
			bestOverlap = overlap
			best = &t.Tracks[i]
		}
	}
	if bestOverlap <= 0 {
		// Silence in every track across the whole segment. whisper heard
		// something nobody transmitted -- most often music, room noise, or a
		// hallucination on a silent stretch.
		return nil
	}
	return best
}

func (t Track) overlap(start, end float64) float64 {
	var total float64
	for _, interval := range t.Intervals {
		lower := max(start, interval.Start)
		upper := min(end, interval.End)
		if upper > lower {
			total += upper - lower
		}
	}
	return total
}
