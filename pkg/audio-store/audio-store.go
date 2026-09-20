package audio_store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"summary-orchestrator/pkg/speakers"

	"summary-orchestrator/internal/utils"

	"github.com/dapr/go-sdk/client"
)

// DaprAudioStore resolves keys in the recordings bucket to presigned URLs,
// using the S3 binding's "presign" operation.
//
// Presigning rather than fetching is deliberate: whisper downloads the audio
// itself, so a multi-hundred-megabyte recording never travels through this
// process nor through the Dapr sidecar's gRPC channel (which caps message
// size well below a full session).
type DaprAudioStore struct {
	componentName string
	ttl           string
	client        utils.Binder
}

func New(componentName, ttl string, client utils.Binder) *DaprAudioStore {
	return &DaprAudioStore{componentName, ttl, client}
}

type presignResponse struct {
	PresignUrl string `json:"presignURL"`
}

func (a *DaprAudioStore) PresignedUrl(ctx context.Context, key string) (string, error) {
	res, err := a.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      a.componentName,
		Operation: "presign",
		Metadata: map[string]string{
			"key":        key,
			"presignTTL": a.ttl,
		},
	})
	if err != nil {
		return "", fmt.Errorf("presigning %s: %w", key, err)
	}

	var parsed presignResponse
	if err := json.Unmarshal(res.Data, &parsed); err != nil {
		return "", fmt.Errorf("presigning %s: unreadable response: %w", key, err)
	}
	if parsed.PresignUrl == "" {
		return "", fmt.Errorf("presigning %s: empty url in response", key)
	}
	return parsed.PresignUrl, nil
}

// listResponse is the part of the S3 binding's "list" answer we read. The
// binding marshals the SDK's ListObjectsV2Output straight through, so the
// field names are the SDK's, capitalised, and LastModified is RFC3339.
type listResponse struct {
	Contents []struct {
		Key          string    `json:"Key"`
		LastModified time.Time `json:"LastModified"`
	} `json:"Contents"`
}

// RecordedAt reports when a recording landed in the bucket.
//
// There is no "stat" operation on the S3 binding -- create, get, delete and
// list are all of it -- so this lists by prefix and picks the exact key out of
// the answer. The prefix is not enough on its own: the sidecar files Pandora
// and cooking-server write share it, so "119472335.ogg" also matches
// "119472335.ogg.data" and "119472335.ogg.users", and those were written at
// different times.
//
// A missing date is not a failure. Older recordings whose objects have been
// pruned, and anything submitted by task id with no key at all, simply have
// none; the caller leaves the episode's reference time unset, which is what
// every episode stored before this existed did.
func (a *DaprAudioStore) RecordedAt(ctx context.Context, key string) (time.Time, error) {
	if key == "" {
		return time.Time{}, fmt.Errorf("no key to date")
	}

	payload, err := json.Marshal(map[string]any{"prefix": key, "maxResults": listPageSize})
	if err != nil {
		return time.Time{}, err
	}
	res, err := a.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      a.componentName,
		Operation: "list",
		Data:      payload,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("listing %s: %w", key, err)
	}

	var parsed listResponse
	if err := json.Unmarshal(res.Data, &parsed); err != nil {
		return time.Time{}, fmt.Errorf("listing %s: unreadable response: %w", key, err)
	}
	for _, object := range parsed.Contents {
		if object.Key == key {
			if object.LastModified.IsZero() {
				return time.Time{}, fmt.Errorf("listing %s: no last-modified date", key)
			}
			return object.LastModified, nil
		}
	}
	return time.Time{}, fmt.Errorf("listing %s: not in the bucket", key)
}

// listPageSize is how many objects to ask for when dating one. The prefix
// matches the recording plus its sidecars (.data, .users, .timeline.json), so
// a handful is enough -- but the exact-key scan above is what makes it correct
// rather than this number.
const listPageSize = 16

// usersSuffix is what Pandora names the roster it writes beside a recording.
const usersSuffix = ".users"

// timelineSuffix is what cooking-server names the speaker timeline it writes
// beside a recording once cooking is done.
const timelineSuffix = ".timeline.json"

// SpeakerTimeline fetches who was transmitting when.
//
// Unlike the roster, this is exact: it comes from the per-user Discord streams
// in the raw record rather than from anything inferred about the audio. It only
// exists for recordings cooked after cooking-server started writing it, since
// the raw record it is derived from is deleted once processing finishes.
func (a *DaprAudioStore) SpeakerTimeline(ctx context.Context, key string) (speakers.Timeline, error) {
	res, err := a.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      a.componentName,
		Operation: "get",
		Metadata:  map[string]string{"key": key + timelineSuffix},
	})
	if err != nil {
		return speakers.Timeline{}, fmt.Errorf("reading the speaker timeline of %s: %w", key, err)
	}
	return speakers.Parse(res.Data)
}

// SpeakerCount counts the people Pandora saw in the voice channel while a
// recording was being made. That is an upper bound on how many voices the
// audio can contain, which is exactly what diarization wants: left to guess,
// it over-segments and invents speakers that were never there.
//
// It is only ever an upper bound. The roster counts everyone who joined,
// including those who never said a word, so it must not be used as a minimum.
//
// A missing or unreadable roster is not a failure. Recordings archived before
// Pandora wrote one, or from another source entirely, simply have none; this
// reports 0, meaning "no idea", and the caller transcribes without a bound.
func (a *DaprAudioStore) SpeakerCount(ctx context.Context, key string) (int, error) {
	res, err := a.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      a.componentName,
		Operation: "get",
		Metadata:  map[string]string{"key": key + usersSuffix},
	})
	if err != nil {
		return 0, fmt.Errorf("reading the roster of %s: %w", key, err)
	}
	return countRoster(res.Data), nil
}

// parseRoster counts the people in Pandora's roster format.
//
// The file is not an object but the inside of one: Pandora appends a line per
// user as they join, so it reads `"0":{}\n,"1":{"id":…}` with no enclosing
// braces and no way to know in advance where it ends. Wrapping it is what
// makes it parseable. Index 0 is a placeholder carrying no id and is not a
// person. Anything unparseable counts as no information rather than as zero
// speakers, since the two mean very different things downstream.
// countRoster reads Pandora's roster, base64-wrapped or not.
//
// The recordings binding is configured with encodeBase64, so a get comes back
// base64-encoded rather than as the bytes that were stored. Rather than
// depending on that staying true, this tries the content as-is first and falls
// back to decoding it: the roster is never valid base64 by accident, and
// base64 is never a valid roster.
func countRoster(raw []byte) int {
	if people := parseRoster(raw); people > 0 {
		return people
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0
	}
	return parseRoster(decoded)
}

func parseRoster(raw []byte) int {
	body := strings.TrimSpace(string(raw))
	if body == "" {
		return 0
	}
	// Tolerate a real object too, in case the format is ever tidied up.
	if !strings.HasPrefix(body, "{") {
		body = "{" + body + "}"
	}

	var roster map[string]struct {
		Id string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &roster); err != nil {
		return 0
	}

	people := 0
	for _, entry := range roster {
		if entry.Id != "" {
			people++
		}
	}
	return people
}
