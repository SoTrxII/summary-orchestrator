package summary_store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"summary-orchestrator/internal/utils"

	"github.com/dapr/go-sdk/client"
)

// DaprSummaryStore writes summaries into the summaries bucket.
//
// The layout matches the keys summary-updater already documents in its gist
// model, so a consumer can fetch the full text from the key it is handed.
type DaprSummaryStore struct {
	componentName string
	client        utils.Binder
}

func New(componentName string, client utils.Binder) *DaprSummaryStore {
	return &DaprSummaryStore{componentName, client}
}

type storedSummary struct {
	CampaignId string `json:"campaignId"`
	EpisodeId  string `json:"episodeId,omitempty"`
	Summary    string `json:"summary"`
}

func EpisodeKey(campaignId, episodeId string) string {
	return fmt.Sprintf("campaigns/%s/episodes/%s/summary.json", campaignId, episodeId)
}

// TranscriptKey is where an episode's transcript is archived. It sits beside
// the summary written from it rather than in a bucket of its own, so the whole
// of what is known about an episode is under one prefix.
func TranscriptKey(campaignId, episodeId string) string {
	return fmt.Sprintf("campaigns/%s/episodes/%s/transcript.json", campaignId, episodeId)
}

// CastKey is where a campaign's cast list lives. It sits beside the campaign
// summary rather than under an episode: the cast is a property of the campaign,
// and every episode of it reads the same file.
func CastKey(campaignId string) string {
	return fmt.Sprintf("campaigns/%s/cast.json", campaignId)
}

// LoadCast reads the campaign's cast list, or reports that it has none.
//
// A missing cast is the normal case, not a failure: only campaigns somebody has
// curated have one, and the summary is merely better when it exists. Every error
// path therefore degrades to the empty string — an unreadable or half-edited file
// must not cost an episode its chapter.
func (s *DaprSummaryStore) LoadCast(ctx context.Context, campaignId string) string {
	key := CastKey(campaignId)
	res, err := s.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      s.componentName,
		Operation: "get",
		Metadata:  map[string]string{"key": key},
	})
	if err != nil {
		slog.Info(fmt.Sprintf("[SummaryStore] :: no cast for campaign %s (%s)", campaignId, err))
		return ""
	}
	// The summaries binding has decodeBase64 false, so this is the stored bytes
	// as written. Validated here rather than in the agent so a broken file is
	// reported by the service that can name the key.
	if !json.Valid(res.Data) {
		slog.Warn(fmt.Sprintf("[SummaryStore] :: %s is not valid JSON, ignoring it", key))
		return ""
	}
	slog.Info(fmt.Sprintf("[SummaryStore] :: loaded cast for campaign %s (%d bytes)", campaignId, len(res.Data)))
	return string(res.Data)
}

func (s *DaprSummaryStore) CampaignKey(campaignId string) string {
	return fmt.Sprintf("campaigns/%s/summary.json", campaignId)
}

func (s *DaprSummaryStore) SaveEpisode(ctx context.Context, campaignId, episodeId, summary string) (string, error) {
	key := EpisodeKey(campaignId, episodeId)
	return key, s.save(ctx, key, storedSummary{
		CampaignId: campaignId,
		EpisodeId:  episodeId,
		Summary:    summary,
	})
}

func (s *DaprSummaryStore) SaveCampaign(ctx context.Context, campaignId, summary string) (string, error) {
	key := s.CampaignKey(campaignId)
	return key, s.save(ctx, key, storedSummary{
		CampaignId: campaignId,
		Summary:    summary,
	})
}

// SaveTranscript archives a transcript. Unlike the summaries it does not go
// through save: the payload is a different shape, and a transcript is large
// enough that copying it into another struct to reuse one function is a poor
// trade.
func (s *DaprSummaryStore) SaveTranscript(ctx context.Context, t Transcript) (string, error) {
	key := TranscriptKey(t.CampaignId, t.EpisodeId)
	data, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	if err := s.put(ctx, key, data); err != nil {
		return "", err
	}
	return key, nil
}

// listedObject is the part of the S3 binding's "list" answer LoadTranscripts
// reads. Same binding shape as audio-store's RecordedAt: the field names are
// the SDK's ListObjectsV2Output, capitalised.
type listedObject struct {
	Key string `json:"Key"`
}

type listResponse struct {
	Contents []listedObject `json:"Contents"`
}

// transcriptsPrefix is where a campaign's episodes live in the bucket.
func transcriptsPrefix(campaignId string) string {
	return fmt.Sprintf("campaigns/%s/episodes/", campaignId)
}

// LoadTranscripts reads back every transcript archived for a campaign.
//
// Sorted by key for a stable order, not by episode number -- lexicographic
// sort puts episode 10 before episode 2, but drafting a cast votes on each
// episode independently (see marlin.cast.draft_cast), so the order they
// arrive in makes no difference to the result. Unlike ingesting a campaign
// into Graphiti, nothing here is order-sensitive.
//
// A transcript that fails to read or parse is skipped rather than failing the
// whole call: one corrupt archive must not cost the draft every other
// episode's evidence.
func (s *DaprSummaryStore) LoadTranscripts(ctx context.Context, campaignId string) ([]Transcript, error) {
	prefix := transcriptsPrefix(campaignId)
	payload, err := json.Marshal(map[string]any{"prefix": prefix, "maxResults": 1000})
	if err != nil {
		return nil, err
	}
	res, err := s.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      s.componentName,
		Operation: "list",
		Data:      payload,
	})
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", prefix, err)
	}

	var listed listResponse
	if err := json.Unmarshal(res.Data, &listed); err != nil {
		return nil, fmt.Errorf("listing %s: unreadable response: %w", prefix, err)
	}

	keys := make([]string, 0, len(listed.Contents))
	for _, o := range listed.Contents {
		if strings.HasSuffix(o.Key, "/transcript.json") {
			keys = append(keys, o.Key)
		}
	}
	sort.Strings(keys)

	transcripts := make([]Transcript, 0, len(keys))
	for _, key := range keys {
		res, err := s.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
			Name:      s.componentName,
			Operation: "get",
			Metadata:  map[string]string{"key": key},
		})
		if err != nil {
			slog.Warn(fmt.Sprintf("[SummaryStore] :: could not read %s, skipping it (%s)", key, err))
			continue
		}
		var t Transcript
		if err := json.Unmarshal(res.Data, &t); err != nil {
			slog.Warn(fmt.Sprintf("[SummaryStore] :: %s is not a valid transcript, skipping it (%s)", key, err))
			continue
		}
		transcripts = append(transcripts, t)
	}
	return transcripts, nil
}

// SaveCast stores a campaign's cast list. Used both for a human's corrected
// file and for the automatic draft -- the caller decides which one it is
// writing, this just puts bytes at the key.
func (s *DaprSummaryStore) SaveCast(ctx context.Context, campaignId, cast string) (string, error) {
	key := CastKey(campaignId)
	return key, s.put(ctx, key, []byte(cast))
}

func (s *DaprSummaryStore) save(ctx context.Context, key string, payload storedSummary) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.put(ctx, key, data)
}

func (s *DaprSummaryStore) put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      s.componentName,
		Operation: "create",
		Data:      data,
		Metadata:  map[string]string{"key": key},
	})
	if err != nil {
		return fmt.Errorf("storing %s: %w", key, err)
	}
	return nil
}
