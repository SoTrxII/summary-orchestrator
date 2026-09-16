package summary_store

import (
	"context"
	"encoding/json"
	"fmt"

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
