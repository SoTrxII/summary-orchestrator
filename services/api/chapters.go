package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"summary-orchestrator/pkg/storyteller"
	summary_store "summary-orchestrator/pkg/summary-store"

	"github.com/gin-gonic/gin"
)

// Chapter is one entry of a video's chapter list, start in seconds.
type Chapter struct {
	Start int    `json:"start"`
	Title string `json:"title"`
}

// ChaptersResponse is what the chapters route answers. Chapters is empty, not
// an error, when the episode has nothing to place: a transcript archived
// before times were kept, or an answer that failed YouTube's rules.
type ChaptersResponse struct {
	CampaignId string    `json:"campaignId"`
	EpisodeId  string    `json:"episodeId"`
	Chapters   []Chapter `json:"chapters"`
}

// ChaptersRequest optionally carries the timed lines to place the scenes
// with. Absent, the episode's archived transcript is used; the archive
// backfill sends YouTube's captions instead, for the sessions transcribed
// before times were kept.
type ChaptersRequest struct {
	Lines []storyteller.Line `json:"lines"`
}

// ErrNoTranscript means the episode has no archived transcript to place its
// scenes with.
var ErrNoTranscript = errors.New("no transcript stored")

// ChapterStore is what making chapters reads and writes in the bucket.
type ChapterStore interface {
	LoadEpisode(ctx context.Context, campaignId, episodeId string) (summary_store.Summary, error)
	LoadTranscriptLines(ctx context.Context, key string) ([]storyteller.Line, error)
	SaveChapters(ctx context.Context, campaignId, episodeId string, chapters []byte) (string, error)
}

// ChapterService writes an episode's YouTube chapters on request. It is not a
// step of the summary workflow: processing-orchestrator asks once it holds
// both the gist and the video, and a failure here can never cost a summary.
type ChapterService struct {
	Agent storyteller.Storyteller
	Store ChapterStore
}

func (s *ChapterService) Make(ctx context.Context, campaignId, episodeId string, lines []storyteller.Line) (ChaptersResponse, error) {
	out := ChaptersResponse{CampaignId: campaignId, EpisodeId: episodeId, Chapters: []Chapter{}}
	episode, err := s.Store.LoadEpisode(ctx, campaignId, episodeId)
	if err != nil {
		return out, err
	}
	if len(lines) == 0 {
		lines, err = s.Store.LoadTranscriptLines(ctx, summary_store.TranscriptKey(campaignId, episodeId))
		if err != nil {
			return out, fmt.Errorf("%w: %s", ErrNoTranscript, err)
		}
	}
	raw, err := s.Agent.Chapters(ctx, campaignId, episodeId, episode.Summary, lines)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal([]byte(raw), &out.Chapters); err != nil {
		return out, fmt.Errorf("the storyteller answered something other than chapters: %w", err)
	}
	if len(out.Chapters) == 0 {
		return out, nil
	}
	stored, _ := json.Marshal(out.Chapters)
	if _, err := s.Store.SaveChapters(ctx, campaignId, episodeId, stored); err != nil {
		// The chapters exist and the caller can still publish them; the copy in
		// the bucket is a record, not the source.
		slog.Warn(fmt.Sprintf("[Api] :: chapters of %s/%s not stored : %s", campaignId, episodeId, err))
	}
	return out, nil
}

// EpisodeChapters writes, stores and returns an episode's YouTube chapters.
// A model call: about a minute, not the read routes' instant answer.
func (c *Controller) EpisodeChapters(ctx *gin.Context) {
	campaignId, ok := idParam(ctx, "campaign")
	if !ok {
		return
	}
	episodeId, ok := idParam(ctx, "episode")
	if !ok {
		return
	}
	// No body means "the archived transcript". Read before deciding: through
	// the Dapr sidecar an empty request can arrive with an unknown length.
	var in ChaptersRequest
	body, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	out, err := c.chapters.Make(ctx, campaignId, episodeId, in.Lines)
	switch {
	case err == nil:
		slog.Info(fmt.Sprintf("[Api] :: %d chapters for campaign %s, episode %s", len(out.Chapters), campaignId, episodeId))
		ctx.JSON(http.StatusOK, out)
	case errors.Is(err, summary_store.ErrNoSummary), errors.Is(err, ErrNoTranscript):
		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	default:
		slog.Error(fmt.Sprintf("[Api] :: chapters of %s/%s failed : %s", campaignId, episodeId, err))
		ctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
	}
}
