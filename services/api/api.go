package api

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	summary_store "summary-orchestrator/pkg/summary-store"
	"summary-orchestrator/workflows"

	"github.com/dapr/durabletask-go/workflow"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Controller exposes the workflow over HTTP, and what it has written.
// processing-orchestrator posts here once a recording has finished cooking;
// Velvet reads the summaries back.
type Controller struct {
	client    Scheduler
	summaries SummaryReader
}

func NewController(client Scheduler, summaries SummaryReader) *Controller {
	return &Controller{client, summaries}
}

func (c *Controller) Register(r gin.IRouter) {
	r.GET("/health", c.Health)
	wf := r.Group("/workflows")
	{
		wf.POST("/episode", c.StartEpisode)
		wf.GET("/:id", c.Status)
	}
	campaign := r.Group("/campaigns/:campaign")
	{
		campaign.GET("/summary", c.CampaignSummary)
		campaign.GET("/episodes/:episode/summary", c.EpisodeSummary)
	}
}

func (c *Controller) Health(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

// StartEpisode schedules a summarization for one recording.
//
// The processing job id doubles as the workflow instance id, which makes the
// endpoint idempotent: a caller that retries, or an orchestrator that replays
// a job after a restart, joins the run already in flight instead of starting
// a second one.
func (c *Controller) StartEpisode(ctx *gin.Context) {
	var in workflows.EpisodeInput
	if err := ctx.ShouldBindJSON(&in); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// A recording to transcribe, or a transcript that already exists -- an
	// episode summarized from an archive has no audio to name, and for the
	// legacy episodes whose recording has been pruned there is none to name.
	if len(in.Keys()) == 0 && in.TranscriptKey == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "audioKey, audioKeys or transcriptKey is required"})
		return
	}
	if in.JobId == "" {
		in.JobId = uuid.NewString()
	}

	id, err := c.client.ScheduleWorkflow(ctx, workflows.WorkflowName,
		workflow.WithInstanceID(in.JobId),
		workflow.WithInput(in),
	)
	if err != nil {
		// An id that is already taken means the work is already happening.
		if meta, lookupErr := c.client.FetchWorkflowMetadata(ctx, in.JobId); lookupErr == nil && meta != nil {
			slog.Info(fmt.Sprintf("[Api] :: job %s is already being summarized", in.JobId))
			ctx.JSON(http.StatusOK, StartResponse{
				WorkflowId: in.JobId,
				Message:    "already running",
			})
			return
		}
		slog.Error(fmt.Sprintf("[Api] :: could not schedule job %s : %s", in.JobId, err))
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	slog.Info(fmt.Sprintf("[Api] :: summarizing job %s (campaign %d, episode %d)", id, in.CampaignId, in.EpisodeId))
	ctx.JSON(http.StatusAccepted, StartResponse{
		WorkflowId: id,
		Message:    fmt.Sprintf("summarizing campaign %d, episode %d", in.CampaignId, in.EpisodeId),
	})
}

func (c *Controller) Status(ctx *gin.Context) {
	meta, err := c.client.FetchWorkflowMetadata(ctx, ctx.Param("id"), workflow.WithFetchPayloads(true))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, meta)
}

// CampaignSummary returns the narrative of a whole campaign, as stored.
//
// Neither this route nor EpisodeSummary goes anywhere but the bucket: no model
// and no graph, so both keep answering while the storyteller or whisper is
// down, or busy with an episode for the next several hours.
func (c *Controller) CampaignSummary(ctx *gin.Context) {
	campaignId, ok := idParam(ctx, "campaign")
	if !ok {
		return
	}
	summary, err := c.summaries.LoadCampaign(ctx, campaignId)
	answerSummary(ctx, summary, err)
}

// EpisodeSummary returns the chapter written for one episode, as stored.
func (c *Controller) EpisodeSummary(ctx *gin.Context) {
	campaignId, ok := idParam(ctx, "campaign")
	if !ok {
		return
	}
	episodeId, ok := idParam(ctx, "episode")
	if !ok {
		return
	}
	summary, err := c.summaries.LoadEpisode(ctx, campaignId, episodeId)
	answerSummary(ctx, summary, err)
}

// idParam reads a campaign or episode number off the path and answers 400
// itself when it is not one.
//
// The ids end up in a bucket key, so nothing but a number may get through. It
// is handed back re-formatted for the same reason the check exists: "007"
// parses, and would name a key nothing was ever written to.
func idParam(ctx *gin.Context, name string) (string, bool) {
	id, err := strconv.ParseUint(ctx.Param(name), 10, 32)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("%s must be a number", name)})
		return "", false
	}
	return strconv.FormatUint(id, 10), true
}

// answerSummary tells the two ways of having no text apart: 404 is "nothing
// was written", which a caller shows as not summarized yet, and 502 is "the
// bucket could not be read", which it shows as an outage.
func answerSummary(ctx *gin.Context, summary summary_store.Summary, err error) {
	switch {
	case err == nil:
		ctx.JSON(http.StatusOK, summary)
	case errors.Is(err, summary_store.ErrNoSummary):
		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	default:
		slog.Error(fmt.Sprintf("[Api] :: could not read a summary : %s", err))
		ctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
	}
}
