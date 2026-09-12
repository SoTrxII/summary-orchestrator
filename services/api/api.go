package api

import (
	"fmt"
	"log/slog"
	"net/http"

	"summary-orchestrator/workflows"

	"github.com/dapr/durabletask-go/workflow"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Controller exposes the workflow over HTTP. processing-orchestrator posts
// here once a recording has finished cooking.
type Controller struct {
	client Scheduler
}

func NewController(client Scheduler) *Controller {
	return &Controller{client}
}

func (c *Controller) Register(r gin.IRouter) {
	r.GET("/health", c.Health)
	wf := r.Group("/workflows")
	{
		wf.POST("/episode", c.StartEpisode)
		wf.GET("/:id", c.Status)
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
