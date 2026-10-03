package api

import (
	"context"

	summary_store "summary-orchestrator/pkg/summary-store"

	"github.com/dapr/durabletask-go/workflow"
)

// Scheduler is the slice of the workflow client the HTTP layer needs.
type Scheduler interface {
	ScheduleWorkflow(ctx context.Context, name string, opts ...workflow.NewWorkflowOptions) (string, error)
	FetchWorkflowMetadata(ctx context.Context, id string, opts ...workflow.FetchWorkflowMetadataOptions) (*workflow.WorkflowMetadata, error)
}

// SummaryReader is the slice of the summary store the HTTP layer needs: what
// has been written, and nothing that writes.
type SummaryReader interface {
	LoadEpisode(ctx context.Context, campaignId, episodeId string) (summary_store.Summary, error)
	LoadCampaign(ctx context.Context, campaignId string) (summary_store.Summary, error)
}

// StartResponse is returned when a summarization is accepted.
type StartResponse struct {
	WorkflowId string `json:"workflowId"`
	Message    string `json:"message"`
}
