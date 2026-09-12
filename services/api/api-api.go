package api

import (
	"context"

	"github.com/dapr/durabletask-go/workflow"
)

// Scheduler is the slice of the workflow client the HTTP layer needs.
type Scheduler interface {
	ScheduleWorkflow(ctx context.Context, name string, opts ...workflow.NewWorkflowOptions) (string, error)
	FetchWorkflowMetadata(ctx context.Context, id string, opts ...workflow.FetchWorkflowMetadataOptions) (*workflow.WorkflowMetadata, error)
}

// StartResponse is returned when a summarization is accepted.
type StartResponse struct {
	WorkflowId string `json:"workflowId"`
	Message    string `json:"message"`
}
