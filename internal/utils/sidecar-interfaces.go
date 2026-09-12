package utils

import (
	"context"

	"github.com/dapr/go-sdk/client"
)

// Binder is the subset of the Dapr client used to talk to output bindings.
// Declared here rather than taking client.Client wholesale so the services
// can be tested without a sidecar.
type Binder interface {
	InvokeBinding(ctx context.Context, in *client.InvokeBindingRequest) (*client.BindingEvent, error)
}

// Publisher is the subset of the Dapr client used to publish events.
type Publisher interface {
	PublishEvent(ctx context.Context, pubsubName, topicName string, data any, opts ...client.PublishEventOption) error
}
