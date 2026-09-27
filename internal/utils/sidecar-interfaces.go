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

// StateStore is the subset of the Dapr client used to read and conditionally
// write a state store key.
type StateStore interface {
	GetState(ctx context.Context, storeName, key string, meta map[string]string) (*client.StateItem, error)
	SaveStateWithETag(ctx context.Context, storeName, key string, data []byte, etag string, meta map[string]string, so ...client.StateOption) error
}

// Publisher is the subset of the Dapr client used to publish events.
type Publisher interface {
	PublishEvent(ctx context.Context, pubsubName, topicName string, data any, opts ...client.PublishEventOption) error
}
