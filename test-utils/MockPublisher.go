package test_utils

import (
	"context"

	"github.com/dapr/go-sdk/client"
)

// Published is one captured PublishEvent call.
type Published struct {
	PubSub string
	Topic  string
	Data   any
}

// MockPublisher records everything published to it.
type MockPublisher struct {
	Err    error
	Events []Published
}

func (m *MockPublisher) PublishEvent(_ context.Context, pubsubName, topicName string, data any, _ ...client.PublishEventOption) error {
	if m.Err != nil {
		return m.Err
	}
	m.Events = append(m.Events, Published{pubsubName, topicName, data})
	return nil
}
