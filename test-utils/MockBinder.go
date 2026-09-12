package test_utils

import (
	"context"

	"github.com/dapr/go-sdk/client"
)

// MockBinder stands in for the Dapr sidecar's binding channel.
type MockBinder struct {
	// Response returned to every call.
	Data []byte
	Err  error
	// Calls records what was asked of it, in order.
	Calls []*client.InvokeBindingRequest
}

func (m *MockBinder) InvokeBinding(_ context.Context, in *client.InvokeBindingRequest) (*client.BindingEvent, error) {
	m.Calls = append(m.Calls, in)
	if m.Err != nil {
		return nil, m.Err
	}
	return &client.BindingEvent{Data: m.Data}, nil
}
