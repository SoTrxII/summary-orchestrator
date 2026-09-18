package test_utils

import "context"

// MockSessionLookup answers with a fixed session number or error.
type MockSessionLookup struct {
	Number int
	Err    error
	// Calls counts how many times the database was asked.
	Calls int
	// GMs are the Discord ids running the campaign, nil when unknown.
	GMs   map[string]bool
	GMErr error
}

// GameMasters answers from GMs, which is nil in most tests: a campaign whose
// game master is unknown must still transcribe, just without the marking.
func (m *MockSessionLookup) GameMasters(_ context.Context, _ int) (map[string]bool, error) {
	return m.GMs, m.GMErr
}

func (m *MockSessionLookup) SessionNumber(_ context.Context, _, _ int) (int, error) {
	m.Calls++
	if m.Err != nil {
		return 0, m.Err
	}
	return m.Number, nil
}
