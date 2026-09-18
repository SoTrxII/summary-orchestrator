package test_utils

import "context"

// MockSessionLookup answers with a fixed session number or error.
type MockSessionLookup struct {
	Number int
	Err    error
	// Calls counts how many times the database was asked.
	Calls int
}

func (m *MockSessionLookup) SessionNumber(_ context.Context, _, _ int) (int, error) {
	m.Calls++
	if m.Err != nil {
		return 0, m.Err
	}
	return m.Number, nil
}
