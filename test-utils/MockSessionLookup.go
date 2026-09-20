package test_utils

import (
	"context"
	"time"
)

// MockSessionLookup answers with a fixed session number or error.
type MockSessionLookup struct {
	Number int
	Err    error
	// Calls counts how many times the database was asked.
	Calls int

	// Separate from Number/Err: dating an episode and numbering it are
	// different questions, and the fallback path exists precisely for rows
	// where one succeeds and the other is irrelevant.
	Date     time.Time
	DateErr  error
	DateArgs [][2]int
}

func (m *MockSessionLookup) SessionNumber(_ context.Context, _, _ int) (int, error) {
	m.Calls++
	if m.Err != nil {
		return 0, m.Err
	}
	return m.Number, nil
}

func (m *MockSessionLookup) SessionDate(_ context.Context, campaignId, episodeIndex int) (time.Time, error) {
	m.DateArgs = append(m.DateArgs, [2]int{campaignId, episodeIndex})
	if m.DateErr != nil {
		return time.Time{}, m.DateErr
	}
	return m.Date, nil
}
