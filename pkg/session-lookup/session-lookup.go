package session_lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"summary-orchestrator/internal/utils"

	"github.com/dapr/go-sdk/client"
)

// The gist is keyed on (campaignId, sessionNumber), and sessionNumber is
// assigned by the database at insert time as MAX(sessionNumber) + 1, while
// Velvet independently guesses it as "number of existing sessions + 1". Those
// two agree only as long as no session was ever deleted, so the number is
// confirmed against the row itself rather than trusted from the caller.
const sessionQuery = `SELECT sessionNumber FROM session WHERE campaignId = $1 AND sessionNumber = $2`

// DaprSessionLookup reads the rpg database through its Dapr binding.
type DaprSessionLookup struct {
	componentName string
	client        utils.Binder
}

func New(componentName string, client utils.Binder) *DaprSessionLookup {
	return &DaprSessionLookup{componentName, client}
}

func (s *DaprSessionLookup) SessionNumber(ctx context.Context, campaignId, episodeIndex int) (int, error) {
	params, err := json.Marshal([]any{campaignId, episodeIndex})
	if err != nil {
		return 0, err
	}

	res, err := s.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      s.componentName,
		Operation: "query",
		Metadata: map[string]string{
			"sql":    sanitize(sessionQuery),
			"params": string(params),
		},
	})
	if err != nil {
		return 0, fmt.Errorf("looking up session %d/%d: %w", campaignId, episodeIndex, err)
	}

	// The binding answers with one array per row.
	var rows [][]any
	if err := json.Unmarshal(res.Data, &rows); err != nil {
		return 0, fmt.Errorf("looking up session %d/%d: unreadable response: %w", campaignId, episodeIndex, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return 0, ErrSessionNotReady
	}

	number, ok := rows[0][0].(float64)
	if !ok {
		return 0, fmt.Errorf("looking up session %d/%d: unexpected session number %v", campaignId, episodeIndex, rows[0][0])
	}
	return int(number), nil
}

// dateQuery reads the session's own timestamp. Same key as sessionQuery: the
// caller's episode index is the session number for every campaign whose rows
// have never been deleted, which is the only case where a date can be found
// this way anyway.
const dateQuery = `SELECT createdAt FROM session WHERE campaignId = $1 AND sessionNumber = $2`

func (s *DaprSessionLookup) SessionDate(ctx context.Context, campaignId, episodeIndex int) (time.Time, error) {
	params, err := json.Marshal([]any{campaignId, episodeIndex})
	if err != nil {
		return time.Time{}, err
	}

	res, err := s.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      s.componentName,
		Operation: "query",
		Metadata: map[string]string{
			"sql":    sanitize(dateQuery),
			"params": string(params),
		},
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("dating session %d/%d: %w", campaignId, episodeIndex, err)
	}

	var rows [][]any
	if err := json.Unmarshal(res.Data, &rows); err != nil {
		return time.Time{}, fmt.Errorf("dating session %d/%d: unreadable response: %w", campaignId, episodeIndex, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return time.Time{}, ErrSessionNotReady
	}

	// The binding renders a timestamp column as an RFC3339 string, not as a
	// number the way it does session numbers: [["2023-02-18T00:00:00Z"]].
	raw, ok := rows[0][0].(string)
	if !ok {
		return time.Time{}, fmt.Errorf("dating session %d/%d: unexpected timestamp %v", campaignId, episodeIndex, rows[0][0])
	}
	when, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("dating session %d/%d: unparseable timestamp %q: %w", campaignId, episodeIndex, raw, err)
	}
	if when.IsZero() {
		return time.Time{}, fmt.Errorf("dating session %d/%d: no date on the row", campaignId, episodeIndex)
	}
	return when, nil
}

// sanitize flattens a query onto one line: the sql travels as binding
// metadata, which is not the place for newlines and tabs.
func sanitize(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}
