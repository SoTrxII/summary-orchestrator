package session_lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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

// Velvet registers every participant against the campaign, GM included, so the
// flag lives here rather than anywhere in the recording pipeline. Discord ids
// are text, which is what the timeline sidecar carries too.
const gameMasterQuery = `SELECT playerid FROM playercampaign WHERE campaignid = $1 AND isgm = true`

func (s *DaprSessionLookup) GameMasters(ctx context.Context, campaignId int) (map[string]bool, error) {
	params, err := json.Marshal([]any{campaignId})
	if err != nil {
		return nil, err
	}

	res, err := s.client.InvokeBinding(ctx, &client.InvokeBindingRequest{
		Name:      s.componentName,
		Operation: "query",
		Metadata: map[string]string{
			"sql":    sanitize(gameMasterQuery),
			"params": string(params),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("looking up the game masters of campaign %d: %w", campaignId, err)
	}

	var rows [][]any
	if err := json.Unmarshal(res.Data, &rows); err != nil {
		return nil, fmt.Errorf("looking up the game masters of campaign %d: unreadable response: %w", campaignId, err)
	}

	gms := make(map[string]bool, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		// Discord ids exceed float64's exact integer range, which is why the
		// column is text and why this must never be parsed as a number.
		if id, ok := row[0].(string); ok && id != "" {
			gms[id] = true
		}
	}
	return gms, nil
}

// sanitize flattens a query onto one line: the sql travels as binding
// metadata, which is not the place for newlines and tabs.
func sanitize(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}
