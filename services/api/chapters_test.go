package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"summary-orchestrator/pkg/storyteller"
	summary_store "summary-orchestrator/pkg/summary-store"

	"github.com/gin-gonic/gin"
)

type chapterStore struct {
	summary  string
	lines    []storyteller.Line
	saved    map[string]string
	loadedTx bool
}

func (s *chapterStore) LoadEpisode(_ context.Context, c, e string) (summary_store.Summary, error) {
	if s.summary == "" {
		return summary_store.Summary{}, summary_store.ErrNoSummary
	}
	return summary_store.Summary{CampaignId: c, EpisodeId: e, Summary: s.summary}, nil
}

func (s *chapterStore) LoadTranscriptLines(_ context.Context, key string) ([]storyteller.Line, error) {
	s.loadedTx = true
	if s.lines == nil {
		return nil, errors.New("object not found")
	}
	return s.lines, nil
}

func (s *chapterStore) SaveChapters(_ context.Context, c, e string, data []byte) (string, error) {
	s.saved[c+"/"+e] = string(data)
	return summary_store.ChaptersKey(c, e), nil
}

type chapterAgent struct {
	storyteller.Storyteller
	answer  string
	chapter string
	lines   []storyteller.Line
}

func (a *chapterAgent) Chapters(_ context.Context, _, _, chapter string, lines []storyteller.Line) (string, error) {
	a.chapter, a.lines = chapter, lines
	return a.answer, nil
}

func postChapters(t *testing.T, svc *ChapterService, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	NewController(&mockScheduler{}, &mockSummaries{}, svc).Register(r)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func at(s float64) *float64 { return &s }

func TestChaptersFromTheArchivedTranscript(t *testing.T) {
	store := &chapterStore{summary: "Le groupe arrive au lac.", saved: map[string]string{},
		lines: []storyteller.Line{{Text: "on y va", Start: at(0), End: at(2)}}}
	agent := &chapterAgent{answer: `[{"start":0,"title":"Avant la partie"},{"start":600,"title":"Le lac"},{"start":1200,"title":"La caverne"}]`}
	w := postChapters(t, &ChapterService{Agent: agent, Store: store}, "/campaigns/22/episodes/9/chapters", "")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body)
	}
	var out ChaptersResponse
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if len(out.Chapters) != 3 || out.Chapters[1] != (Chapter{600, "Le lac"}) {
		t.Fatalf("unexpected answer %+v", out)
	}
	if agent.chapter != "Le groupe arrive au lac." || !store.loadedTx || store.saved["22/9"] == "" {
		t.Fatalf("expected the stored chapter and transcript used and the result saved")
	}
}

// The archive backfill sends YouTube's captions: they replace the transcript,
// which has no times for any session transcribed before 0.5.0.
func TestChaptersFromLinesInTheRequest(t *testing.T) {
	store := &chapterStore{summary: "x", saved: map[string]string{}}
	agent := &chapterAgent{answer: `[]`}
	w := postChapters(t, &ChapterService{Agent: agent, Store: store}, "/campaigns/19/episodes/19/chapters",
		`{"lines":[{"speaker":"","text":"légende","start":5,"end":7}]}`)

	if w.Code != http.StatusOK || store.loadedTx || len(agent.lines) != 1 || *agent.lines[0].Start != 5 {
		t.Fatalf("expected the posted lines used, got %d %+v", w.Code, agent.lines)
	}
	if len(store.saved) != 0 || !strings.Contains(w.Body.String(), `"chapters":[]`) {
		t.Fatalf("no chapters: nothing stored, an empty list answered: %s", w.Body)
	}
}

func TestChaptersNeedASummaryAndATranscript(t *testing.T) {
	agent := &chapterAgent{answer: `[]`}
	if w := postChapters(t, &ChapterService{Agent: agent, Store: &chapterStore{}}, "/campaigns/1/episodes/1/chapters", ""); w.Code != http.StatusNotFound {
		t.Errorf("no summary: expected 404, got %d", w.Code)
	}
	if w := postChapters(t, &ChapterService{Agent: agent, Store: &chapterStore{summary: "x"}}, "/campaigns/1/episodes/1/chapters", ""); w.Code != http.StatusNotFound {
		t.Errorf("no transcript: expected 404, got %d", w.Code)
	}
	bad := &chapterAgent{answer: "désolé"}
	if w := postChapters(t, &ChapterService{Agent: bad, Store: &chapterStore{summary: "x", lines: []storyteller.Line{{Text: "a"}}}}, "/campaigns/1/episodes/1/chapters", ""); w.Code != http.StatusBadGateway {
		t.Errorf("unparseable agent answer: expected 502, got %d", w.Code)
	}
}
