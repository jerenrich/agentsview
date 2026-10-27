package sync

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

// claudeForkTranscript has a long main branch and a later in-file fork from the
// first reply. The main branch's 30-minute turn belongs only to the main session.
func claudeForkTranscript(sessionID string) string {
	line := func(kind, uuid, parent, ts, text string) string {
		parentField := ""
		if parent != "" {
			parentField = `"parentUuid":"` + parent + `",`
		}
		content := `"` + text + `"`
		if kind == "assistant" {
			content = `[{"type":"text","text":"` + text + `"}]`
		}
		return `{"type":"` + kind + `","sessionId":"` + sessionID + `","uuid":"` + uuid + `",` + parentField +
			`"timestamp":"` + ts + `","message":{"role":"` + kind + `","content":` + content + `}}`
	}
	return strings.Join([]string{
		line("user", "u1", "", "2026-04-26T10:00:00Z", "root"),
		line("assistant", "a1", "u1", "2026-04-26T10:00:05Z", "root reply"),
		line("user", "u2", "a1", "2026-04-26T10:01:00Z", "main continue"),
		line("assistant", "a2", "u2", "2026-04-26T10:31:00Z", "main reply after thirty minutes"),
		line("user", "u3", "a2", "2026-04-26T10:32:00Z", "main continue two"),
		line("assistant", "a3", "u3", "2026-04-26T10:33:00Z", "main reply two"),
		line("user", "u4", "a3", "2026-04-26T10:34:00Z", "main continue three"),
		line("assistant", "a4", "u4", "2026-04-26T10:35:00Z", "main reply three"),
		line("user", "u5", "a4", "2026-04-26T10:36:00Z", "main continue four"),
		line("assistant", "a5", "u5", "2026-04-26T10:37:00Z", "main reply four"),
		line("user", "fu1", "a1", "2026-04-26T11:00:00Z", "fork question"),
		line("assistant", "fa1", "fu1", "2026-04-26T11:00:03Z", "fork reply"),
	}, "\n") + "\n"
}

func TestClaudeForkTimingIsNotSharedAcrossSessions(t *testing.T) {
	const sessionID = "f0f0f0f0-1111-4222-8333-444444444444"
	root := t.TempDir()
	path := filepath.Join(root, "project", sessionID+".jsonl")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(claudeForkTranscript(sessionID)), 0o600))
	database := openTestDB(t)
	engine := NewEngine(t.Context(), database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}},
		Machine:   "local",
	})
	t.Cleanup(engine.Close)
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))

	forkID := sessionID + "-fu1"
	fork, err := database.GetSessionFull(t.Context(), forkID)
	require.NoError(t, err)
	require.NotNil(t, fork, "the transcript must split into a fork session")
	require.Equal(t, string(parser.RelFork), fork.RelationshipType)

	raw, err := sql.Open("sqlite3", database.Path())
	require.NoError(t, err)
	defer raw.Close()
	for _, id := range []string{sessionID, forkID} {
		var payload string
		require.NoError(t, raw.QueryRowContext(t.Context(),
			"SELECT evidence FROM session_timing_evidence WHERE session_id=?", id).Scan(&payload))
		assert.Empty(t, payload, "%s must store a rejection, not whole-file evidence", id)
	}

	forkTiming, err := database.GetSessionTiming(t.Context(), forkID)
	require.NoError(t, err)
	require.NotNil(t, forkTiming)
	assert.NotEqual(t, "persisted", forkTiming.Coverage.Evidence)
	assert.NotEqual(t, "source", forkTiming.TimingSource)
	require.Len(t, forkTiming.Activity, 1, "the fork owns only its own prompt")
	assert.Equal(t, int64(3000), forkTiming.ActiveDurationMs)

	mainTiming, err := database.GetSessionTiming(t.Context(), sessionID)
	require.NoError(t, err)
	require.NotNil(t, mainTiming)
	assert.NotEqual(t, "source", mainTiming.TimingSource)
	for _, row := range mainTiming.Activity {
		assert.NotEqual(t, "2026-04-26T11:00:00Z", row.StartedAt, "the main session must not absorb the fork's turn")
	}
	// Source-only readers and older archives have no durable decision. Their
	// ownership lookup must load paths, including a fork retained in the trash.
	_, err = raw.ExecContext(t.Context(), "DELETE FROM session_timing_evidence")
	require.NoError(t, err)
	for _, trashed := range []bool{false, true} {
		if trashed {
			require.NoError(t, database.SoftDeleteSession(t.Context(), forkID))
		}
		fallback, err := database.GetSessionTiming(t.Context(), sessionID)
		require.NoError(t, err)
		assert.Empty(t, fallback.TimingSource)
		assert.Equal(t, mainTiming.ActiveDurationMs, fallback.ActiveDurationMs)
	}
}
