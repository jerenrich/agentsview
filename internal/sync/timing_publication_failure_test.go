package sync

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestFailedFullContentDoesNotPublishTiming(t *testing.T) {
	for _, mode := range []struct {
		name         string
		staged, bulk bool
	}{
		{name: "normal"}, {name: "staged", staged: true}, {name: "bulk staged", staged: true, bulk: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "rollout-2026-10-01T00-00-00-"+codexTimingID+".jsonl")
			prefix, tail := codexTimingRollout()
			initial := prefix + tail
			require.NoError(t, os.WriteFile(path, []byte(initial), 0o600))
			database := openTestDB(t)
			cfg := EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: {root}}, Machine: "local", DisableFilesystemProjectDiscovery: true, DiscardPendingWritesOnCancel: mode.bulk}
			if mode.staged {
				cfg.StagedCodexParseMinBytes = 1
				cfg.CodexStagingDir = t.TempDir()
			}
			engine := NewEngine(t.Context(), database, cfg)
			t.Cleanup(engine.Close)
			require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
			raw, err := sql.Open("sqlite3", database.Path())
			require.NoError(t, err)
			defer raw.Close()
			id := "codex:" + codexTimingID
			var beforeHash string
			require.NoError(t, raw.QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_evidence WHERE session_id=?", id).Scan(&beforeHash))
			beforeMessages, err := database.GetAllMessages(t.Context(), id)
			require.NoError(t, err)
			appendOnly := testjsonl.JoinJSONL(testjsonl.CodexMsgJSON("user", "Additional uncommitted request", "2026-10-01T01:00:00Z"), testjsonl.CodexMsgJSON("assistant", "Additional uncommitted answer", "2026-10-01T01:00:05Z"))
			require.NoError(t, os.WriteFile(path, []byte(initial+appendOnly), 0o600))
			_, err = raw.ExecContext(t.Context(), `CREATE TRIGGER reject_new_timing_content BEFORE INSERT ON messages WHEN NEW.content LIKE 'Additional uncommitted%' BEGIN SELECT RAISE(ABORT,'injected normalized content failure'); END`)
			require.NoError(t, err)
			engine.forceFullParse = true
			require.ErrorContains(t, engine.SyncPathsContext(t.Context(), []string{path}), "failures")
			var afterHash string
			require.NoError(t, raw.QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_evidence WHERE session_id=?", id).Scan(&afterHash))
			assert.Equal(t, beforeHash, afterHash, "a failed full/staged write must not publish its new source timing")
			afterMessages, err := database.GetAllMessages(t.Context(), id)
			require.NoError(t, err)
			assert.Equal(t, beforeMessages, afterMessages)
			_, err = raw.ExecContext(t.Context(), "DROP TRIGGER reject_new_timing_content")
			require.NoError(t, err)
			engine.forceFullParse = false
			require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
			require.NoError(t, raw.QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_evidence WHERE session_id=?", id).Scan(&afterHash))
			assert.NotEqual(t, beforeHash, afterHash, "a successful retry publishes timing before the source can become fresh")
			require.NoError(t, os.Remove(path))
			timing, err := database.GetSessionTiming(t.Context(), id)
			require.NoError(t, err)
			require.NotNil(t, timing)
			assert.Equal(t, "persisted", timing.Coverage.Evidence)
		})
	}
}

func TestFailedTimingPublicationRetriesWithoutForce(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "rollout-2026-10-01T00-00-00-"+codexTimingID+".jsonl")
	prefix, tail := codexTimingRollout()
	initial := prefix + tail
	require.NoError(t, os.WriteFile(path, []byte(initial), 0o600))
	database := openTestDB(t)
	engine := NewEngine(t.Context(), database, EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: {root}}, Machine: "local", DisableFilesystemProjectDiscovery: true})
	t.Cleanup(engine.Close)
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
	raw, err := sql.Open("sqlite3", database.Path())
	require.NoError(t, err)
	defer raw.Close()
	id := "codex:" + codexTimingID
	var beforeHash string
	require.NoError(t, raw.QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_evidence WHERE session_id=?", id).Scan(&beforeHash))
	additional := testjsonl.JoinJSONL(testjsonl.CodexMsgJSON("user", "Next request", "2026-10-01T01:00:00Z"), testjsonl.CodexMsgJSON("assistant", "Next answer", "2026-10-01T01:00:05Z"))
	require.NoError(t, os.WriteFile(path, []byte(initial+additional), 0o600))
	_, err = raw.ExecContext(t.Context(), `CREATE TRIGGER reject_timing_publication BEFORE INSERT ON session_timing_evidence BEGIN SELECT RAISE(ABORT,'injected timing publication failure'); END`)
	require.NoError(t, err)
	engine.forceFullParse = true
	require.ErrorContains(t, engine.SyncPathsContext(t.Context(), []string{path}), "failures")
	var afterHash string
	require.NoError(t, raw.QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_evidence WHERE session_id=?", id).Scan(&afterHash))
	assert.Equal(t, beforeHash, afterHash)
	assert.Less(t, database.GetSessionDataVersion(t.Context(), id), db.CurrentDataVersion(), "the already-committed new fingerprint cannot make failed publication fresh")
	_, err = raw.ExecContext(t.Context(), "DROP TRIGGER reject_timing_publication")
	require.NoError(t, err)
	engine.forceFullParse = false
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
	assert.Equal(t, db.CurrentDataVersion(), database.GetSessionDataVersion(t.Context(), id))
	require.NoError(t, raw.QueryRowContext(t.Context(), "SELECT file_hash FROM session_timing_evidence WHERE session_id=?", id).Scan(&afterHash))
	assert.NotEqual(t, beforeHash, afterHash)
}
