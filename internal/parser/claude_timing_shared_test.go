package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaudeSourceTimingSharedMarksMultiOwnerFiles(t *testing.T) {
	t.Parallel()
	main := []string{
		`{"type":"user","sessionId":"s1","uuid":"u1","timestamp":"2026-04-26T10:00:00Z","message":{"content":"root"}}`,
		`{"type":"assistant","sessionId":"s1","uuid":"a1","parentUuid":"u1","timestamp":"2026-04-26T10:00:05Z","message":{"content":[{"type":"text","text":"reply"}]}}`,
	}
	// Enough later turns that the parser splits the fork instead of treating it
	// as a retry.
	parent := "a1"
	for _, id := range []string{"2", "3", "4", "5"} {
		main = append(main,
			`{"type":"user","sessionId":"s1","uuid":"u`+id+`","parentUuid":"`+parent+`","timestamp":"2026-04-26T10:0`+id+`:00Z","message":{"content":"next `+id+`"}}`,
			`{"type":"assistant","sessionId":"s1","uuid":"a`+id+`","parentUuid":"u`+id+`","timestamp":"2026-04-26T10:0`+id+`:05Z","message":{"content":[{"type":"text","text":"answer `+id+`"}]}}`,
		)
		parent = "a" + id
	}
	fork := []string{
		`{"type":"user","sessionId":"s1","uuid":"fu1","parentUuid":"a1","timestamp":"2026-04-26T11:00:00Z","message":{"content":"fork question"}}`,
		`{"type":"assistant","sessionId":"s1","uuid":"fa1","parentUuid":"fu1","timestamp":"2026-04-26T11:00:03Z","message":{"content":[{"type":"text","text":"fork reply"}]}}`,
	}
	for _, tc := range []struct {
		name     string
		lines    []string
		sessions int
		shared   bool
	}{
		{"single owner", main, 1, false},
		{"in-file fork", append(append([]string(nil), main...), fork...), 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "s1.jsonl")
			require.NoError(t, os.WriteFile(path, []byte(strings.Join(tc.lines, "\n")+"\n"), 0o600))
			results, err := parseClaudeSession(path, "project", "local")
			require.NoError(t, err)
			require.Len(t, results, tc.sessions)
			for _, result := range results {
				assert.Equal(t, tc.shared, result.Session.SourceTimingShared, result.Session.ID)
			}
		})
	}
}

func TestClaudeSourceTimingSharedMarksReplayedParentPrefix(t *testing.T) {
	t.Parallel()
	_, forkPath := writeLineageFixture(t,
		"orig-1111.jsonl", lineageOriginalContent(),
		"fork-2222.jsonl", lineageForkContent(),
	)
	results, _, err := claudeParseFile(forkPath, "my_app", "local", claudeParseOptions{siblingLineage: true})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.True(t, results[0].Session.SourceTimingShared, "the replayed prefix belongs to the parent session")
}
