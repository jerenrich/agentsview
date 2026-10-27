//go:build !windows

package db

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimingCaptureRejectsNamedPipeWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	s := Session{ID: "codex:timing", Agent: "codex", FilePath: &path, FileSize: new(int64(1))}
	done := make(chan *timingEvidenceRecord, 1)
	go func() {
		r, _ := scanTimingCapture(t.Context(), &s, nil)
		done <- r
	}()
	select {
	case r := <-done:
		assert.Nil(t, r)
	case <-time.After(5 * time.Second):
		// Release a blocking open so this regression also cleans up on failure.
		writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			require.NoError(t, writer.Close())
		}
		require.FailNow(t, "timing capture blocked opening a named pipe")
	}
}

func TestTimingSourceOpenDoesNotBlockOnNamedPipeReplacement(t *testing.T) {
	// Exercise the open itself: an earlier stat may have observed a regular
	// file that was replaced with this FIFO before the descriptor was opened.
	path := filepath.Join(t.TempDir(), "replaced.jsonl")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	done := make(chan error, 1)
	go func() {
		file, err := openTimingSourceFile(path)
		if err == nil {
			err = file.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			require.NoError(t, writer.Close())
		}
		require.FailNow(t, "source replacement blocked the descriptor open")
	}
}

func TestTimingCaptureRegularSymlinkCompatibility(t *testing.T) {
	d := testDB(t)
	s := archivedTimingFixture(t, d)
	source, err := os.ReadFile(*s.FilePath)
	require.NoError(t, err)
	link := filepath.Join(t.TempDir(), "linked.jsonl")
	require.NoError(t, os.Symlink(*s.FilePath, link))
	s.FilePath = &link
	first := requireTimingCaptureMatchesFull(t, d, s, source)
	require.NoError(t, d.UpsertSession(t.Context(), first))
	source = append(source, []byte(`{"opaque":"append through symlink"}`+"\n")...)
	s = writeTimingSnapshot(t, s, source)
	next := requireTimingCaptureMatchesFull(t, d, s, source)
	assert.True(t, next.TimingCapture.Resumed)
}
