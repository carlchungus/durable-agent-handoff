package runstate

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRecorderPersistsAtomicUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempt.json")
	r, err := Create(path, Manifest{ID: "lead/1", WorkflowID: "wf_test", NodeID: "lead", Attempt: 1, Runtime: "codex", Worktree: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Update(func(m *Manifest) {
		m.Status = "running"
		m.PID = 42
		m.SessionID = "session-exact"
		m.EventOffset = 99
	}); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != Version || got.Status != "running" || got.SessionID != "session-exact" || got.EventOffset != 99 {
		t.Fatalf("manifest=%+v", got)
	}
	if _, err = os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary manifest remains: %v", err)
	}
}

func TestProcessMatchesUsesStartToken(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows process liveness uses the service manager")
	}
	pid := os.Getpid()
	token := ProcessStartToken(pid)
	if token == "" {
		t.Fatal("missing process start token")
	}
	if !ProcessMatches(Manifest{PID: pid, ProcessStartToken: token}) {
		t.Fatal("current process should match its token")
	}
	if ProcessMatches(Manifest{PID: pid, ProcessStartToken: token + " stale"}) {
		t.Fatal("PID reuse guard accepted a stale token")
	}
}

func TestCommandDigestIncludesArgumentBoundaries(t *testing.T) {
	a := CommandDigest("tool", []string{"ab", "c"})
	b := CommandDigest("tool", []string{"a", "bc"})
	if a == b {
		t.Fatal("argument boundaries must affect the digest")
	}
}
