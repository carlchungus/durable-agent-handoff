package runstate

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
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

func TestSupervisorLeaseFencesStaleRecorder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempt.json")
	old, err := Create(path, Manifest{ID: "lead/1", WorkflowID: "wf_test", NodeID: "lead", Attempt: 1, Runtime: "codex", Worktree: t.TempDir(), SupervisorID: "old-owner", SupervisorGeneration: 7})
	if err != nil {
		t.Fatal(err)
	}
	old.mu.Lock()
	old.manifest.SupervisorLeaseExpiresAt = time.Now().Add(-time.Second)
	err = old.writeLocked()
	old.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := ClaimSupervisor(path, "new-owner", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if claimed.SupervisorGeneration != 8 || claimed.SupervisorID != "new-owner" {
		t.Fatalf("claimed=%+v", claimed)
	}
	if err = old.Update(func(m *Manifest) { m.Status = "completed" }); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale recorder err=%v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status == "completed" || loaded.SupervisorID != "new-owner" {
		t.Fatalf("stale recorder overwrote lease: %+v", loaded)
	}
}

func TestActiveSupervisorLeaseCannotBeStolen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempt.json")
	r, err := Create(path, Manifest{ID: "lead/1", WorkflowID: "wf_test", NodeID: "lead", Attempt: 1, Runtime: "codex", Worktree: t.TempDir(), SupervisorID: "owner-one"})
	if err != nil {
		t.Fatal(err)
	}
	before := r.Snapshot()
	got, ok, err := ClaimSupervisor(path, "owner-two", time.Minute)
	if err != nil || ok {
		t.Fatalf("stole active lease ok=%v err=%v", ok, err)
	}
	if got.SupervisorID != before.SupervisorID || got.SupervisorGeneration != before.SupervisorGeneration {
		t.Fatalf("active lease changed: before=%+v got=%+v", before, got)
	}
}
