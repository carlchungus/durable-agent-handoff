package finalize

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

func TestInspectDiffCountsTrackedAndUntrackedLines(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	git("init", "-q")
	git("config", "user.email", "test@example.com")
	git("config", "user.name", "Test")
	if err := os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "base")
	if err := os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("one\nchanged\nthree\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "new.txt"), []byte("a\nb\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stats, err := InspectDiff(context.Background(), ExecRunner{Dir: dir}, dir)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Files != 2 {
		t.Fatalf("files=%d paths=%v", stats.Files, stats.Paths)
	}
	if stats.Lines < 5 {
		t.Fatalf("lines=%d", stats.Lines)
	}
}

type scriptedRunner struct{ calls []string }

func (r *scriptedRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	call := name + " " + strings.Join(args, " ")
	r.calls = append(r.calls, call)
	switch {
	case call == "git status --porcelain=v1 -z":
		return nil, nil
	case call == "git diff --numstat HEAD":
		return nil, nil
	case call == "git branch --show-current":
		return []byte("feature/safe\n"), nil
	case strings.HasPrefix(call, "gh pr view feature/safe "):
		return []byte("https://example.test/pr/7\n"), nil
	case strings.Contains(call, "--json number,url,headRefOid,mergeStateStatus,statusCheckRollup"):
		return []byte(`{"number":7,"url":"https://example.test/pr/7","headRefOid":"deadbeef","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"verify","status":"COMPLETED","conclusion":"SUCCESS"}]}`), nil
	case strings.HasPrefix(call, "gh pr merge "):
		return []byte("merged"), nil
	default:
		return []byte(call), errors.New("unexpected command")
	}
}

func TestExecuteMergesOnlyAfterAttestationAndNamedGate(t *testing.T) {
	now := time.Now().UTC()
	w := &core.Workflow{ID: "wf", Root: t.TempDir(), Budget: core.DefaultBudget(), Attestations: []core.Attestation{{ID: "a", NodeID: "verify", Verifier: "independent", Verdict: "pass"}}, Nodes: map[string]*core.Node{}, CreatedAt: now, UpdatedAt: now}
	n := &core.Node{ID: "finish", Title: "ship", Kind: "finalize", Metadata: map[string]string{"repo": "owner/repo", "gates": "verify"}}
	r := &scriptedRunner{}
	result, err := Execute(context.Background(), r, w, n)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Merged || result.HeadSHA != "deadbeef" {
		t.Fatalf("result=%#v", result)
	}
	found := false
	for _, call := range r.calls {
		if strings.Contains(call, "--match-head-commit deadbeef") {
			found = true
		}
	}
	if !found {
		t.Fatalf("merge was not pinned to head: %v", r.calls)
	}
}

func TestExecuteRefusesWithoutIndependentAttestation(t *testing.T) {
	w := &core.Workflow{Budget: core.DefaultBudget()}
	_, err := Execute(context.Background(), &scriptedRunner{}, w, &core.Node{})
	if err == nil || !strings.Contains(err.Error(), "attestation") {
		t.Fatalf("err=%v", err)
	}
}
