package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

func TestExecutionStartFileStdinUsesStrictV2Response(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	request := `{"idempotency_key":"arca-file-start-01","goal":"promote work","prompt":"secret stdin prompt","remote_root":"` + root + `","runtime":"codex","resume_id":"exact-thread","sandbox":"read-only","role":"arca-cloud"}`
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = write.WriteString(request); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	previous := os.Stdin
	os.Stdin = read
	t.Cleanup(func() {
		os.Stdin = previous
		_ = read.Close()
	})
	var out, errOut bytes.Buffer
	if err = run([]string{"execution", "start", "--state", state, "--file", "-", "--json"}, &out, &errOut); err != nil {
		t.Fatalf("start: %v stderr=%s", err, errOut.String())
	}
	var response struct {
		WorkflowID supervisor.WorkflowID `json:"workflow_id"`
		NodeID     supervisor.NodeID     `json:"node_id"`
	}
	if err = json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(out.Bytes(), &fields); err != nil || len(fields) != 2 {
		t.Fatalf("promotion response was not the exact flat envelope: %s err=%v", out.String(), err)
	}
	if response.WorkflowID == "" || response.NodeID == "" || strings.Contains(out.String(), "secret stdin prompt") {
		t.Fatalf("unexpected response=%s", out.String())
	}
	if err = run([]string{"execution", "start", "--state", state, "--file", "-", "--json"}, &out, &errOut); err == nil {
		t.Fatal("closed stdin unexpectedly accepted a second request")
	}
}

func TestStatusListReplyAndPauseUseSupervisorProjection(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := run([]string{"start", "--state", state, "--root", root, "--runtime", "codex", "--session", "exact-thread", "--prompt", "work", "--sandbox", "read-only", "--authorized-by", "human:test", "--idempotency-key", "cli-v2-start-01", "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var response struct {
		Execution supervisor.Execution `json:"execution"`
	}
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Execution.ID == "" {
		t.Fatal("missing execution")
	}
	out.Reset()
	if err := run([]string{"list", "--state", state, "--json"}, &out, &errOut); err != nil || !strings.Contains(out.String(), string(response.Execution.ID)) {
		t.Fatalf("list=%s err=%v", out.String(), err)
	}
	out.Reset()
	if err := run([]string{"status", string(response.Execution.ID), "--state", state}, &out, &errOut); err != nil || !strings.Contains(out.String(), "status=queued") {
		t.Fatalf("status=%s err=%v", out.String(), err)
	}
	out.Reset()
	if err := run([]string{"execution", "pause", "--state", state, "--workflow", string(response.Execution.WorkflowID), "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"workflow_id"`) || !strings.Contains(out.String(), `"existing":false`) {
		t.Fatalf("pause=%s", out.String())
	}
}

func TestPauseCLIWaitsForDurableExitWithoutMutatingWhilePolling(t *testing.T) {
	stateRoot, worktree, output := t.TempDir(), t.TempDir(), new(bytes.Buffer)
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	var startOut, errOut bytes.Buffer
	if err := run([]string{"start", "--state", stateRoot, "--root", worktree, "--runtime", "claude", "--prompt", "work", "--sandbox", "workspace-write", "--authorized-by", "human:test", "--idempotency-key", "pause-cli-start", "--json"}, &startOut, &errOut); err != nil {
		t.Fatal(err)
	}
	var started struct {
		Execution supervisor.Execution `json:"execution"`
	}
	if err := json.Unmarshal(startOut.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	store, err := supervisor.Open(stateRoot, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	activity := state.Activities[started.Execution.FirstActivity]
	attempt, _, err := store.PrepareAttempt(context.Background(), supervisor.PrepareAttemptInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, CommandDigest: "pause-cli", Outputs: supervisor.OutputIdentity{Stdout: "pause-cli-out", Stderr: "pause-cli-err"}, IdempotencyKey: "pause-cli-attempt"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RecordMilestone(context.Background(), supervisor.RecordMilestoneInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, Milestone: supervisor.Milestone{Kind: supervisor.MilestoneProcessSpawned, Process: &supervisor.ProcessIdentity{PID: 55, StartToken: "pause-cli-birth"}}, IdempotencyKey: "pause-cli-spawn"}); err != nil {
		t.Fatal(err)
	}
	before, err := store.Events(0)
	if err != nil {
		t.Fatal(err)
	}
	err = run([]string{"execution", "pause", "--state", stateRoot, "--workflow", string(started.Execution.WorkflowID), "--timeout", "100ms", "--json"}, output, &errOut)
	if !errors.Is(err, supervisor.ErrPausePending) {
		t.Fatalf("pause returned wrong error=%v", err)
	}
	after, err := store.Events(0)
	if err != nil || len(after) != len(before)+1 {
		t.Fatalf("projection polling mutated journal: before=%d after=%d err=%v", len(before), len(after), err)
	}
	state, _ = store.Projection()
	if state.Leases[attempt.LeaseID] == nil || !state.Leases[attempt.LeaseID].ReleasedAt.IsZero() {
		t.Fatal("CLI polling released the writer lease before terminal exit")
	}
}
