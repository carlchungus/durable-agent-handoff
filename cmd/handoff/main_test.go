package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/supervisor"
)

func TestExecutionStartFileStdinUsesStrictV2Response(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(executionStartRequest{IdempotencyKey: "arca-file-start-01", Goal: "promote work", Prompt: "secret stdin prompt", RemoteRoot: root, Runtime: "codex", ResumeID: "exact-thread", Sandbox: supervisor.SandboxReadOnly, Role: "arca-cloud", FinalizerEnabled: true, FinalizerRequiredChecks: []string{"verify"}, FinalizerRequireHuman: true, Autonomous: true, EvaluatorModel: "deepseek/deepseek-v4-flash-0731", MaxTurns: 100})
	if err != nil {
		t.Fatal(err)
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = write.Write(request); err != nil {
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
	store, err := supervisor.Open(state, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if finalizer := projection.Workflows[response.WorkflowID].Finalizer; !finalizer.Enabled || !finalizer.RequireHuman || len(finalizer.RequiredChecks) != 1 || finalizer.RequiredChecks[0] != "verify" {
		t.Fatalf("promotion finalizer was not persisted immutably: %+v", finalizer)
	}
	if autonomy := projection.Workflows[response.WorkflowID].Autonomy; !autonomy.Enabled || autonomy.EvaluatorModel != "deepseek/deepseek-v4-flash-0731" || autonomy.MaxTurns != 100 {
		t.Fatalf("promotion autonomy was not persisted immutably: %+v", autonomy)
	}
	divergent := strings.Replace(string(request), `"promote work"`, `"different work"`, 1)
	out.Reset()
	if err = runWithPrompt(t, []string{"execution", "start", "--state", state, "--file", "-", "--json"}, divergent, &out, &errOut); !errors.Is(err, supervisor.ErrIdempotencyConflict) {
		t.Fatalf("divergent strict idempotency reuse was not rejected: %v", err)
	}
	if err = run([]string{"execution", "start", "--state", state, "--file", "-", "--json"}, &out, &errOut); err == nil {
		t.Fatal("closed stdin unexpectedly accepted a second request")
	}
}

func TestOrdinaryStartPersistsAdvertisedFinalizerAndRejectsIncompleteConfig(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := runWithPrompt(t, []string{"start", "--state", state, "--root", root, "--runtime", "codex", "--file", "-", "--sandbox", "read-only", "--authorized-by", "human:test", "--idempotency-key", "cli-finalizer-start", "--finalizer-enabled", "--required-check", "verify", "--require-human", "--json"}, "ship", &out, &errOut); err != nil {
		t.Fatalf("ordinary finalizer start: %v stderr=%s", err, errOut.String())
	}
	var response ordinaryStartResponse
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	store, err := supervisor.Open(state, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if finalizer := projection.Workflows[response.Execution.WorkflowID].Finalizer; !finalizer.Enabled || len(finalizer.RequiredChecks) != 1 || finalizer.RequiredChecks[0] != "verify" {
		t.Fatalf("ordinary finalizer was not persisted: %+v", finalizer)
	}
	before, err := store.Events(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := runWithPrompt(t, []string{"start", "--state", state, "--root", root, "--runtime", "codex", "--file", "-", "--sandbox", "read-only", "--authorized-by", "human:test", "--idempotency-key", "cli-finalizer-incomplete", "--finalizer-enabled", "--require-human"}, "missing gate", &out, &errOut); err == nil {
		t.Fatal("enabled finalizer without required checks was accepted")
	}
	after, _ := store.Events(0)
	if len(after) != len(before) {
		t.Fatal("rejected incomplete finalizer mutated the journal")
	}
}

func TestStartRejectsEvaluatorConfigurationWithoutAutonomy(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	err := runWithPrompt(t, []string{"start", "--state", state, "--root", root, "--goal", "keep shipping", "--runtime", "codex", "--file", "-", "--sandbox", "read-only", "--authorized-by", "human:test", "--idempotency-key", "partial-autonomy", "--evaluator-model", "fake/evaluator"}, "ship", &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), "require autonomous execution") {
		t.Fatalf("partial autonomy configuration was not rejected: %v", err)
	}
	store, openErr := supervisor.Open(state, supervisor.Options{})
	if openErr != nil {
		t.Fatal(openErr)
	}
	events, eventsErr := store.Events(0)
	if eventsErr != nil {
		t.Fatal(eventsErr)
	}
	if len(events) != 0 {
		t.Fatal("rejected partial autonomy configuration mutated the journal")
	}
}

func TestStatusListReplyAndPauseUseSupervisorProjection(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := runWithPrompt(t, []string{"start", "--state", state, "--root", root, "--runtime", "codex", "--session", "exact-thread", "--file", "-", "--sandbox", "read-only", "--authorized-by", "human:test", "--idempotency-key", "cli-v2-start-01", "--json"}, "work", &out, &errOut); err != nil {
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
	store, err := supervisor.Open(state, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	activity := projection.Activities[response.Execution.FirstActivity]
	if activity == nil {
		t.Fatal("missing first activity")
	}
	attempt, _, err := store.PrepareAttempt(context.Background(), supervisor.PrepareAttemptInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, Runtime: supervisor.RuntimeSpec{Name: "codex", Sandbox: supervisor.SandboxReadOnly}, CommandDigest: "cli-reply-command", Outputs: supervisor.OutputIdentity{Stdout: "cli-reply-stdout", Stderr: "cli-reply-stderr"}, IdempotencyKey: "cli-reply-attempt"})
	if err != nil {
		t.Fatal(err)
	}
	for _, milestone := range []supervisor.Milestone{
		{Kind: supervisor.MilestoneProcessSpawned, Process: &supervisor.ProcessIdentity{PID: 55, StartToken: "cli-reply-process"}},
		{Kind: supervisor.MilestoneTurnStarted},
		{Kind: supervisor.MilestoneResult, Result: &supervisor.WorkerResult{Status: "completed", Summary: "cli reply predecessor"}},
		{Kind: supervisor.MilestoneExit, Exit: &supervisor.Exit{Code: 0}},
	} {
		if _, err := store.RecordMilestone(context.Background(), supervisor.RecordMilestoneInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, Milestone: milestone, IdempotencyKey: "cli-reply-" + string(milestone.Kind)}); err != nil {
			t.Fatal(err)
		}
	}
	out.Reset()
	if err := runWithPrompt(t, []string{"reply", "--state", state, "--execution", string(response.Execution.ID), "--activity", string(activity.ID), "--file", "-", "--json"}, "continue privately", &out, &errOut); err != nil {
		t.Fatalf("stdin-only reply: %v", err)
	}
	if strings.Contains(out.String(), "continue privately") {
		t.Fatalf("reply body leaked in response: %s", out.String())
	}
	if err := run([]string{"reply", "--state", state, "--execution", string(response.Execution.ID), "--activity", string(activity.ID), "--message", "argv-secret"}, &out, &errOut); err == nil || strings.Contains(out.String(), "argv-secret") {
		t.Fatal("legacy reply --message was accepted or exposed")
	}
	promptPath := filepath.Join(t.TempDir(), "prompt.txt")
	if err := os.WriteFile(promptPath, []byte("path-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"start", "--state", state, "--root", root, "--runtime", "codex", "--file", promptPath, "--authorized-by", "human:test", "--idempotency-key", "path-prompt-rejected"}, &out, &errOut); err == nil || strings.Contains(out.String(), "path-secret") {
		t.Fatal("ordinary start accepted or exposed an arbitrary prompt path")
	}
	out.Reset()
	if err := run([]string{"execution", "pause", "--state", state, "--workflow", string(response.Execution.WorkflowID), "--json"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"workflow_id"`) || !strings.Contains(out.String(), `"existing":false`) {
		t.Fatalf("pause=%s", out.String())
	}
	if err := run([]string{"start", "--state", state, "--runtime", "codex", "--prompt", "secret-argv-prompt", "--authorized-by", "human:test", "--idempotency-key", "prompt-argv-rejected"}, &out, &errOut); err == nil || strings.Contains(out.String(), "secret-argv-prompt") {
		t.Fatal("ordinary start accepted or exposed a prompt body in argv")
	}
	out.Reset()
	if err := run([]string{"activity", "list", "--state", state, "--json"}, &out, &errOut); err != nil || strings.Contains(out.String(), `"prompt"`) {
		t.Fatalf("activity projection leaked prompt or failed: output=%s err=%v", out.String(), err)
	}
	var activities []supervisor.ActivityView
	if err := json.Unmarshal(out.Bytes(), &activities); err != nil || len(activities) == 0 || activities[0].Status == "" {
		t.Fatalf("cloud activity shape missing v2 status: activities=%+v err=%v", activities, err)
	}
	for _, legacyField := range []string{`"version"`, `"state"`, `"revision"`, `"work"`, `"work_digest"`, `"attempts"`, `"controls"`, `"created_at"`, `"updated_at"`, `"owner_session_id"`, `"attempt_ids"`} {
		if strings.Contains(out.String(), legacyField) {
			t.Fatalf("legacy Activity compatibility field leaked into v2 projection: field=%s output=%s", legacyField, out.String())
		}
	}
}

func TestReplyWorkflowNodeCompatibilityRoute(t *testing.T) {
	stateRoot, worktree := t.TempDir(), t.TempDir()
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := runWithPrompt(t, []string{"start", "--state", stateRoot, "--root", worktree, "--runtime", "codex", "--session", "exact-reply-session", "--file", "-", "--sandbox", "read-only", "--authorized-by", "human:test", "--idempotency-key", "reply-workflow-node-start", "--json"}, "work", &out, &errOut); err != nil {
		t.Fatalf("start: %v stderr=%s", err, errOut.String())
	}
	var response struct {
		Execution supervisor.Execution `json:"execution"`
	}
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
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
	activity := state.Activities[response.Execution.FirstActivity]
	if activity == nil {
		t.Fatal("missing predecessor activity")
	}
	attempt, _, err := store.PrepareAttempt(context.Background(), supervisor.PrepareAttemptInput{
		ActivityID: activity.ID, ExpectedGeneration: activity.Generation, Runtime: supervisor.RuntimeSpec{Name: "codex", Sandbox: supervisor.SandboxReadOnly}, CommandDigest: "reply-workflow-node-command", Outputs: supervisor.OutputIdentity{Stdout: "reply-workflow-node-stdout", Stderr: "reply-workflow-node-stderr"}, IdempotencyKey: "reply-workflow-node-attempt",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, milestone := range []supervisor.Milestone{
		{Kind: supervisor.MilestoneProcessSpawned, Process: &supervisor.ProcessIdentity{PID: 55, StartToken: "reply-workflow-node-process"}},
		{Kind: supervisor.MilestoneTurnStarted},
		{Kind: supervisor.MilestoneResult, Result: &supervisor.WorkerResult{Status: "completed", Summary: "workflow node predecessor"}},
		{Kind: supervisor.MilestoneExit, Exit: &supervisor.Exit{Code: 0}},
	} {
		if _, err := store.RecordMilestone(context.Background(), supervisor.RecordMilestoneInput{ActivityID: activity.ID, ExpectedGeneration: activity.Generation, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, Milestone: milestone, IdempotencyKey: "reply-workflow-node-" + string(milestone.Kind)}); err != nil {
			t.Fatal(err)
		}
	}

	out.Reset()
	if err := runWithPrompt(t, []string{"reply", "--state", stateRoot, "--workflow", string(response.Execution.WorkflowID), "--activity", string(response.Execution.RootNodeID), "--file", "-", "--json"}, "workflow node continuation", &out, &errOut); err != nil {
		t.Fatalf("workflow/node reply route: %v stderr=%s", err, errOut.String())
	}
	var continuation struct {
		Activity struct {
			ID supervisor.ActivityID `json:"id"`
		} `json:"activity"`
	}
	if err := json.Unmarshal(out.Bytes(), &continuation); err != nil || continuation.Activity.ID == "" || strings.Contains(out.String(), "workflow node continuation") {
		t.Fatalf("workflow/node reply response=%s err=%v", out.String(), err)
	}
}

func TestPreferenceLadderIsJournaledAndOrdinaryStartUsesChildFallbacks(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := run([]string{"preference", "set", "planner", "--state", state, "--candidate", "codex:gpt-5:xhigh", "--candidate", "claude:sonnet:high"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := run([]string{"preference", "list", "--state", state}, &out, &errOut); err != nil || !strings.Contains(out.String(), "planner") {
		t.Fatalf("preference list=%s err=%v", out.String(), err)
	}
	out.Reset()
	if err := runWithPrompt(t, []string{"start", "--state", state, "--root", root, "--role", "planner", "--file", "-", "--authorized-by", "human:test", "--idempotency-key", "role-start-01", "--json"}, "role prompt", &out, &errOut); err != nil {
		t.Fatal(err)
	}
	var response struct {
		Execution supervisor.Execution `json:"execution"`
	}
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	store, err := supervisor.Open(state, supervisor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := store.Projection()
	if err != nil {
		t.Fatal(err)
	}
	work := projection.Workflows[response.Execution.WorkflowID].Nodes[response.Execution.RootNodeID].Work
	if len(work.Fallbacks) != 1 || work.Runtime.Name != "codex" || work.Fallbacks[0].Name != "claude" {
		t.Fatalf("role ladder was not preserved in one journal workflow: %+v", work)
	}
}

func TestEnvironmentJSONRequiresPrivateFileAndReturnsSortedTransientValues(t *testing.T) {
	path := t.TempDir() + "/environment.json"
	if err := os.WriteFile(path, []byte(`{"ZED":"last","ALPHA":"first"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	values, err := readEnvironmentJSON(path)
	if err != nil || strings.Join(values, ",") != "ALPHA=first,ZED=last" {
		t.Fatalf("environment=%v err=%v", values, err)
	}
	if runtime.GOOS != "windows" {
		if err = os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err = readEnvironmentJSON(path); err == nil {
			t.Fatal("world-readable environment file was accepted")
		}
	}
}

func TestPauseCLIWaitsForDurableExitWithoutMutatingWhilePolling(t *testing.T) {
	stateRoot, worktree, output := t.TempDir(), t.TempDir(), new(bytes.Buffer)
	if err := os.Chmod(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	var startOut, errOut bytes.Buffer
	if err := runWithPrompt(t, []string{"start", "--state", stateRoot, "--root", worktree, "--runtime", "claude", "--file", "-", "--sandbox", "workspace-write", "--authorized-by", "human:test", "--idempotency-key", "pause-cli-start", "--json"}, "work", &startOut, &errOut); err != nil {
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

func runWithPrompt(t *testing.T, args []string, prompt string, out, errOut *bytes.Buffer) error {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		return err
	}
	if _, err = write.WriteString(prompt); err != nil {
		_ = read.Close()
		_ = write.Close()
		return err
	}
	_ = write.Close()
	previous := os.Stdin
	os.Stdin = read
	defer func() {
		os.Stdin = previous
		_ = read.Close()
	}()
	return run(args, out, errOut)
}
