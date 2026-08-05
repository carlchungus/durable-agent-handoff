package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

func TestQuickJSWorkflowCapabilityAndTopLevelAwait(t *testing.T) {
	w := testWorkflow(t)
	w.Evidence = append(w.Evidence, core.Evidence{ID: "ev1", NodeID: "lead", Kind: "test", Summary: "ready"})
	input := vmInput(t, w, `
await Promise.resolve();
if (handoff.workflow.id !== "wf_test") throw new Error("wrong workflow");
if (handoff.node("lead").title !== "Lead") throw new Error("wrong node");
if (handoff.evidence("lead").length !== 1) throw new Error("wrong evidence");
if (handoff.args.mode !== "review") throw new Error("wrong args");
handoff.propose([{
  op: "add_node",
  node: {id: "review", title: "Review", kind: "agent", depends_on: ["lead"], runtime: {sandbox: "read-only"}}
}], "state-driven review");`)

	got, err := (QuickJSRuntime{}).Evaluate(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rationale != "state-driven review" || len(got.Mutations) != 1 || got.Mutations[0].Node.ID != "review" {
		t.Fatalf("unexpected output: %+v", got)
	}
	if got.FuelUsed == 0 {
		t.Fatal("expected deterministic execution fuel accounting")
	}
}

func TestQuickJSEvaluationIsDeterministic(t *testing.T) {
	w := testWorkflow(t)
	input := vmInput(t, w, `
let total = 0;
for (const value of handoff.args.values) total += value;
handoff.propose([{op: "set_state", node_id: "lead", state: "waiting"}], String(total));`)
	input.ArgsJSON = json.RawMessage(`{"values":[3,1,4,1,5]}`)
	first, err := (QuickJSRuntime{}).Evaluate(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := (QuickJSRuntime{}).Evaluate(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, _ := json.Marshal(first.Mutations)
	secondJSON, _ := json.Marshal(second.Mutations)
	if first.Rationale != second.Rationale || string(firstJSON) != string(secondJSON) || first.FuelUsed != second.FuelUsed {
		t.Fatalf("nondeterministic evaluation:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func TestQuickJSSandboxHasNoAmbientAuthority(t *testing.T) {
	w := testWorkflow(t)
	input := vmInput(t, w, `
	const denied = [typeof process, typeof require, typeof fetch, typeof Deno, typeof Date, typeof performance, typeof std, typeof os, typeof crypto, typeof WebSocket];
let randomDenied = false;
try { Math.random(); } catch (_) { randomDenied = true; }
let constructorDenied = false;
try { (() => {}).constructor("return 1")(); } catch (_) { constructorDenied = true; }
if (denied.some(v => v !== "undefined") || !randomDenied || !constructorDenied) {
  throw new Error("ambient authority or dynamic code generation leaked");
}
handoff.propose([{op: "set_state", node_id: "lead", state: "waiting"}], "sandboxed");`)
	if _, err := (QuickJSRuntime{}).Evaluate(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

func TestQuickJSRejectsModuleAccessBeforeEvaluation(t *testing.T) {
	w := testWorkflow(t)
	input := vmInput(t, w, `const moduleName = "qjs:os"; await import(moduleName);`)
	_, err := (QuickJSRuntime{}).Evaluate(context.Background(), input)
	var vmErr *VMError
	if !errors.As(err, &vmErr) || vmErr.Kind() != "capability" || !strings.Contains(err.Error(), "import/export") {
		t.Fatalf("expected actionable capability error, got %v", err)
	}
}

func TestQuickJSInstructionLimitStopsTightLoop(t *testing.T) {
	w := testWorkflow(t)
	input := vmInput(t, w, `let n = 0; while (true) { n++; }`)
	input.Limits.InstructionFuel = 2_000
	input.Limits.Timeout = 3 * time.Second
	started := time.Now()
	_, err := (QuickJSRuntime{}).Evaluate(context.Background(), input)
	var vmErr *VMError
	if !errors.As(err, &vmErr) || vmErr.Kind() != "instruction_limit" {
		t.Fatalf("expected instruction limit, got %v", err)
	}
	if elapsed := time.Since(started); elapsed >= input.Limits.Timeout {
		t.Fatalf("instruction limit did not stop before timeout: %s", elapsed)
	}
}

func TestQuickJSTimeoutStopsLongEvaluation(t *testing.T) {
	w := testWorkflow(t)
	input := vmInput(t, w, `let n = 0; while (true) { n++; }`)
	input.Limits.InstructionFuel = ^uint64(0)
	input.Limits.Timeout = 25 * time.Millisecond
	_, err := (QuickJSRuntime{}).Evaluate(context.Background(), input)
	var vmErr *VMError
	if !errors.As(err, &vmErr) || vmErr.Kind() != "timeout" {
		t.Fatalf("expected timeout, got %v", err)
	}
}

func TestQuickJSOutputLimitRejectsOversizedProposal(t *testing.T) {
	w := testWorkflow(t)
	input := vmInput(t, w, `handoff.propose([{op: "set_state", node_id: "lead", state: "waiting"}], "x".repeat(10000));`)
	input.Limits.MaxOutputBytes = 256
	_, err := (QuickJSRuntime{}).Evaluate(context.Background(), input)
	var vmErr *VMError
	if !errors.As(err, &vmErr) || vmErr.Kind() != "output_limit" {
		t.Fatalf("expected output limit, got %v", err)
	}
}

func TestQuickJSMemoryLimitIsEnforced(t *testing.T) {
	w := testWorkflow(t)
	input := vmInput(t, w, `"x".repeat(100000000);`)
	input.Limits.MemoryBytes = 16 << 20
	input.Limits.InstructionFuel = ^uint64(0)
	input.Limits.Timeout = 3 * time.Second
	_, err := (QuickJSRuntime{}).Evaluate(context.Background(), input)
	var vmErr *VMError
	if !errors.As(err, &vmErr) || vmErr.Kind() != "memory_limit" {
		t.Fatalf("expected classified memory limit failure, got %v", err)
	}
}

func testWorkflow(t *testing.T) *core.Workflow {
	t.Helper()
	now := time.Unix(100, 0).UTC()
	root := t.TempDir()
	return &core.Workflow{
		ID: "wf_test", Goal: "test", Root: root, Status: core.WorkflowActive,
		Budget: core.DefaultBudget(), CreatedAt: now, UpdatedAt: now,
		Nodes: map[string]*core.Node{
			"lead": {ID: "lead", Title: "Lead", Kind: "agent", State: core.NodeReady, CreatedAt: now, UpdatedAt: now},
		},
		Order: []string{"lead"},
	}
}

func vmInput(t *testing.T, w *core.Workflow, source string) VMInput {
	t.Helper()
	snapshot, err := json.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	return VMInput{
		Filename: "workflow.js", Source: source, WorkflowJSON: snapshot,
		ArgsJSON: json.RawMessage(`{"mode":"review"}`), MaxMutations: 8, Limits: DefaultVMLimits(),
	}
}

func scriptFromSource(source string) Script {
	sum := sha256.Sum256([]byte(source))
	return Script{Filename: "workflow.js", Source: source, Hash: hex.EncodeToString(sum[:])}
}

func TestLoadScriptRejectsSymlinkEscapeAndOversize(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.js")
	if err := os.WriteFile(outside, []byte("handoff.propose([])"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "escaped.js")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := LoadScript(link, []string{root}, 1024); err == nil || !strings.Contains(err.Error(), "outside allowed roots") {
		t.Fatalf("expected symlink boundary error, got %v", err)
	}
	inside := filepath.Join(root, "large.js")
	if err := os.WriteFile(inside, []byte(strings.Repeat("x", 33)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScript(inside, []string{root}, 32); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected source size error, got %v", err)
	}
}
