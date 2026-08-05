package workflow

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

type recordingScriptRuntime struct {
	calls    int
	identity string
	output   VMOutput
	err      error
}

func (r *recordingScriptRuntime) Identity() string {
	if r.identity == "" {
		return "recording-v1"
	}
	return r.identity
}

func (r *recordingScriptRuntime) Evaluate(context.Context, VMInput) (VMOutput, error) {
	r.calls++
	return r.output, r.err
}

func TestScriptEvaluatorValidatesAndReplaysProposal(t *testing.T) {
	w := testWorkflow(t)
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := &recordingScriptRuntime{output: VMOutput{
		Mutations: coreMutations{{Op: "set_state", NodeID: "lead", State: "waiting"}}.toCore(),
		Rationale: "wait for input",
		FuelUsed:  42,
	}}
	evaluator := ScriptEvaluator{Runtime: runtime}
	req := ScriptRequest{
		RunID: "run_1", Actor: "lead", Workflow: w,
		Script: scriptFromSource(`handoff.propose([])`), Args: json.RawMessage(`{"b":2,"a":1}`),
		Limits: DefaultVMLimits(), Journal: journal,
	}
	first, err := evaluator.Evaluate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := evaluator.Evaluate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.calls != 1 || first.Replayed || !second.Replayed || first.Fingerprint != second.Fingerprint {
		t.Fatalf("unexpected replay: calls=%d first=%+v second=%+v", runtime.calls, first, second)
	}
	if first.FuelUsed != second.FuelUsed {
		t.Fatalf("replay lost fuel accounting: first=%d second=%d", first.FuelUsed, second.FuelUsed)
	}
	events, err := Load(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != "script.started" || events[1].Type != "script.proposed" {
		t.Fatalf("unexpected journal: %+v", events)
	}
}

func TestScriptEvaluatorRerunsAfterCrashAndTruncatedTail(t *testing.T) {
	w := testWorkflow(t)
	path := filepath.Join(t.TempDir(), "events.jsonl")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &recordingScriptRuntime{output: VMOutput{
		Mutations: coreMutations{{Op: "set_state", NodeID: "lead", State: "waiting"}}.toCore(),
	}}
	evaluator := ScriptEvaluator{Runtime: runtime}
	req := ScriptRequest{RunID: "run_crash", Actor: "lead", Workflow: w, Script: scriptFromSource(`handoff.propose([])`), Limits: DefaultVMLimits(), Journal: journal}
	args, _ := canonicalJSON(nil, json.RawMessage(`null`))
	snapshot, _ := json.Marshal(w)
	fingerprint, _ := scriptFingerprint(req, snapshot, args, runtime.Identity())
	if err := journal.Append(Event{Type: "script.started", Script: &ScriptCall{RunID: req.RunID, Fingerprint: fingerprint, SourceHash: req.Script.Hash, Filename: req.Script.Filename}}); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"sequence":2,"type":"script.proposed"`)
	_ = f.Close()
	journal, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	req.Journal = journal
	result, err := evaluator.Evaluate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.calls != 1 || result.Replayed {
		t.Fatalf("unfinished run must execute live: calls=%d result=%+v", runtime.calls, result)
	}
	events, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[2].Type != "script.proposed" {
		t.Fatalf("recovered result was not durable after truncated tail: %+v", events)
	}
}

func TestScriptEvaluatorRejectsProposalAtomicallyAndJournalsFailure(t *testing.T) {
	w := testWorkflow(t)
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := &recordingScriptRuntime{output: VMOutput{Mutations: coreMutations{
		{Op: "set_state", NodeID: "lead", State: "waiting"},
		{Op: "add_dependency", NodeID: "missing", DependsOn: []string{"lead"}},
	}.toCore()}}
	_, err = (ScriptEvaluator{Runtime: runtime}).Evaluate(context.Background(), ScriptRequest{
		RunID: "run_bad", Actor: "lead", Workflow: w, Script: scriptFromSource(`handoff.propose([])`), Limits: DefaultVMLimits(), Journal: journal,
	})
	if err == nil || !strings.Contains(err.Error(), "rejected atomically") {
		t.Fatalf("expected atomic policy rejection, got %v", err)
	}
	if w.Nodes["lead"].State != "ready" {
		t.Fatalf("live workflow mutated on rejected proposal: %+v", w.Nodes["lead"])
	}
	events, err := Load(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Type != "script.failed" || events[1].ScriptFailure.Kind != "policy" {
		t.Fatalf("unexpected rejection journal: %+v", events)
	}
}

func TestScriptEvaluatorRejectsMismatchedCachedIdentity(t *testing.T) {
	w := testWorkflow(t)
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := &recordingScriptRuntime{}
	req := ScriptRequest{RunID: "run_tampered", Actor: "lead", Workflow: w, Script: scriptFromSource(`handoff.propose([])`), Limits: DefaultVMLimits(), Journal: journal}
	args, _ := canonicalJSON(nil, json.RawMessage(`null`))
	snapshot, _ := json.Marshal(w)
	fingerprint, _ := scriptFingerprint(req, snapshot, args, runtime.Identity())
	if err = journal.Append(Event{Type: "script.started", Script: &ScriptCall{RunID: req.RunID, Fingerprint: fingerprint}}); err != nil {
		t.Fatal(err)
	}
	if err = journal.Append(Event{Type: "script.proposed", ScriptResult: &ScriptResult{
		RunID: req.RunID, Fingerprint: fingerprint,
		Proposal: core.Proposal{WorkflowID: w.ID, Actor: "human", Mutations: coreMutations{{Op: "pause"}}.toCore()},
	}}); err != nil {
		t.Fatal(err)
	}
	_, err = (ScriptEvaluator{Runtime: runtime}).Evaluate(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("expected cached identity rejection, got %v", err)
	}
	if runtime.calls != 0 {
		t.Fatalf("tampered cached result unexpectedly executed runtime %d times", runtime.calls)
	}
}

func TestScriptEvaluatorEngineIdentityInvalidatesReplay(t *testing.T) {
	w := testWorkflow(t)
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	output := VMOutput{Mutations: coreMutations{{Op: "set_state", NodeID: "lead", State: "waiting"}}.toCore()}
	firstRuntime := &recordingScriptRuntime{identity: "engine-v1", output: output}
	req := ScriptRequest{RunID: "run_engine", Actor: "lead", Workflow: w, Script: scriptFromSource(`handoff.propose([])`), Limits: DefaultVMLimits(), Journal: journal}
	first, err := (ScriptEvaluator{Runtime: firstRuntime}).Evaluate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	secondRuntime := &recordingScriptRuntime{identity: "engine-v2", output: output}
	second, err := (ScriptEvaluator{Runtime: secondRuntime}).Evaluate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Replayed || second.Replayed || first.Fingerprint == second.Fingerprint || secondRuntime.calls != 1 {
		t.Fatalf("engine upgrade incorrectly replayed cached output: first=%+v second=%+v calls=%d", first, second, secondRuntime.calls)
	}
}

// coreMutation keeps test fixtures compact while preserving the production
// JSON protocol names used by scripts.
type coreMutation struct {
	Op        string
	NodeID    string
	State     string
	DependsOn []string
}

type coreMutations []coreMutation

func (m coreMutations) toCore() []core.Mutation {
	out := make([]core.Mutation, len(m))
	for i, item := range m {
		out[i] = core.Mutation{Op: item.Op, NodeID: item.NodeID, State: core.NodeState(item.State), DependsOn: item.DependsOn}
	}
	return out
}
