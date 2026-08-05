package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/finalize"
	"github.com/carlchungus/durable-agent-handoff/internal/preferences"
	hruntime "github.com/carlchungus/durable-agent-handoff/internal/runtime"
)

type Result struct {
	Status       string             `json:"status"`
	Summary      string             `json:"summary"`
	SessionID    string             `json:"session_id,omitempty"`
	Mutations    []core.Mutation    `json:"mutations,omitempty"`
	Attestations []core.Attestation `json:"attestations,omitempty"`
}

type Engine struct {
	Store       *core.Store
	Preferences *preferences.Manager
}

func (e *Engine) RunOne(ctx context.Context, id string) (*core.Node, error) {
	w, err := e.Store.Load(id)
	if err != nil {
		return nil, err
	}
	if w.Paused {
		return nil, errors.New("workflow is paused")
	}
	var node *core.Node
	for _, nid := range w.Order {
		n := w.Nodes[nid]
		if n.State == core.NodeReady && n.Kind != "human" && n.Kind != "merge" {
			node = n
			break
		}
	}
	if node == nil {
		return nil, errors.New("no runnable node")
	}
	if e.Preferences != nil {
		routed, index, routeErr := e.Preferences.Resolve(node.Role, node.Runtime)
		if routeErr != nil {
			return nil, routeErr
		}
		if !reflect.DeepEqual(routed, node.Runtime) || index != node.CandidateIndex {
			_, err = e.Store.Apply(core.Proposal{WorkflowID: id, Actor: "supervisor", Mutations: []core.Mutation{
				{Op: "set_runtime", NodeID: node.ID, Runtime: &routed, CandidateIndex: index},
				{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("route-%s-%d", node.ID, node.Attempt+1), NodeID: node.ID, Kind: "routing", Summary: fmt.Sprintf("selected preference %d: %s", index+1, preferences.Key(routed))}},
			}})
			if err != nil {
				return nil, err
			}
			node.Runtime, node.CandidateIndex = routed, index
		}
	}
	if _, err = e.Store.Apply(core.Proposal{WorkflowID: id, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_state", NodeID: node.ID, State: core.NodeRunning}}}); err != nil {
		return nil, err
	}
	if node.Kind == "command" {
		return node, e.runCommand(ctx, w, node)
	}
	if node.Kind == "finalize" {
		return node, e.runFinalize(ctx, w, node)
	}
	return node, e.runAgent(ctx, w, node)
}

func (e *Engine) runFinalize(ctx context.Context, w *core.Workflow, n *core.Node) error {
	ctx, cancel := context.WithTimeout(ctx, w.Budget.MaxRuntime)
	defer cancel()
	result, err := finalize.Execute(ctx, finalize.ExecRunner{Dir: workdir(w, n)}, w, n)
	if errors.Is(err, finalize.ErrGatesPending) {
		_, applyErr := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{
			{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("pr-wait-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "github", Summary: result.Summary, URI: result.PRURL}},
			{Op: "set_state", NodeID: n.ID, State: core.NodeReady},
		}})
		return applyErr
	}
	if err != nil {
		return e.fail(w, n, err.Error())
	}
	_, err = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{
		{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("merged-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "github", Summary: result.Summary, URI: result.PRURL, Digest: result.HeadSHA}},
		{Op: "set_state", NodeID: n.ID, State: core.NodeCompleted},
	}})
	return err
}

func (e *Engine) runCommand(ctx context.Context, w *core.Workflow, n *core.Node) error {
	ctx, cancel := context.WithTimeout(ctx, w.Budget.MaxRuntime)
	defer cancel()
	if n.Runtime.Executable == "" {
		return e.fail(w, n, "command executable is empty")
	}
	cmd := exec.CommandContext(ctx, n.Runtime.Executable, n.Runtime.Args...)
	cmd.Dir = workdir(w, n)
	out, err := cmd.CombinedOutput()
	ev := core.Evidence{ID: "evidence-" + n.ID + fmt.Sprint(n.Attempt+1), NodeID: n.ID, Kind: "command", Summary: truncate(string(out), 4000)}
	state := core.NodeCompleted
	if err != nil {
		state = core.NodeFailed
		ev.Summary = fmt.Sprintf("%v\n%s", err, ev.Summary)
	}
	_, applyErr := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "add_evidence", Evidence: &ev}, {Op: "set_state", NodeID: n.ID, State: state}}})
	return applyErr
}

func (e *Engine) runAgent(ctx context.Context, w *core.Workflow, n *core.Node) error {
	ctx, cancel := context.WithTimeout(ctx, w.Budget.MaxRuntime)
	defer cancel()
	dir := filepath.Join(e.Store.Dir(), "workflows", w.ID, "runs", n.ID, fmt.Sprintf("%d", n.Attempt+1))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	prompt := contract(w, n)
	schema := filepath.Join(dir, "result.schema.json")
	output := filepath.Join(dir, "last-message.json")
	_ = os.WriteFile(schema, []byte(resultSchema), 0o600)
	c, err := hruntime.Build(n.Runtime, workdir(w, n), prompt, n.SessionID, schema, output)
	if err != nil {
		return e.fail(w, n, err.Error())
	}
	cmd := exec.CommandContext(ctx, c.Name, c.Args...)
	cmd.Dir = workdir(w, n)
	if c.PromptOnStdin {
		cmd.Stdin = strings.NewReader(prompt)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	_ = os.WriteFile(filepath.Join(dir, "events.jsonl"), stdout.Bytes(), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "stderr.log"), stderr.Bytes(), 0o600)
	if err != nil {
		failure := fmt.Sprintf("runtime failed: %v: %s %s", err, truncate(stderr.String(), 1000), truncate(stdout.String(), 1000))
		if e.routeAfterLimit(w, n, failure) {
			return nil
		}
		return e.fail(w, n, failure)
	}
	b := stdout.Bytes()
	if data, readErr := os.ReadFile(output); readErr == nil && len(bytes.TrimSpace(data)) > 0 {
		b = data
	}
	result, err := parseResult(b)
	if err != nil {
		failure := err.Error() + ": " + truncate(stderr.String(), 1000) + " " + truncate(stdout.String(), 1000)
		if e.routeAfterLimit(w, n, failure) {
			return nil
		}
		return e.fail(w, n, failure)
	}
	mut := append([]core.Mutation{}, result.Mutations...)
	sessionID := result.SessionID
	if sessionID == "" {
		sessionID = extractSessionID(stdout.Bytes())
	}
	if sessionID != "" && sessionID != n.SessionID {
		mut = append(mut, core.Mutation{Op: "set_session", NodeID: n.ID, Reason: sessionID})
	}
	for i := range result.Attestations {
		a := result.Attestations[i]
		if a.NodeID == "" {
			a.NodeID = n.ID
		}
		if a.ID == "" {
			a.ID = fmt.Sprintf("attest-%s-%d", n.ID, i+1)
		}
		mut = append(mut, core.Mutation{Op: "attest", Attestation: &a})
	}
	state := core.NodeCompleted
	if result.Status == "continue" {
		state = core.NodeReady
	}
	if result.Status == "needs_human" {
		state = core.NodeWaiting
	}
	if result.Status == "blocked" {
		state = core.NodeFailed
	}
	mut = append(mut, core.Mutation{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("result-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "agent", Summary: result.Summary}}, core.Mutation{Op: "set_state", NodeID: n.ID, State: state})
	_, err = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: n.ID, Mutations: mut, Rationale: result.Summary})
	if err != nil {
		return e.fail(w, n, "runtime proposed an invalid workflow mutation: "+err.Error())
	}
	return nil
}

func (e *Engine) routeAfterLimit(w *core.Workflow, n *core.Node, failure string) bool {
	if e.Preferences == nil || n.Role == "" {
		return false
	}
	class := preferences.ClassifyFailure(failure)
	if class == "runtime_error" {
		return false
	}
	if e.Preferences.Record(n.Runtime, class, failure) != nil {
		return false
	}
	next, index, routeErr := e.Preferences.Resolve(n.Role, n.Runtime)
	mutations := []core.Mutation{
		{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("limit-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "provider_limit", Summary: fmt.Sprintf("%s hit %s; %s", preferences.Key(n.Runtime), class, truncate(failure, 500))}},
		{Op: "set_state", NodeID: n.ID, State: core.NodeReady},
	}
	if routeErr == nil {
		mutations = append([]core.Mutation{{Op: "set_runtime", NodeID: n.ID, Runtime: &next, CandidateIndex: index}}, mutations...)
	}
	_, err := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: mutations, Rationale: "observable preference-ladder fallback"})
	return err == nil
}

func (e *Engine) fail(w *core.Workflow, n *core.Node, reason string) error {
	state := core.NodeFailed
	if n.Attempt+1 < n.MaxAttempts {
		state = core.NodeReady
	}
	_, err := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("error-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "runtime_error", Summary: reason}}, {Op: "set_state", NodeID: n.ID, State: state}}})
	return err
}

func workdir(w *core.Workflow, n *core.Node) string {
	if n.Worktree != "" {
		return n.Worktree
	}
	return w.Root
}
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
func parseResult(b []byte) (Result, error) {
	var r Result
	if json.Unmarshal(bytes.TrimSpace(b), &r) == nil && r.Status != "" {
		return r, nil
	}
	lines := bytes.Split(b, []byte("\n"))
	for i := len(lines) - 1; i >= 0; i-- {
		var direct Result
		if json.Unmarshal(lines[i], &direct) == nil && direct.Status != "" {
			return direct, nil
		}
		var v any
		if json.Unmarshal(lines[i], &v) != nil {
			continue
		}
		found := findResult(v)
		if found != nil {
			return *found, nil
		}
	}
	return r, errors.New("runtime did not emit a valid result object")
}
func findResult(v any) *Result {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if x, ok := m["result"]; ok {
		if s, ok := x.(string); ok {
			var r Result
			if json.Unmarshal([]byte(s), &r) == nil && r.Status != "" {
				return &r
			}
		}
	}
	for _, x := range m {
		if r := findResult(x); r != nil {
			return r
		}
	}
	return nil
}
func extractSessionID(b []byte) string {
	lines := bytes.Split(b, []byte("\n"))
	for _, line := range lines {
		var v any
		if json.Unmarshal(line, &v) == nil {
			if id := findSessionID(v); id != "" {
				return id
			}
		}
	}
	return ""
}
func findSessionID(v any) string {
	switch x := v.(type) {
	case map[string]any:
		for _, key := range []string{"thread_id", "threadId", "session_id", "sessionId", "session_file", "sessionFile", "session_path", "sessionPath"} {
			if id, ok := x[key].(string); ok && len(id) >= 8 {
				return id
			}
		}
		for _, child := range x {
			if id := findSessionID(child); id != "" {
				return id
			}
		}
	case []any:
		for _, child := range x {
			if id := findSessionID(child); id != "" {
				return id
			}
		}
	}
	return ""
}
func contract(w *core.Workflow, n *core.Node) string {
	return fmt.Sprintf(`Goal: %s

Current dynamic task: %s
Task id: %s

%s

You own how to accomplish and verify this task. Inspect live state, adapt the plan when evidence changes, and propose new nodes or independent verifier work when useful. Do not push, merge, access production, or expand authority. End with one JSON result matching the supplied schema.`, w.Goal, n.Title, n.ID, n.Prompt)
}

const resultSchema = `{"type":"object","required":["status","summary"],"properties":{"status":{"enum":["completed","continue","blocked","needs_human"]},"summary":{"type":"string"},"session_id":{"type":"string"},"mutations":{"type":"array"},"attestations":{"type":"array"}},"additionalProperties":false}`
