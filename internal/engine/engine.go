package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
	"github.com/carlchungus/durable-agent-handoff/internal/finalize"
	"github.com/carlchungus/durable-agent-handoff/internal/preferences"
	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
	hruntime "github.com/carlchungus/durable-agent-handoff/internal/runtime"
)

type Result struct {
	Status       string             `json:"status"`
	Summary      string             `json:"summary"`
	SessionID    string             `json:"session_id,omitempty"`
	Mutations    []encodedMutation  `json:"mutations,omitempty"`
	Attestations []core.Attestation `json:"attestations,omitempty"`
}

// encodedMutation accepts ordinary mutation objects from runtimes without
// strict structured output and JSON-encoded object strings from Codex. Codex's
// strict response schemas cannot express the intentionally dynamic mutation
// union without making every nested operation shape required.
type encodedMutation struct{ core.Mutation }

func (m *encodedMutation) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var encoded string
		if err := json.Unmarshal(data, &encoded); err != nil {
			return err
		}
		data = []byte(encoded)
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}
	if _, ok := probe["op"]; ok {
		return json.Unmarshal(data, &m.Mutation)
	}
	// Agent models commonly describe the intuitive task mutation even when the
	// prompt names the lower-level core mutation. Accept that narrow shape and
	// translate it at the protocol boundary; unknown kinds still fail closed.
	var task struct {
		Kind      string `json:"kind"`
		NodeID    string `json:"node_id"`
		ParentID  string `json:"parent_id"`
		Priority  string `json:"priority"`
		Task      string `json:"task"`
		Authority string `json:"authority"`
	}
	if err := json.Unmarshal(data, &task); err != nil {
		return err
	}
	if task.Kind != "add_node" || task.NodeID == "" || task.Task == "" {
		return fmt.Errorf("unsupported encoded mutation kind %q", task.Kind)
	}
	m.Mutation = core.Mutation{Op: "add_node", Node: &core.Node{ID: task.NodeID, Title: task.Task, Kind: "agent", Prompt: task.Task, Metadata: map[string]string{"parent_id": task.ParentID, "priority": task.Priority, "authority": task.Authority}}}
	return nil
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

// Reconcile repairs nodes left in running state after a supervisor restart.
// It never guesses from a PID alone and never reruns ambiguous side effects.
func (e *Engine) Reconcile(_ context.Context, id string) error {
	w, err := e.Store.Load(id)
	if err != nil {
		return err
	}
	for _, nodeID := range w.Order {
		n := w.Nodes[nodeID]
		if n == nil || n.State != core.NodeRunning {
			continue
		}
		dir := filepath.Join(e.Store.Dir(), "workflows", w.ID, "runs", n.ID, fmt.Sprint(n.Attempt))
		manifest, manifestErr := runstate.Load(filepath.Join(dir, "attempt.json"))
		if manifestErr == nil && runstate.ProcessMatches(manifest) {
			continue
		}

		outputPath := filepath.Join(dir, "last-message.json")
		b, readErr := os.ReadFile(outputPath)
		if readErr != nil || len(bytes.TrimSpace(b)) == 0 {
			b, _ = os.ReadFile(filepath.Join(dir, "events.jsonl"))
		}
		if result, parseErr := parseResult(b); parseErr == nil {
			sessionID := result.SessionID
			if sessionID == "" && manifestErr == nil {
				sessionID = manifest.SessionID
			}
			return e.applyAgentResult(w, n, result, sessionID, n.Attempt)
		}

		sessionID := n.SessionID
		if sessionID == "" && manifestErr == nil {
			sessionID = manifest.SessionID
		}
		state := core.NodeWaiting
		summary := "supervisor lost the worker before a resumable session identity was persisted; human review is required before retrying"
		if sessionID != "" {
			state = core.NodeReady
			summary = "worker process ended after supervisor interruption; resuming the exact persisted runtime session"
		} else if manifestErr == nil && manifest.RestartSafe {
			state = core.NodeReady
			summary = "restart-safe worker ended after supervisor interruption; scheduling a fresh attempt"
		}
		mutations := []core.Mutation{{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("recovery-%s-%d", n.ID, n.Attempt), NodeID: n.ID, Kind: "recovery", Summary: summary}}, {Op: "set_state", NodeID: n.ID, State: state}}
		if sessionID != "" && sessionID != n.SessionID {
			mutations = append([]core.Mutation{{Op: "set_session", NodeID: n.ID, Reason: sessionID}}, mutations...)
		}
		_, err = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: mutations, Rationale: "reconcile interrupted runtime attempt"})
		return err
	}
	return nil
}

// RecoverAttempt reapplies a completed runtime result that an older or crashed
// supervisor failed to reduce. It is explicit and attempt-addressed so recovery
// never guesses which concurrent session produced the result.
func (e *Engine) RecoverAttempt(id, nodeID string, attempt int) error {
	w, err := e.Store.Load(id)
	if err != nil {
		return err
	}
	n := w.Nodes[nodeID]
	if n == nil {
		return fmt.Errorf("node %q does not exist", nodeID)
	}
	if attempt < 1 || attempt > n.Attempt {
		return fmt.Errorf("attempt %d is outside recorded range 1..%d", attempt, n.Attempt)
	}
	dir := filepath.Join(e.Store.Dir(), "workflows", w.ID, "runs", n.ID, fmt.Sprint(attempt))
	b, readErr := os.ReadFile(filepath.Join(dir, "last-message.json"))
	if readErr != nil || len(bytes.TrimSpace(b)) == 0 {
		b, readErr = os.ReadFile(filepath.Join(dir, "events.jsonl"))
	}
	if readErr != nil {
		return readErr
	}
	result, err := parseResult(b)
	if err != nil {
		return err
	}
	stream, _ := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	return e.applyAgentResult(w, n, result, extractSessionID(stream), attempt)
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
	stdoutPath := filepath.Join(dir, "events.jsonl")
	stderrPath := filepath.Join(dir, "stderr.log")
	recorder, err := runstate.Create(filepath.Join(dir, "attempt.json"), runstate.Manifest{
		ID:            fmt.Sprintf("%s/%d", n.ID, n.Attempt+1),
		WorkflowID:    w.ID,
		NodeID:        n.ID,
		Attempt:       n.Attempt + 1,
		Runtime:       n.Runtime.Name,
		Model:         n.Runtime.Model,
		Effort:        n.Runtime.Effort,
		SessionID:     n.SessionID,
		CommandDigest: runstate.CommandDigest(c.Name, c.Args),
		Worktree:      workdir(w, n),
		RestartSafe:   n.Metadata["restart_safe"] == "true",
	})
	if err != nil {
		return e.fail(w, n, err.Error())
	}
	stdout, err := os.OpenFile(stdoutPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return e.fail(w, n, err.Error())
	}
	defer stdout.Close()
	stderr, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return e.fail(w, n, err.Error())
	}
	defer stderr.Close()
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err = cmd.Start(); err != nil {
		_ = recorder.Update(func(m *runstate.Manifest) {
			m.Status, m.Error, m.FinishedAt = "failed", err.Error(), time.Now().UTC()
		})
		return e.fail(w, n, err.Error())
	}
	_ = recorder.Update(func(m *runstate.Manifest) {
		m.Status = "running"
		m.PID = cmd.Process.Pid
		m.ProcessStartToken = runstate.ProcessStartToken(cmd.Process.Pid)
	})
	heartbeatStop := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatStop:
				return
			case <-ticker.C:
				_ = recorder.Update(func(m *runstate.Manifest) {
					if info, statErr := os.Stat(stdoutPath); statErr == nil {
						m.EventOffset = info.Size()
					}
				})
			}
		}
	}()
	stopObserve := make(chan struct{})
	observed := observeRuntimeEvents(stdoutPath, stopObserve, func(sessionID string) {
		if sessionID == "" || sessionID == n.SessionID {
			return
		}
		_ = recorder.Update(func(m *runstate.Manifest) { m.SessionID = sessionID })
		_, _ = e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{{Op: "set_session", NodeID: n.ID, Reason: sessionID}}, Rationale: "persist runtime session identity before the process exits"})
	})
	err = cmd.Wait()
	close(heartbeatStop)
	<-heartbeatDone
	close(stopObserve)
	<-observed
	_ = stdout.Sync()
	_ = stderr.Sync()
	stdoutBytes, _ := os.ReadFile(stdoutPath)
	stderrBytes, _ := os.ReadFile(stderrPath)
	_ = recorder.Update(func(m *runstate.Manifest) {
		m.FinishedAt = time.Now().UTC()
		m.EventOffset = int64(len(stdoutBytes))
		code := 0
		if err != nil {
			m.Status, m.Error = "failed", err.Error()
			code = -1
			if cmd.ProcessState != nil {
				code = cmd.ProcessState.ExitCode()
			}
		} else {
			m.Status = "completed"
		}
		m.ExitCode = &code
	})
	if err != nil {
		failure := fmt.Sprintf("runtime failed: %v: %s %s", err, truncate(string(stderrBytes), 1000), truncate(string(stdoutBytes), 1000))
		if e.routeAfterLimit(w, n, failure, string(stderrBytes), string(stdoutBytes)) {
			return nil
		}
		return e.fail(w, n, failure)
	}
	b := stdoutBytes
	if data, readErr := os.ReadFile(output); readErr == nil && len(bytes.TrimSpace(data)) > 0 {
		b = data
	}
	result, err := parseResult(b)
	if err != nil {
		failure := err.Error() + ": " + truncate(string(stderrBytes), 1000) + " " + truncate(string(stdoutBytes), 1000)
		if e.routeAfterLimit(w, n, failure, string(stderrBytes), string(stdoutBytes)) {
			return nil
		}
		return e.fail(w, n, failure)
	}
	stats, inspectErr := finalize.InspectDiff(ctx, finalize.ExecRunner{Dir: workdir(w, n)}, workdir(w, n))
	if inspectErr == nil && exceedsDiffBudget(w.Budget, stats) {
		summary := fmt.Sprintf("worker stopped at deterministic diff budget: %d files / %d lines exceeds %d files / %d lines; result: %s", stats.Files, stats.Lines, w.Budget.MaxChangedFiles, w.Budget.MaxDiffLines, truncate(result.Summary, 1000))
		_, applyErr := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: "supervisor", Mutations: []core.Mutation{
			{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("budget-%s-%d", n.ID, n.Attempt+1), NodeID: n.ID, Kind: "budget", Summary: summary}},
			{Op: "set_state", NodeID: n.ID, State: core.NodeWaiting},
		}, Rationale: "deterministic changed-file and diff-line budget"})
		return applyErr
	}
	return e.applyAgentResult(w, n, result, extractSessionID(stdoutBytes), n.Attempt+1)
}

func exceedsDiffBudget(budget core.Budget, stats finalize.DiffStats) bool {
	return stats.Files > budget.MaxChangedFiles || stats.Lines > budget.MaxDiffLines
}

func (e *Engine) applyAgentResult(w *core.Workflow, n *core.Node, result Result, streamSessionID string, attempt int) error {
	mut := make([]core.Mutation, 0, len(result.Mutations)+4)
	for _, proposed := range result.Mutations {
		mutation := proposed.Mutation
		if mutation.Op == "add_node" && mutation.Node != nil && mutation.Node.Runtime.Name == "" {
			node := *mutation.Node
			node.Runtime, node.Role = n.Runtime, n.Role
			mutation.Node = &node
		}
		mut = append(mut, mutation)
	}
	// Runtime protocol events, unlike the model-authored result, are the source
	// of truth for exact resume identity.
	sessionID := streamSessionID
	if sessionID == "" {
		sessionID = result.SessionID
	}
	if sessionID != "" && sessionID != n.SessionID {
		mut = append(mut, core.Mutation{Op: "set_session", NodeID: n.ID, Reason: sessionID})
	}
	for i := range result.Attestations {
		a := result.Attestations[i]
		if a.Verdict == "pass_with_runtime_limit" {
			a.Verdict = "repair"
		}
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
	mut = append(mut, core.Mutation{Op: "add_evidence", Evidence: &core.Evidence{ID: fmt.Sprintf("result-%s-%d", n.ID, attempt), NodeID: n.ID, Kind: "agent", Summary: result.Summary}}, core.Mutation{Op: "set_state", NodeID: n.ID, State: state})
	_, err := e.Store.Apply(core.Proposal{WorkflowID: w.ID, Actor: n.ID, Mutations: mut, Rationale: result.Summary})
	if err != nil {
		return e.fail(w, n, "runtime proposed an invalid workflow mutation: "+err.Error())
	}
	return nil
}

// observeRuntimeEvents tails a file that the child process writes directly.
// Direct file descriptors matter: if the supervisor crashes, the child keeps
// its descriptor and can finish emitting output instead of losing a Go pipe.
func observeRuntimeEvents(path string, stop <-chan struct{}, onSession func(string)) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f, err := os.Open(path)
		if err != nil {
			return
		}
		defer f.Close()
		buf := make([]byte, 32<<10)
		pending := make([]byte, 0, 32<<10)
		seen := false
		stopping := false
		for {
			n, readErr := f.Read(buf)
			if n > 0 && !seen {
				pending = append(pending, buf[:n]...)
				if id := extractSessionID(pending); id != "" {
					seen = true
					onSession(id)
				}
			}
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return
			}
			if n > 0 {
				continue
			}
			if stopping {
				return
			}
			select {
			case <-stop:
				stopping = true
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	return done
}

func (e *Engine) routeAfterLimit(w *core.Workflow, n *core.Node, failure string, rawOutput ...string) bool {
	if e.Preferences == nil || n.Role == "" {
		return false
	}
	class := preferences.ClassifyFailure(strings.Join(append([]string{failure}, rawOutput...), " "))
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

const resultSchema = `{"type":"object","required":["status","summary","session_id","mutations","attestations"],"properties":{"status":{"enum":["completed","continue","blocked","needs_human"]},"summary":{"type":"string"},"session_id":{"type":"string"},"mutations":{"type":"array","description":"Workflow mutations. Each item is a JSON-encoded core Mutation object. Use an empty array when no graph change is needed.","items":{"type":"string"}},"attestations":{"type":"array","items":{"type":"object","required":["verifier","verdict","summary","evidence_ids"],"properties":{"verifier":{"type":"string"},"verdict":{"type":"string"},"summary":{"type":"string"},"evidence_ids":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}}},"additionalProperties":false}`
