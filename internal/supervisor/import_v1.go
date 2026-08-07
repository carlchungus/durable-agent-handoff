package supervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/core"
)

type ImportV1Input struct {
	SourceRoot     string `json:"source_root"`
	IdempotencyKey string `json:"-"`
}

type importV1Command struct {
	Input   ImportV1Input
	Digest  string
	Files   map[string]string
	History []legacyWorkflowHistory
}

func (c importV1Command) commandType() string     { return "ImportV1" }
func (c importV1Command) idempotencyKey() string  { return c.Input.IdempotencyKey }
func (c importV1Command) digest() (string, error) { return c.Digest, nil }

// ImportV1 is a deterministic one-way import. It reads legacy ledgers, never
// their replaceable snapshots, and appends one v2 transaction. Source bytes are
// left untouched and are never consulted again by Supervisor execution.
func (s *Store) ImportV1(ctx context.Context, input ImportV1Input) (Receipt, error) {
	canonical, err := canonicalDirectory(input.SourceRoot)
	if err != nil {
		return Receipt{}, err
	}
	input.SourceRoot = canonical
	digest, files, rawFiles, err := digestLegacyLedgers(canonical)
	if err != nil {
		return Receipt{}, err
	}
	var history []legacyWorkflowHistory
	paths := make([]string, 0, len(rawFiles))
	for path := range rawFiles {
		if strings.HasPrefix(path, "workflows/") {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		item, parseErr := parseLegacyWorkflow(rawFiles[path])
		if parseErr != nil {
			return Receipt{}, fmt.Errorf("parse legacy %s: %w", path, parseErr)
		}
		history = append(history, item)
	}
	if len(history) == 0 {
		return Receipt{}, errors.New("legacy source has no workflow ledgers")
	}
	return s.Execute(ctx, importV1Command{Input: input, Digest: digest, Files: files, History: history})
}

func (c importV1Command) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	if _, exists := state.LegacyImports[c.Digest]; exists {
		return nil, "", errors.New("legacy source digest was already imported")
	}
	data := legacyImportedEvent{Import: LegacyImport{SourceDigest: c.Digest, SourceRoot: c.Input.SourceRoot, ImportedAt: now, Records: len(c.Files), Files: cloneStringMap(c.Files)}}
	for _, history := range c.History {
		legacy := history.Workflow
		if legacy == nil {
			return nil, "", errors.New("legacy workflow history has no creation event")
		}
		root, err := canonicalDirectory(legacy.Root)
		if err != nil {
			return nil, "", fmt.Errorf("legacy workflow %s root: %w", legacy.ID, err)
		}
		workflowID := WorkflowID(stableID("workflow", c.Digest+"/"+legacy.ID))
		workflow := &Workflow{ID: workflowID, Root: root, Authority: AuthoritySpec{RequestedBy: "legacy-import", Sandbox: SandboxReadOnly}, Budget: migrateBudget(legacy.Budget), Nodes: map[NodeID]*Node{}, CreatedAt: legacy.CreatedAt}
		if workflow.CreatedAt.IsZero() {
			workflow.CreatedAt = now
		}
		for _, legacyNodeID := range legacy.Order {
			legacyNode := legacy.Nodes[legacyNodeID]
			if legacyNode == nil {
				continue
			}
			nodeID := NodeID(stableID("node", c.Digest+"/"+legacy.ID+"/"+legacyNode.ID))
			workRoot := root
			if legacyNode.Worktree != "" {
				candidate, canonicalErr := canonicalDirectory(legacyNode.Worktree)
				if canonicalErr != nil {
					return nil, "", fmt.Errorf("legacy node %s worktree: %w", legacyNode.ID, canonicalErr)
				}
				if !withinRoot(root, candidate) {
					return nil, "", fmt.Errorf("legacy node %s worktree is outside workflow root", legacyNode.ID)
				}
				workRoot = candidate
			}
			dependencies := make([]NodeID, 0, len(legacyNode.DependsOn))
			for _, dependency := range legacyNode.DependsOn {
				dependencies = append(dependencies, NodeID(stableID("node", c.Digest+"/"+legacy.ID+"/"+dependency)))
			}
			node := &Node{ID: nodeID, WorkflowID: workflowID, Title: legacyNode.Title, Work: WorkSpec{Kind: legacyNode.Kind, Prompt: legacyNode.Prompt, Root: workRoot, Runtime: migrateRuntime(legacyNode.Runtime)}, Dependencies: dependencies, CreatedAt: legacyNode.CreatedAt}
			if node.CreatedAt.IsZero() {
				node.CreatedAt = workflow.CreatedAt
			}
			workflow.Nodes[nodeID] = node
			workflow.Order = append(workflow.Order, nodeID)
			sessionID := SessionID(stableID("session", c.Digest+"/"+legacy.ID+"/"+legacyNode.ID))
			native := NativeSessionIdentity{Runtime: legacyNode.Runtime.Name, ID: legacyNode.SessionID}
			session := &Session{ID: sessionID, WorkflowID: workflowID, Native: native, ImportedUnresolved: strings.TrimSpace(native.ID) == "", Root: workRoot, CreatedAt: node.CreatedAt}
			data.Sessions = append(data.Sessions, session)
			generations := history.Completions[legacyNode.ID]
			if generations == 0 && legacyNode.State == core.NodeCompleted {
				generations = 1
			}
			var parent ActivityID
			for generation := 1; generation <= generations; generation++ {
				activityID := ActivityID(stableID("activity", c.Digest+"/"+legacy.ID+"/"+legacyNode.ID+fmt.Sprintf("/%d", generation)))
				activity := &Activity{ID: activityID, WorkflowID: workflowID, NodeID: nodeID, SessionID: sessionID, Generation: uint64(generation), ParentActivityID: parent, Prompt: legacyNode.Prompt, CreatedAt: node.CreatedAt.Add(time.Duration(generation-1) * time.Nanosecond)}
				attemptID := AttemptID(stableID("attempt", string(activityID)+"/legacy"))
				attempt := &Attempt{ID: attemptID, ActivityID: activityID, ActivityGeneration: uint64(generation), Ordinal: 1, Runtime: node.Work.Runtime, CommandDigest: "legacy-import", CreatedAt: activity.CreatedAt, Milestones: []Milestone{{Kind: MilestoneTurnStarted, At: activity.CreatedAt, SourceType: "legacy.import"}, {Kind: MilestoneResult, At: activity.CreatedAt, Result: &WorkerResult{Status: "completed", Summary: "Imported immutable legacy completion"}, SourceType: "legacy.import"}, {Kind: MilestoneExit, At: activity.CreatedAt, Exit: &Exit{Code: 0}, SourceType: "legacy.import"}}}
				resultID := ResultID(stableID("result", string(activityID)))
				result := &Result{ID: resultID, WorkflowID: workflowID, NodeID: nodeID, ActivityID: activityID, AttemptID: attemptID, Generation: uint64(generation), Status: "completed", Summary: "Imported immutable legacy completion", CreatedAt: activity.CreatedAt}
				data.Activities, data.Attempts, data.Results = append(data.Activities, activity), append(data.Attempts, attempt), append(data.Results, result)
				parent = activityID
			}
			if history.Reopened[legacyNode.ID] >= generations && history.Reopened[legacyNode.ID] > 0 && legacyNode.State != core.NodeCompleted {
				generation := generations + 1
				activityID := ActivityID(stableID("activity", c.Digest+"/"+legacy.ID+"/"+legacyNode.ID+fmt.Sprintf("/%d", generation)))
				data.Activities = append(data.Activities, &Activity{ID: activityID, WorkflowID: workflowID, NodeID: nodeID, SessionID: sessionID, Generation: uint64(generation), ParentActivityID: parent, Prompt: legacyNode.Prompt, CreatedAt: now})
			}
		}
		if len(workflow.Order) == 0 {
			return nil, "", fmt.Errorf("legacy workflow %s has no nodes", legacy.ID)
		}
		executionID := ExecutionID(stableID("exec", c.Digest+"/"+legacy.ID))
		rootNode := workflow.Order[0]
		var rootSession SessionID
		var firstActivity ActivityID
		for _, session := range data.Sessions {
			if session.WorkflowID == workflowID {
				rootSession = session.ID
				break
			}
		}
		for _, activity := range data.Activities {
			if activity.WorkflowID == workflowID && activity.NodeID == rootNode {
				firstActivity = activity.ID
				break
			}
		}
		data.Executions = append(data.Executions, &Execution{ID: executionID, WorkflowID: workflowID, RootNodeID: rootNode, SessionID: rootSession, FirstActivity: firstActivity, IdempotencyKey: c.Input.IdempotencyKey, InputDigest: c.Digest, CreatedAt: workflow.CreatedAt})
		data.Workflows = append(data.Workflows, workflow)
	}
	return []DomainEvent{mustEvent(eventLegacyImported, data)}, c.Digest, nil
}

type legacyWorkflowHistory struct {
	Workflow    *core.Workflow
	Completions map[string]int
	Reopened    map[string]int
}

func parseLegacyWorkflow(raw []byte) (legacyWorkflowHistory, error) {
	history := legacyWorkflowHistory{Completions: map[string]int{}, Reopened: map[string]int{}}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for {
		var entry struct {
			Type string          `json:"type"`
			At   time.Time       `json:"at"`
			Data json.RawMessage `json:"data"`
		}
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return history, err
		}
		switch entry.Type {
		case "workflow.created":
			var workflow core.Workflow
			if err = json.Unmarshal(entry.Data, &workflow); err != nil {
				return history, err
			}
			history.Workflow = &workflow
			for id, node := range workflow.Nodes {
				if node.State == core.NodeCompleted {
					history.Completions[id]++
				}
			}
		case "proposal.applied":
			if history.Workflow == nil {
				return history, errors.New("proposal preceded workflow creation")
			}
			var proposal core.Proposal
			if err = json.Unmarshal(entry.Data, &proposal); err != nil {
				return history, err
			}
			for _, mutation := range proposal.Mutations {
				if mutation.Op == "set_state" && mutation.State == core.NodeCompleted {
					history.Completions[mutation.NodeID]++
				}
				if mutation.Op == "reopen_agent" {
					history.Reopened[mutation.NodeID]++
				}
			}
			if err = core.ApplyProposal(history.Workflow, proposal, entry.At); err != nil {
				return history, err
			}
		}
	}
	if history.Workflow == nil {
		return history, errors.New("workflow ledger has no creation event")
	}
	return history, nil
}

func digestLegacyLedgers(root string) (string, map[string]string, map[string][]byte, error) {
	var paths []string
	for _, namespace := range []string{"workflows", "sessions", "activities", "teams"} {
		matches, err := filepath.Glob(filepath.Join(root, namespace, "*", "events.jsonl"))
		if err != nil {
			return "", nil, nil, err
		}
		paths = append(paths, matches...)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return "", nil, nil, errors.New("legacy source has no event ledgers")
	}
	whole := sha256.New()
	files, rawFiles := map[string]string{}, map[string][]byte{}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", nil, nil, err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return "", nil, nil, err
		}
		relative = filepath.ToSlash(relative)
		fileSum := sha256.Sum256(raw)
		files[relative] = hex.EncodeToString(fileSum[:])
		_, _ = whole.Write([]byte(relative))
		_, _ = whole.Write([]byte{0})
		_, _ = whole.Write(raw)
		_, _ = whole.Write([]byte{0})
		rawFiles[relative] = raw
	}
	return hex.EncodeToString(whole.Sum(nil)), files, rawFiles, nil
}

func migrateRuntime(runtime core.RuntimeSpec) RuntimeSpec {
	sandbox := Sandbox(runtime.Sandbox)
	if sandbox != SandboxReadOnly && sandbox != SandboxWorkspaceWrite {
		sandbox = SandboxWorkspaceWrite
	}
	// Legacy free-form argv is intentionally not promoted: v2 Drivers own the
	// complete safety envelope and imported executions remain inert until a
	// human explicitly promotes them with typed authority.
	return RuntimeSpec{Name: runtime.Name, Executable: runtime.Executable, Model: runtime.Model, Effort: runtime.Effort, Sandbox: sandbox}
}

func migrateBudget(budget core.Budget) Budget {
	maxAttempts := budget.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = DefaultBudget().MaxTaskAttempts
	}
	return Budget{MaxTaskAttempts: maxAttempts, MaxLaunches: maxAttempts * 4}
}

func cloneStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
