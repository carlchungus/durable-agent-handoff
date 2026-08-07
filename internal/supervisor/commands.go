package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type StartExecutionInput struct {
	NativeSession  NativeSessionIdentity `json:"native_session"`
	Prompt         string                `json:"prompt"`
	Goal           string                `json:"goal,omitempty"`
	Runtime        RuntimeSpec           `json:"runtime"`
	Fallbacks      []RuntimeSpec         `json:"fallbacks,omitempty"`
	Role           string                `json:"role,omitempty"`
	Root           string                `json:"root"`
	Authority      AuthoritySpec         `json:"authority"`
	Finalizer      FinalizerSpec         `json:"finalizer"`
	Budget         Budget                `json:"budget"`
	IdempotencyKey string                `json:"-"`
}

type startExecutionCommand struct{ Input StartExecutionInput }

func (c startExecutionCommand) commandType() string    { return "StartExecution" }
func (c startExecutionCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c startExecutionCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) StartExecution(ctx context.Context, input StartExecutionInput) (*Execution, Receipt, error) {
	canonical, err := canonicalDirectory(input.Root)
	if err != nil {
		return nil, Receipt{}, err
	}
	input.Root = canonical
	if input.Runtime.Sandbox == SandboxWorkspaceWrite && withinRoot(input.Root, s.root) {
		return nil, Receipt{}, errors.New("Supervisor state root must be outside a workspace-write execution root")
	}
	input.Authority.AllowedRoots = append([]string(nil), input.Authority.AllowedRoots...)
	input.Finalizer.RequiredChecks = append([]string(nil), input.Finalizer.RequiredChecks...)
	input.Finalizer.Verifiers = append([]string(nil), input.Finalizer.Verifiers...)
	input.Fallbacks = append([]RuntimeSpec(nil), input.Fallbacks...)
	for index, check := range input.Finalizer.RequiredChecks {
		input.Finalizer.RequiredChecks[index] = strings.TrimSpace(check)
	}
	for index, verifier := range input.Finalizer.Verifiers {
		input.Finalizer.Verifiers[index] = strings.TrimSpace(verifier)
	}
	for index, root := range input.Authority.AllowedRoots {
		input.Authority.AllowedRoots[index], err = canonicalDirectory(root)
		if err != nil {
			return nil, Receipt{}, fmt.Errorf("authority root: %w", err)
		}
	}
	sort.Strings(input.Authority.AllowedRoots)
	input.Authority.AllowedRoots = uniqueStrings(input.Authority.AllowedRoots)
	sort.Strings(input.Finalizer.RequiredChecks)
	input.Finalizer.RequiredChecks = uniqueStrings(input.Finalizer.RequiredChecks)
	sort.Strings(input.Finalizer.Verifiers)
	input.Finalizer.Verifiers = uniqueStrings(input.Finalizer.Verifiers)
	if input.Budget.MaxTaskAttempts == 0 && input.Budget.MaxLaunches == 0 {
		input.Budget = DefaultBudget()
	}
	receipt, err := s.Execute(ctx, startExecutionCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	execution := state.Executions[ExecutionID(receipt.ResourceID)]
	if execution == nil {
		return nil, receipt, errors.New("committed execution is absent from projection")
	}
	return cloneExecution(execution), receipt, nil
}

func (c startExecutionCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	if err := validateStartInput(in); err != nil {
		return nil, "", err
	}
	executionID := ExecutionID(stableID("exec", in.IdempotencyKey))
	workflowID := WorkflowID(stableID("workflow", in.IdempotencyKey))
	nodeID := NodeID(stableID("node", in.IdempotencyKey))
	sessionID := SessionID(stableID("session", in.IdempotencyKey))
	activityID := ActivityID(stableID("activity", in.IdempotencyKey+"/1"))
	digest, _ := c.digest()
	workflow := &Workflow{
		ID: workflowID, Root: in.Root, Role: in.Role, Authority: in.Authority, Finalizer: in.Finalizer, Budget: in.Budget,
		Nodes: map[NodeID]*Node{}, CreatedAt: now,
	}
	title := in.Goal
	if strings.TrimSpace(title) == "" {
		title = "Root execution"
	}
	node := &Node{ID: nodeID, WorkflowID: workflowID, Title: title, Work: WorkSpec{Kind: "agent", Prompt: in.Prompt, Root: in.Root, Runtime: in.Runtime, Fallbacks: append([]RuntimeSpec(nil), in.Fallbacks...)}, CreatedAt: now}
	session := &Session{ID: sessionID, WorkflowID: workflowID, Native: in.NativeSession, Root: in.Root, CreatedAt: now}
	activity := &Activity{ID: activityID, WorkflowID: workflowID, NodeID: nodeID, SessionID: sessionID, Generation: 1, Prompt: in.Prompt, CreatedAt: now}
	execution := &Execution{ID: executionID, WorkflowID: workflowID, RootNodeID: nodeID, SessionID: sessionID, FirstActivity: activityID, IdempotencyKey: in.IdempotencyKey, InputDigest: digest, CreatedAt: now}
	return []DomainEvent{mustEvent(eventExecutionStarted, executionStartedEvent{Execution: execution, Workflow: workflow, Node: node, Session: session, Activity: activity})}, string(executionID), nil
}

func validateStartInput(in StartExecutionInput) error {
	if strings.TrimSpace(in.IdempotencyKey) == "" || strings.TrimSpace(in.Prompt) == "" {
		return errors.New("idempotency key and prompt are required")
	}
	if strings.TrimSpace(in.NativeSession.Runtime) == "" {
		return errors.New("native session runtime is required")
	}
	if in.NativeSession.Runtime != in.Runtime.Name {
		return errors.New("native session runtime must match RuntimeSpec")
	}
	if err := validateRuntime(in.Runtime); err != nil {
		return err
	}
	for _, fallback := range in.Fallbacks {
		if err := validateRuntime(fallback); err != nil {
			return fmt.Errorf("fallback runtime: %w", err)
		}
		if in.Authority.Sandbox == SandboxReadOnly && fallback.Sandbox != SandboxReadOnly {
			return errors.New("read-only workflow cannot use a write-capable fallback")
		}
	}
	seenRuntimeNames := map[string]bool{in.Runtime.Name: true}
	for _, fallback := range in.Fallbacks {
		if seenRuntimeNames[fallback.Name] {
			return fmt.Errorf("runtime fallback reuses provider identity %q", fallback.Name)
		}
		seenRuntimeNames[fallback.Name] = true
	}
	if !in.Authority.HumanAuthorized || strings.TrimSpace(in.Authority.RequestedBy) == "" {
		return errors.New("StartExecution requires explicit human authorization")
	}
	if in.Authority.Sandbox != in.Runtime.Sandbox {
		return errors.New("runtime authority must exactly match the authorized sandbox")
	}
	if in.Budget.MaxTaskAttempts < 1 || in.Budget.MaxLaunches < in.Budget.MaxTaskAttempts {
		return errors.New("budget requires positive task attempts and at least as many OS launches")
	}
	if in.Finalizer.Enabled && (!in.Finalizer.RequireHuman || !in.Finalizer.RequireVerifier || len(in.Finalizer.Verifiers) == 0 || len(in.Finalizer.RequiredChecks) == 0) {
		return errors.New("enabled finalizer requires human, independent verifier, and named check gates")
	}
	if in.Finalizer.Enabled {
		for _, check := range in.Finalizer.RequiredChecks {
			if strings.TrimSpace(check) == "" {
				return errors.New("enabled finalizer requires non-empty named check gates")
			}
		}
		for _, verifier := range in.Finalizer.Verifiers {
			if strings.TrimSpace(verifier) == "" {
				return errors.New("enabled finalizer requires non-empty verifier identities")
			}
			if verifier == in.Authority.RequestedBy {
				return errors.New("enabled finalizer verifier must differ from workflow requester")
			}
		}
	}
	allowed := false
	for _, root := range in.Authority.AllowedRoots {
		canonical, err := canonicalDirectory(root)
		if err != nil {
			return fmt.Errorf("authority root: %w", err)
		}
		if withinRoot(canonical, in.Root) {
			allowed = true
		}
	}
	if len(in.Authority.AllowedRoots) == 0 {
		allowed = true
	}
	if !allowed {
		return errors.New("canonical execution root is outside the authorized roots")
	}
	return nil
}

func validateRuntime(runtime RuntimeSpec) error {
	if strings.TrimSpace(runtime.Name) == "" {
		return errors.New("runtime name is required")
	}
	if runtime.Sandbox != SandboxReadOnly && runtime.Sandbox != SandboxWorkspaceWrite {
		return fmt.Errorf("unsupported runtime sandbox %q", runtime.Sandbox)
	}
	return nil
}

type SetRoleLadderInput struct {
	Role           string        `json:"role"`
	Candidates     []RuntimeSpec `json:"candidates"`
	IdempotencyKey string        `json:"-"`
}

type setRoleLadderCommand struct{ Input SetRoleLadderInput }

func (c setRoleLadderCommand) commandType() string     { return "SetRoleLadder" }
func (c setRoleLadderCommand) idempotencyKey() string  { return c.Input.IdempotencyKey }
func (c setRoleLadderCommand) digest() (string, error) { return digestValue(c.Input, "IdempotencyKey") }

func (s *Store) SetRoleLadder(ctx context.Context, input SetRoleLadderInput) ([]RuntimeSpec, Receipt, error) {
	receipt, err := s.Execute(ctx, setRoleLadderCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	return append([]RuntimeSpec(nil), state.RoleLadders[input.Role]...), receipt, nil
}

func (s *Store) RoleLadder(role string) ([]RuntimeSpec, error) {
	state, err := s.Projection()
	if err != nil {
		return nil, err
	}
	return append([]RuntimeSpec(nil), state.RoleLadders[strings.TrimSpace(role)]...), nil
}

func (c setRoleLadderCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	if strings.TrimSpace(c.Input.Role) == "" || len(c.Input.Candidates) == 0 {
		return nil, "", errors.New("role ladder requires a role and at least one candidate")
	}
	candidates := make([]RuntimeSpec, len(c.Input.Candidates))
	seen := map[string]bool{}
	seenNames := map[string]bool{}
	for index, candidate := range c.Input.Candidates {
		if err := validateRuntime(candidate); err != nil {
			return nil, "", fmt.Errorf("candidate %d: %w", index, err)
		}
		key := runtimeSpecKey(candidate)
		if seen[key] {
			return nil, "", fmt.Errorf("duplicate role ladder candidate %q", key)
		}
		if seenNames[candidate.Name] {
			return nil, "", fmt.Errorf("role ladder reuses provider identity %q", candidate.Name)
		}
		seen[key] = true
		seenNames[candidate.Name] = true
		candidates[index] = candidate
	}
	return []DomainEvent{mustEvent(eventRoleLadderSet, roleLadderSetEvent{Role: strings.TrimSpace(c.Input.Role), Candidates: candidates})}, c.Input.Role, nil
}

type StartFallbackActivityInput struct {
	ParentActivityID ActivityID  `json:"parent_activity_id"`
	Runtime          RuntimeSpec `json:"runtime"`
	IdempotencyKey   string      `json:"-"`
}

type startFallbackActivityCommand struct{ Input StartFallbackActivityInput }

func (c startFallbackActivityCommand) commandType() string    { return "StartFallbackActivity" }
func (c startFallbackActivityCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c startFallbackActivityCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) StartFallbackActivity(ctx context.Context, input StartFallbackActivityInput) (*Activity, Receipt, error) {
	receipt, err := s.Execute(ctx, startFallbackActivityCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	activity := state.Activities[ActivityID(receipt.ResourceID)]
	if activity == nil {
		return nil, receipt, errors.New("committed fallback Activity is absent")
	}
	return cloneActivity(activity), receipt, nil
}

func (c startFallbackActivityCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	parent := state.Activities[c.Input.ParentActivityID]
	if parent == nil || resultForActivity(state, parent.ID) != nil {
		return nil, "", errors.New("fallback requires an incomplete parent Activity")
	}
	workflow := state.Workflows[parent.WorkflowID]
	if workflow == nil {
		return nil, "", errors.New("fallback parent workflow does not exist")
	}
	node := workflow.Nodes[parent.NodeID]
	parentSession := state.Sessions[parent.SessionID]
	if workflow == nil || node == nil || parentSession == nil || parentSession.ImportedUnresolved {
		return nil, "", errors.New("fallback parent has broken durable identity")
	}
	if !runtimeAllowed(node.Work, c.Input.Runtime) {
		return nil, "", errors.New("fallback runtime is not an authorized Work candidate")
	}
	if c.Input.Runtime.Name == parentSession.Native.Runtime {
		return nil, "", errors.New("same-runtime retry must preserve the existing Session")
	}
	for _, activity := range state.Activities {
		if activity.ParentActivityID != parent.ID {
			continue
		}
		session := state.Sessions[activity.SessionID]
		if session != nil && session.Native.Runtime == c.Input.Runtime.Name {
			return nil, "", errors.New("fallback child already exists")
		}
	}
	sessionID := SessionID(stableID("session", c.Input.IdempotencyKey))
	activityID := ActivityID(stableID("activity", c.Input.IdempotencyKey))
	session := &Session{ID: sessionID, WorkflowID: workflow.ID, Native: NativeSessionIdentity{Runtime: c.Input.Runtime.Name}, ParentID: parentSession.ID, Root: parentSession.Root, CreatedAt: now}
	activity := &Activity{ID: activityID, WorkflowID: parent.WorkflowID, NodeID: parent.NodeID, SessionID: sessionID, Generation: parent.Generation, ParentActivityID: parent.ID, Prompt: parent.Prompt, DependencyBindings: append([]ResultBinding(nil), parent.DependencyBindings...), CreatedAt: now}
	return []DomainEvent{mustEvent(eventFallbackQueued, fallbackQueuedEvent{Session: session, Activity: activity})}, string(activityID), nil
}

func FindFallbackActivity(state *State, parent ActivityID, runtime RuntimeSpec) *Activity {
	if state == nil {
		return nil
	}
	for _, activity := range state.Activities {
		if activity.ParentActivityID != parent {
			continue
		}
		session := state.Sessions[activity.SessionID]
		if session != nil && session.Native.Runtime == runtime.Name {
			return cloneActivity(activity)
		}
	}
	return nil
}

func runtimeSpecKey(runtime RuntimeSpec) string {
	return strings.Join([]string{runtime.Name, runtime.Executable, runtime.Model, runtime.Effort, string(runtime.Sandbox)}, "\x00")
}

type AddNodeInput struct {
	WorkflowID     WorkflowID `json:"workflow_id"`
	NodeID         NodeID     `json:"node_id"`
	Title          string     `json:"title"`
	Work           WorkSpec   `json:"work"`
	Dependencies   []NodeID   `json:"dependencies,omitempty"`
	Actor          string     `json:"actor"`
	IdempotencyKey string     `json:"-"`
}

type addNodeCommand struct{ Input AddNodeInput }

func (c addNodeCommand) commandType() string     { return "AddNode" }
func (c addNodeCommand) idempotencyKey() string  { return c.Input.IdempotencyKey }
func (c addNodeCommand) digest() (string, error) { return digestValue(c.Input, "IdempotencyKey") }

func (s *Store) AddNode(ctx context.Context, input AddNodeInput) (*Node, Receipt, error) {
	receipt, err := s.Execute(ctx, addNodeCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	node := lookupNode(state, input.WorkflowID, NodeID(receipt.ResourceID))
	if node == nil {
		return nil, receipt, errors.New("committed node is absent from projection")
	}
	return cloneNode(node), receipt, nil
}

func (c addNodeCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	workflow := state.Workflows[in.WorkflowID]
	if workflow == nil {
		return nil, "", errors.New("workflow does not exist")
	}
	if strings.TrimSpace(in.Actor) == "" || strings.TrimSpace(string(in.NodeID)) == "" || strings.TrimSpace(in.Title) == "" {
		return nil, "", errors.New("actor, node id, and title are required")
	}
	if workflow.Nodes[in.NodeID] != nil {
		return nil, "", errors.New("node identity already exists")
	}
	if in.Work.Root == "" {
		in.Work.Root = workflow.Root
	}
	canonical, err := canonicalDirectory(in.Work.Root)
	if err != nil {
		return nil, "", err
	}
	in.Work.Root = canonical
	if !withinRoot(workflow.Root, canonical) {
		return nil, "", errors.New("node work root must be inside the canonical workflow root")
	}
	if err = validateRuntime(in.Work.Runtime); err != nil {
		return nil, "", err
	}
	seenRuntimeNames := map[string]bool{in.Work.Runtime.Name: true}
	for _, fallback := range in.Work.Fallbacks {
		if err = validateRuntime(fallback); err != nil {
			return nil, "", fmt.Errorf("fallback runtime: %w", err)
		}
		if workflow.Authority.Sandbox == SandboxReadOnly && fallback.Sandbox != SandboxReadOnly {
			return nil, "", errors.New("read-only workflow cannot add write-capable fallback")
		}
		if seenRuntimeNames[fallback.Name] {
			return nil, "", fmt.Errorf("runtime fallback reuses provider identity %q", fallback.Name)
		}
		seenRuntimeNames[fallback.Name] = true
	}
	if workflow.Authority.Sandbox == SandboxReadOnly && in.Work.Runtime.Sandbox != SandboxReadOnly {
		return nil, "", errors.New("read-only workflow cannot add write-capable work")
	}
	for _, dependency := range in.Dependencies {
		if workflow.Nodes[dependency] == nil {
			return nil, "", fmt.Errorf("dependency %s does not exist", dependency)
		}
	}
	node := &Node{ID: in.NodeID, WorkflowID: in.WorkflowID, Title: in.Title, Work: in.Work, Dependencies: append([]NodeID(nil), in.Dependencies...), CreatedAt: now}
	return []DomainEvent{mustEvent(eventNodeAdded, nodeAddedEvent{Node: node})}, string(node.ID), nil
}

type QueueActivityInput struct {
	WorkflowID     WorkflowID `json:"workflow_id"`
	NodeID         NodeID     `json:"node_id"`
	SessionID      SessionID  `json:"session_id"`
	IdempotencyKey string     `json:"-"`
}
type queueActivityCommand struct{ Input QueueActivityInput }

func (c queueActivityCommand) commandType() string     { return "QueueActivity" }
func (c queueActivityCommand) idempotencyKey() string  { return c.Input.IdempotencyKey }
func (c queueActivityCommand) digest() (string, error) { return digestValue(c.Input, "IdempotencyKey") }

func (s *Store) QueueActivity(ctx context.Context, input QueueActivityInput) (*Activity, Receipt, error) {
	receipt, err := s.Execute(ctx, queueActivityCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	activity := state.Activities[ActivityID(receipt.ResourceID)]
	if activity == nil {
		return nil, receipt, errors.New("committed activity is absent")
	}
	return cloneActivity(activity), receipt, nil
}
func (c queueActivityCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	workflow := state.Workflows[in.WorkflowID]
	node := lookupNode(state, in.WorkflowID, in.NodeID)
	session := state.Sessions[in.SessionID]
	if workflow == nil || node == nil || session == nil || session.WorkflowID != in.WorkflowID {
		return nil, "", errors.New("queue targets an unknown workflow, node, or session")
	}
	for _, activity := range state.Activities {
		if activity.NodeID == in.NodeID {
			return nil, "", errors.New("desired work already has an Activity; continuations require ContinueSession")
		}
	}
	bindings, err := dependencyBindings(state, node)
	if err != nil {
		return nil, "", err
	}
	id := ActivityID(stableID("activity", in.IdempotencyKey))
	activity := &Activity{ID: id, WorkflowID: in.WorkflowID, NodeID: in.NodeID, SessionID: in.SessionID, Generation: 1, Prompt: node.Work.Prompt, DependencyBindings: bindings, CreatedAt: now}
	return []DomainEvent{mustEvent(eventActivityQueued, activityQueuedEvent{Activity: activity})}, string(id), nil
}

type ContinueSessionInput struct {
	ExecutionID           ExecutionID `json:"execution_id"`
	SessionID             SessionID   `json:"session_id"`
	PredecessorActivityID ActivityID  `json:"predecessor_activity_id"`
	From                  string      `json:"from"`
	Message               string      `json:"message"`
	IdempotencyKey        string      `json:"-"`
}
type continueSessionCommand struct{ Input ContinueSessionInput }

func (c continueSessionCommand) commandType() string    { return "ContinueSession" }
func (c continueSessionCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c continueSessionCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) ContinueSession(ctx context.Context, input ContinueSessionInput) (*Activity, Receipt, error) {
	receipt, err := s.Execute(ctx, continueSessionCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	activity := state.Activities[ActivityID(receipt.ResourceID)]
	if activity == nil {
		return nil, receipt, errors.New("committed continuation is absent")
	}
	return cloneActivity(activity), receipt, nil
}
func (c continueSessionCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	execution := state.Executions[in.ExecutionID]
	session := state.Sessions[in.SessionID]
	predecessor := state.Activities[in.PredecessorActivityID]
	if execution == nil || session == nil || predecessor == nil || execution.WorkflowID != session.WorkflowID || predecessor.WorkflowID != execution.WorkflowID || predecessor.SessionID != in.SessionID {
		return nil, "", errors.New("continuation must name the execution's exact session lineage and predecessor")
	}
	if session.ImportedUnresolved || strings.TrimSpace(session.Native.ID) == "" {
		return nil, "", errors.New("continuation requires a previously bound exact native Session")
	}
	if strings.TrimSpace(in.From) == "" || strings.TrimSpace(in.Message) == "" {
		return nil, "", errors.New("reply sender and message are required")
	}
	if resultForActivity(state, predecessor.ID) == nil {
		return nil, "", errors.New("continuation predecessor has no immutable completed result")
	}
	generation := predecessor.Generation + 1
	for _, activity := range state.Activities {
		if activity.NodeID == predecessor.NodeID && activity.Generation >= generation {
			generation = activity.Generation + 1
		}
	}
	activityID := ActivityID(stableID("activity", in.IdempotencyKey))
	messageID := MessageID(stableID("message", in.IdempotencyKey))
	activity := &Activity{ID: activityID, WorkflowID: predecessor.WorkflowID, NodeID: predecessor.NodeID, SessionID: predecessor.SessionID, Generation: generation, ParentActivityID: predecessor.ID, Prompt: in.Message, DependencyBindings: append([]ResultBinding(nil), predecessor.DependencyBindings...), CreatedAt: now}
	message := &Message{ID: messageID, SessionID: session.ID, ActivityID: activityID, Body: in.Message, From: in.From, State: MessageQueued, CreatedAt: now}
	return []DomainEvent{mustEvent(eventActivityQueued, activityQueuedEvent{Activity: activity}), mustEvent(eventMessageQueued, messageEvent{Message: message})}, string(activityID), nil
}

type PrepareAttemptInput struct {
	ActivityID         ActivityID     `json:"activity_id"`
	ExpectedGeneration uint64         `json:"expected_generation"`
	Runtime            RuntimeSpec    `json:"runtime,omitempty"`
	CommandDigest      string         `json:"command_digest"`
	Outputs            OutputIdentity `json:"outputs"`
	IdempotencyKey     string         `json:"-"`
}
type prepareAttemptCommand struct{ Input PrepareAttemptInput }

func (c prepareAttemptCommand) commandType() string    { return "PrepareAttempt" }
func (c prepareAttemptCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c prepareAttemptCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) PrepareAttempt(ctx context.Context, input PrepareAttemptInput) (*Attempt, Receipt, error) {
	receipt, err := s.Execute(ctx, prepareAttemptCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	attempt := state.Attempts[AttemptID(receipt.ResourceID)]
	if attempt == nil {
		return nil, receipt, errors.New("committed attempt is absent")
	}
	return cloneAttempt(attempt), receipt, nil
}
func (c prepareAttemptCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	activity := state.Activities[in.ActivityID]
	if activity == nil || activity.Generation != in.ExpectedGeneration {
		return nil, "", ErrFenced
	}
	if resultForActivity(state, activity.ID) != nil {
		return nil, "", errors.New("completed Activity is immutable")
	}
	if fallbackChildForActivity(state, activity.ID) != nil {
		return nil, "", ErrFenced
	}
	if strings.TrimSpace(in.CommandDigest) == "" || strings.TrimSpace(in.Outputs.Stdout) == "" || strings.TrimSpace(in.Outputs.Stderr) == "" {
		return nil, "", errors.New("attempt requires command digest and exact output identities")
	}
	workflow := state.Workflows[activity.WorkflowID]
	node := workflow.Nodes[activity.NodeID]
	session := state.Sessions[activity.SessionID]
	if state.Pauses[activity.WorkflowID] != nil {
		return nil, "", ErrFenced
	}
	if !workflow.Authority.HumanAuthorized || session == nil || session.ImportedUnresolved {
		return nil, "", errors.New("Activity lacks human-authorized execution authority")
	}
	if workflow.Authority.Sandbox == SandboxReadOnly && node.Work.Runtime.Sandbox != SandboxReadOnly {
		return nil, "", errors.New("Activity runtime exceeds Workflow authority")
	}
	runtimeSpec := node.Work.Runtime
	if in.Runtime.Name != "" {
		runtimeSpec = in.Runtime
	}
	if !runtimeAllowed(node.Work, runtimeSpec) {
		return nil, "", errors.New("Attempt runtime is not an authorized primary or fallback candidate")
	}
	if workflow.Authority.Sandbox == SandboxReadOnly && runtimeSpec.Sandbox != SandboxReadOnly {
		return nil, "", errors.New("Attempt fallback exceeds Workflow authority")
	}
	launches, turns := attemptCounts(state, activity.WorkflowID, activity.NodeID)
	if launches >= workflow.Budget.MaxLaunches {
		return nil, "", errors.New("OS launch budget exhausted")
	}
	if turns >= workflow.Budget.MaxTaskAttempts {
		return nil, "", errors.New("task-attempt budget exhausted")
	}
	for _, attempt := range state.Attempts {
		if attempt.ActivityID == activity.ID && !attemptTerminal(attempt) {
			return nil, "", errors.New("Activity already has a live Attempt")
		}
	}
	for _, lease := range state.Leases {
		if lease.ReleasedAt.IsZero() && lease.CanonicalWorktree == node.Work.Root {
			return nil, "", ErrLeaseHeld
		}
	}
	ordinal := 1
	for _, attempt := range state.Attempts {
		if attempt.ActivityID == activity.ID && attempt.Ordinal >= ordinal {
			ordinal = attempt.Ordinal + 1
		}
	}
	attemptID := AttemptID(stableID("attempt", in.IdempotencyKey))
	leaseID := LeaseID(stableID("lease", in.IdempotencyKey))
	attempt := &Attempt{ID: attemptID, ActivityID: activity.ID, ActivityGeneration: activity.Generation, Ordinal: ordinal, Runtime: runtimeSpec, CommandDigest: in.CommandDigest, Outputs: in.Outputs, LeaseID: leaseID, CreatedAt: now}
	lease := &Lease{ID: leaseID, CanonicalWorktree: node.Work.Root, ActivityID: activity.ID, ActivityGeneration: activity.Generation, AttemptID: attemptID, AcquiredAt: now}
	events := []DomainEvent{mustEvent(eventAttemptPrepared, attemptPreparedEvent{Attempt: attempt, Lease: lease})}
	generation := nextDeliveryGeneration(state, activity.SessionID)
	for _, message := range orderedMessages(state) {
		if message.ActivityID == activity.ID && message.State == MessageQueued {
			events = append(events, mustEvent(eventMessageDispatched, messageDispatchEvent{MessageID: message.ID, AttemptID: attemptID, DeliveryGeneration: generation}))
		}
	}
	return events, string(attemptID), nil
}

type RecordMilestoneInput struct {
	ActivityID         ActivityID `json:"activity_id"`
	ExpectedGeneration uint64     `json:"expected_generation"`
	AttemptID          AttemptID  `json:"attempt_id"`
	LeaseID            LeaseID    `json:"lease_id"`
	Milestone          Milestone  `json:"milestone"`
	IdempotencyKey     string     `json:"-"`
}
type recordMilestoneCommand struct{ Input RecordMilestoneInput }

func (c recordMilestoneCommand) commandType() string    { return "RecordMilestone" }
func (c recordMilestoneCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c recordMilestoneCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}
func (s *Store) RecordMilestone(ctx context.Context, input RecordMilestoneInput) (Receipt, error) {
	return s.Execute(ctx, recordMilestoneCommand{Input: input})
}
func (c recordMilestoneCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	attempt := state.Attempts[in.AttemptID]
	activity := state.Activities[in.ActivityID]
	lease := state.Leases[in.LeaseID]
	if attempt == nil || activity == nil || lease == nil || activity.Generation != in.ExpectedGeneration || attempt.ActivityID != activity.ID || attempt.ActivityGeneration != activity.Generation || attempt.LeaseID != lease.ID || lease.AttemptID != attempt.ID || !lease.ReleasedAt.IsZero() {
		return nil, "", ErrFenced
	}
	if state.Pauses[activity.WorkflowID] != nil && in.Milestone.Kind != MilestoneExit {
		return nil, "", ErrFenced
	}
	m := cloneMilestone(in.Milestone)
	if m.At.IsZero() {
		m.At = now
	}
	if err := validateMilestone(state, attempt, activity, m); err != nil {
		return nil, "", err
	}
	events := []DomainEvent{mustEvent(eventMilestone, milestoneEvent{AttemptID: attempt.ID, Milestone: m})}
	terminal := m.Kind == MilestoneExit || m.Kind == MilestoneAdapterStartFailed || m.Kind == MilestoneProviderUnavailable
	if m.Kind == MilestoneResult {
		result := &Result{ID: ResultID(stableID("result", string(activity.ID))), WorkflowID: activity.WorkflowID, NodeID: activity.NodeID, ActivityID: activity.ID, AttemptID: attempt.ID, Generation: activity.Generation, Status: m.Result.Status, Summary: m.Result.Summary, CreatedAt: now}
		events = append(events, mustEvent(eventResultCreated, resultCreatedEvent{Result: result}))
		events = append(events, settleMessages(state, activity.ID, attempt.ID, true, now)...)
	}
	if terminal {
		if m.Kind != MilestoneExit || resultForActivity(state, activity.ID) == nil {
			events = append(events, settleMessages(state, activity.ID, attempt.ID, false, now)...)
		}
		if m.Kind == MilestoneExit {
			events = append(events, mustEvent(eventLeaseReleased, leaseReleasedEvent{LeaseID: lease.ID, At: now}))
		}
	}
	return events, string(attempt.ID), nil
}

func validateMilestone(state *State, attempt *Attempt, activity *Activity, m Milestone) error {
	if !validMilestoneKind(m.Kind) {
		return fmt.Errorf("unknown milestone %q", m.Kind)
	}
	turnStarted, resultSeen, exitSeen, failedBeforeExit := false, false, false, false
	for _, prior := range attempt.Milestones {
		turnStarted = turnStarted || prior.Kind == MilestoneTurnStarted
		resultSeen = resultSeen || prior.Kind == MilestoneResult
		exitSeen = exitSeen || prior.Kind == MilestoneExit
		failedBeforeExit = failedBeforeExit || prior.Kind == MilestoneAdapterStartFailed || prior.Kind == MilestoneProviderUnavailable
	}
	if exitSeen {
		return errors.New("Attempt is terminal")
	}
	if failedBeforeExit && m.Kind != MilestoneExit {
		return errors.New("only exit may follow a pre-exit failure")
	}
	if m.At.Before(attempt.CreatedAt) || len(attempt.Milestones) > 0 && m.At.Before(attempt.Milestones[len(attempt.Milestones)-1].At) {
		return errors.New("milestone timestamp precedes immutable Attempt history")
	}
	if resultSeen && m.Kind != MilestoneExit {
		return errors.New("only exit may follow an immutable result")
	}
	switch m.Kind {
	case MilestoneProcessSpawned:
		if attempt.Process != nil || m.Process == nil || m.Process.PID <= 0 || strings.TrimSpace(m.Process.StartToken) == "" {
			return errors.New("process_spawned requires one exact PID and start token")
		}
	case MilestoneSessionBound:
		if attempt.Process == nil || m.Session == nil {
			return errors.New("session_bound requires exact native identity")
		}
		session := state.Sessions[activity.SessionID]
		if session == nil || m.Session.Runtime != session.Native.Runtime || (session.Native.ID != "" && *m.Session != session.Native) {
			return errors.New("runtime bound a different native session identity")
		}
	case MilestoneTurnStarted:
		if attempt.Process == nil || turnStarted {
			return errors.New("turn_started requires a spawned process and occurs once")
		}
	case MilestoneEffectStarted:
		if !turnStarted || strings.TrimSpace(m.Effect) == "" {
			return errors.New("effect_started requires a started turn and typed effect")
		}
	case MilestoneMeaningfulProgress:
		if !turnStarted || strings.TrimSpace(m.Progress) == "" {
			return errors.New("meaningful_progress requires a started turn and semantic progress")
		}
	case MilestoneResult:
		if !turnStarted || resultSeen || m.Result == nil || strings.TrimSpace(m.Result.Summary) == "" {
			return errors.New("result requires one started turn and a non-empty typed result")
		}
		if m.Result.Status != "completed" && m.Result.Status != "needs_human" && m.Result.Status != "blocked" {
			return fmt.Errorf("unsupported result status %q", m.Result.Status)
		}
	case MilestoneProviderUnavailable:
		if turnStarted || strings.TrimSpace(m.Failure) == "" {
			return errors.New("provider_unavailable is pre-turn and requires a classified reason")
		}
	case MilestoneAdapterStartFailed:
		if turnStarted || strings.TrimSpace(m.Failure) == "" {
			return errors.New("adapter_start_failed is pre-turn and requires a reason")
		}
	case MilestoneExit:
		if m.Exit == nil {
			return errors.New("exit requires an exit payload")
		}
	}
	return nil
}

type RecordAttestationInput struct {
	ResultID       ResultID `json:"result_id"`
	Verifier       string   `json:"verifier"`
	Verdict        string   `json:"verdict"`
	Summary        string   `json:"summary"`
	EvidenceIDs    []string `json:"evidence_ids,omitempty"`
	IdempotencyKey string   `json:"-"`
}

type recordAttestationCommand struct{ Input RecordAttestationInput }

func (c recordAttestationCommand) commandType() string    { return "RecordAttestation" }
func (c recordAttestationCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c recordAttestationCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

// RecordAttestation is deliberately separate from a worker Result. Only a
// verifier identity explicitly configured by the human-authorized Workflow can
// produce publication evidence, so a runtime cannot self-attest its own work.
func (s *Store) RecordAttestation(ctx context.Context, input RecordAttestationInput) (*Attestation, Receipt, error) {
	input.Verifier = strings.TrimSpace(input.Verifier)
	input.Summary = strings.TrimSpace(input.Summary)
	input.EvidenceIDs = append([]string(nil), input.EvidenceIDs...)
	sort.Strings(input.EvidenceIDs)
	input.EvidenceIDs = uniqueStrings(input.EvidenceIDs)
	receipt, err := s.Execute(ctx, recordAttestationCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	attestation := state.Attestations[AttestationID(receipt.ResourceID)]
	if attestation == nil {
		return nil, receipt, errors.New("committed attestation is absent")
	}
	return cloneAttestation(attestation), receipt, nil
}

func (c recordAttestationCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	result := state.Results[in.ResultID]
	if result == nil {
		return nil, "", errors.New("attestation targets an unknown immutable Result")
	}
	workflow := state.Workflows[result.WorkflowID]
	if workflow == nil || !workflow.Finalizer.Enabled || !workflow.Finalizer.RequireVerifier {
		return nil, "", errors.New("Workflow has no independent verifier gate")
	}
	allowed := false
	for _, verifier := range workflow.Finalizer.Verifiers {
		if verifier == in.Verifier {
			allowed = true
			break
		}
	}
	if !allowed || in.Verifier == workflow.Authority.RequestedBy {
		return nil, "", errors.New("attestation verifier is not an authorized independent identity")
	}
	for _, existing := range state.Attestations {
		if existing.ResultID == result.ID && existing.Verifier == in.Verifier {
			return nil, "", ErrDuplicateAttestation
		}
	}
	attestation := Attestation{Verifier: in.Verifier, Verdict: in.Verdict, Summary: in.Summary, EvidenceIDs: append([]string(nil), in.EvidenceIDs...)}
	if err := validateSourceAttestation(attestation); err != nil {
		return nil, "", err
	}
	switch attestation.Verdict {
	case "fail_blocking":
		attestation.RawVerdict, attestation.Verdict = "fail_blocking", "blocked"
	case "pass_with_limit", "pass_with_runtime_limit":
		attestation.RawVerdict, attestation.Verdict = attestation.Verdict, "repair"
	}
	attestation.ID = AttestationID(stableID("attestation", in.IdempotencyKey))
	attestation.ResultID = result.ID
	attestation.At = now
	return []DomainEvent{mustEvent(eventAttestationRecorded, attestationRecordedEvent{Attestation: &attestation})}, string(attestation.ID), nil
}

// PauseWorkflowInput is the durable control-plane request used by cloud
// callers and the CLI. The command records exact stop controls in one journal
// transaction. The executor applies those controls; only terminal exit
// evidence releases the writer Lease.
type PauseWorkflowInput struct {
	WorkflowID     WorkflowID `json:"workflow_id"`
	RequestedBy    string     `json:"requested_by"`
	IdempotencyKey string     `json:"-"`
}

type pauseWorkflowCommand struct{ Input PauseWorkflowInput }

func (c pauseWorkflowCommand) commandType() string    { return "PauseWorkflow" }
func (c pauseWorkflowCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c pauseWorkflowCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) PauseWorkflow(ctx context.Context, input PauseWorkflowInput) (*Pause, Receipt, error) {
	receipt, err := s.Execute(ctx, pauseWorkflowCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	pause := state.Pauses[input.WorkflowID]
	if pause == nil {
		return nil, receipt, errors.New("committed pause is absent from projection")
	}
	copy := *pause
	copy.FencedAttemptIDs = append([]AttemptID(nil), pause.FencedAttemptIDs...)
	copy.ReleasedLeaseIDs = append([]LeaseID(nil), pause.ReleasedLeaseIDs...)
	return &copy, receipt, nil
}

func (c pauseWorkflowCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	if state.Workflows[in.WorkflowID] == nil {
		return nil, "", errors.New("workflow does not exist")
	}
	if strings.TrimSpace(in.RequestedBy) == "" {
		return nil, "", errors.New("pause requires a requesting identity")
	}
	if state.Pauses[in.WorkflowID] != nil {
		return nil, "", errors.New("workflow is already paused; retry the original idempotency key")
	}
	pause := &Pause{WorkflowID: in.WorkflowID, RequestedBy: in.RequestedBy, RequestedAt: now, Phase: PauseRequested}
	var events []DomainEvent
	for _, attempt := range orderedAttempts(state, in.WorkflowID) {
		lease := state.Leases[attempt.LeaseID]
		if lease == nil || !lease.ReleasedAt.IsZero() || attemptHasExit(attempt) {
			continue
		}
		activity := state.Activities[attempt.ActivityID]
		if activity == nil || attempt.ActivityGeneration != activity.Generation {
			continue
		}
		if existing := AcceptedControlForAttempt(state, activity, attempt); existing != nil {
			// A pause fences the already accepted exact control. It must not
			// create a second accepted stop/pause command for the same Attempt.
			events = append(events, settleMessages(state, activity.ID, attempt.ID, false, now)...)
			pause.FencedAttemptIDs = append(pause.FencedAttemptIDs, attempt.ID)
			continue
		}
		control := &Control{ID: ControlID(stableID("control", string(in.WorkflowID)+"/pause/"+string(attempt.ID))), Kind: "pause", Actor: in.RequestedBy, ActivityID: activity.ID, ExpectedGeneration: activity.Generation, ExpectedAttemptID: attempt.ID, Accepted: true, CreatedAt: now}
		events = append(events, mustEvent(eventControlRecorded, controlRecordedEvent{Control: control}))
		events = append(events, settleMessages(state, activity.ID, attempt.ID, false, now)...)
		pause.FencedAttemptIDs = append(pause.FencedAttemptIDs, attempt.ID)
	}
	if len(pause.FencedAttemptIDs) == 0 {
		pause.Phase = PauseCompleted
		pause.CompletedAt = now
	} else {
		pause.Phase = PauseDraining
	}
	events = append(events, mustEvent(eventWorkflowPaused, workflowPausedEvent{Pause: pause}))
	return events, string(in.WorkflowID), nil
}

type SettlePauseInput struct {
	WorkflowID     WorkflowID `json:"workflow_id"`
	IdempotencyKey string     `json:"-"`
}

type settlePauseCommand struct{ Input SettlePauseInput }

func (c settlePauseCommand) commandType() string    { return "SettlePause" }
func (c settlePauseCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c settlePauseCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) SettlePause(ctx context.Context, input SettlePauseInput) (*Pause, Receipt, error) {
	receipt, err := s.Execute(ctx, settlePauseCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	pause := state.Pauses[input.WorkflowID]
	if pause == nil {
		return nil, receipt, errors.New("committed pause is absent from projection")
	}
	return clonePause(pause), receipt, nil
}

func (c settlePauseCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	pause := state.Pauses[c.Input.WorkflowID]
	if pause == nil {
		return nil, "", errors.New("workflow is not paused")
	}
	if pause.Phase == PauseCompleted {
		return nil, "", errors.New("pause is already settled; retry the original idempotency key")
	}
	for _, attemptID := range pause.FencedAttemptIDs {
		attempt := state.Attempts[attemptID]
		if attempt == nil || !attemptHasExit(attempt) {
			return nil, "", ErrPausePending
		}
		lease := state.Leases[attempt.LeaseID]
		if lease == nil || lease.ReleasedAt.IsZero() {
			return nil, "", ErrPausePending
		}
	}
	return []DomainEvent{mustEvent(eventPauseSettled, pauseSettledEvent{WorkflowID: c.Input.WorkflowID, CompletedAt: now})}, string(c.Input.WorkflowID), nil
}

type ApplyControlInput struct {
	ControlID          ControlID  `json:"control_id"`
	ActivityID         ActivityID `json:"activity_id"`
	ExpectedGeneration uint64     `json:"expected_generation"`
	AttemptID          AttemptID  `json:"attempt_id"`
	IdempotencyKey     string     `json:"-"`
}

type applyControlCommand struct{ Input ApplyControlInput }

func (c applyControlCommand) commandType() string    { return "ApplyControl" }
func (c applyControlCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c applyControlCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) ApplyControl(ctx context.Context, input ApplyControlInput) (*Control, Receipt, error) {
	receipt, err := s.Execute(ctx, applyControlCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	control := state.Controls[input.ControlID]
	if control == nil {
		return nil, receipt, errors.New("committed control is absent from projection")
	}
	return cloneControl(control), receipt, nil
}

func (c applyControlCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	control := state.Controls[in.ControlID]
	activity := state.Activities[in.ActivityID]
	attempt := state.Attempts[in.AttemptID]
	if control == nil || !control.Accepted || !control.AppliedAt.IsZero() || activity == nil || attempt == nil || control.ActivityID != activity.ID || control.ExpectedGeneration != in.ExpectedGeneration || control.ExpectedAttemptID != attempt.ID || attempt.ActivityID != activity.ID || attempt.ActivityGeneration != activity.Generation {
		return nil, "", ErrFenced
	}
	return []DomainEvent{mustEvent(eventControlApplied, controlAppliedEvent{ControlID: control.ID, At: now})}, string(control.ID), nil
}

type RequestControlInput struct {
	ActivityID         ActivityID `json:"activity_id"`
	ExpectedGeneration uint64     `json:"expected_generation"`
	ExpectedAttemptID  AttemptID  `json:"expected_attempt_id"`
	Kind               string     `json:"kind"`
	Actor              string     `json:"actor"`
	IdempotencyKey     string     `json:"-"`
}
type requestControlCommand struct{ Input RequestControlInput }

func (c requestControlCommand) commandType() string    { return "RequestControl" }
func (c requestControlCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c requestControlCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}
func (s *Store) RequestControl(ctx context.Context, input RequestControlInput) (*Control, Receipt, error) {
	receipt, err := s.Execute(ctx, requestControlCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	control := state.Controls[ControlID(receipt.ResourceID)]
	return cloneControl(control), receipt, nil
}
func (c requestControlCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	if in.Kind != "stop" || strings.TrimSpace(in.Actor) == "" {
		return nil, "", errors.New("control requires actor and supported kind")
	}
	activity := state.Activities[in.ActivityID]
	attempt := state.Attempts[in.ExpectedAttemptID]
	exactAttempt := activity != nil && attempt != nil && activity.Generation == in.ExpectedGeneration && attempt.ActivityID == activity.ID && attempt.ActivityGeneration == activity.Generation
	if exactAttempt {
		if existing := AcceptedControlForAttempt(state, activity, attempt); existing != nil {
			return nil, "", fmt.Errorf("%w: %s", ErrControlAlreadyAccepted, existing.ID)
		}
	}
	workflowPaused := activity != nil && state.Pauses[activity.WorkflowID] != nil
	accepted := !workflowPaused && exactAttempt && !attemptTerminal(attempt)
	reason := ""
	if !accepted {
		reason = "activity is paused or generation or exact Attempt identity changed"
	}
	id := ControlID(stableID("control", in.IdempotencyKey))
	control := &Control{ID: id, Kind: in.Kind, Actor: in.Actor, ActivityID: in.ActivityID, ExpectedGeneration: in.ExpectedGeneration, ExpectedAttemptID: in.ExpectedAttemptID, Accepted: accepted, Reason: reason, CreatedAt: now}
	return []DomainEvent{mustEvent(eventControlRecorded, controlRecordedEvent{Control: control})}, string(id), nil
}

func dependencyBindings(state *State, node *Node) ([]ResultBinding, error) {
	bindings := make([]ResultBinding, 0, len(node.Dependencies))
	for _, dependency := range node.Dependencies {
		result := latestResultForNode(state, node.WorkflowID, dependency)
		if result == nil {
			return nil, fmt.Errorf("dependency %s has no immutable result", dependency)
		}
		bindings = append(bindings, ResultBinding{DependencyNodeID: dependency, ResultID: result.ID})
	}
	return bindings, nil
}
func latestResultForNode(state *State, workflowID WorkflowID, nodeID NodeID) *Result {
	var selected *Result
	for _, result := range state.Results {
		if result.WorkflowID == workflowID && result.NodeID == nodeID && result.Status == "completed" && (selected == nil || result.Generation > selected.Generation || result.Generation == selected.Generation && result.ID < selected.ID) {
			selected = result
		}
	}
	return selected
}
func resultForActivity(state *State, activityID ActivityID) *Result {
	for _, result := range state.Results {
		if result.ActivityID == activityID {
			return result
		}
	}
	return nil
}
func attemptCounts(state *State, workflowID WorkflowID, nodeID NodeID) (launches, turns int) {
	for _, attempt := range state.Attempts {
		activity := state.Activities[attempt.ActivityID]
		if activity == nil || activity.WorkflowID != workflowID || activity.NodeID != nodeID {
			continue
		}
		launches++
		for _, m := range attempt.Milestones {
			if m.Kind == MilestoneTurnStarted && !hasProviderUnavailable(attempt) {
				turns++
				break
			}
		}
	}
	return
}
func attemptTerminal(attempt *Attempt) bool {
	return attemptHasExit(attempt)
}

func attemptHasExit(attempt *Attempt) bool {
	for _, m := range attempt.Milestones {
		if m.Kind == MilestoneExit {
			return true
		}
	}
	return false
}

func runtimeAllowed(work WorkSpec, wanted RuntimeSpec) bool {
	if sameRuntime(work.Runtime, wanted) {
		return true
	}
	for _, candidate := range work.Fallbacks {
		if sameRuntime(candidate, wanted) {
			return true
		}
	}
	return false
}

func sameRuntime(a, b RuntimeSpec) bool {
	return a.Name == b.Name && a.Executable == b.Executable && a.Model == b.Model && a.Effort == b.Effort && a.Sandbox == b.Sandbox
}

func hasProviderUnavailable(attempt *Attempt) bool {
	for _, milestone := range attempt.Milestones {
		if milestone.Kind == MilestoneProviderUnavailable {
			return true
		}
	}
	return false
}

func validMilestoneKind(kind MilestoneKind) bool {
	switch kind {
	case MilestoneProcessSpawned, MilestoneSessionBound, MilestoneTurnStarted, MilestoneEffectStarted, MilestoneMeaningfulProgress, MilestoneResult, MilestoneProviderUnavailable, MilestoneAdapterStartFailed, MilestoneExit:
		return true
	}
	return false
}
func settleMessages(state *State, activityID ActivityID, attemptID AttemptID, delivered bool, at time.Time) []DomainEvent {
	var events []DomainEvent
	for _, message := range orderedMessages(state) {
		if message.ActivityID == activityID && message.AttemptID == attemptID && message.State == MessageDispatched {
			events = append(events, mustEvent(eventMessageSettled, messageSettledEvent{MessageID: message.ID, Delivered: delivered, At: at}))
		}
	}
	return events
}
func nextDeliveryGeneration(state *State, sessionID SessionID) uint64 {
	var generation uint64 = 1
	for _, message := range state.Messages {
		if message.SessionID == sessionID && message.DeliveryGeneration >= generation {
			generation = message.DeliveryGeneration + 1
		}
	}
	return generation
}
func orderedMessages(state *State) []*Message {
	messages := make([]*Message, 0, len(state.Messages))
	for _, message := range state.Messages {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].CreatedAt.Before(messages[j].CreatedAt) || messages[i].CreatedAt.Equal(messages[j].CreatedAt) && messages[i].ID < messages[j].ID
	})
	return messages
}
func validateSourceAttestation(attestation Attestation) error {
	if strings.TrimSpace(attestation.Verifier) == "" || strings.TrimSpace(attestation.Summary) == "" {
		return errors.New("attestation requires verifier and summary")
	}
	switch attestation.Verdict {
	case "pass", "repair", "blocked", "fail_blocking", "pass_with_limit", "pass_with_runtime_limit":
		return nil
	default:
		return fmt.Errorf("unknown attestation verdict %q", attestation.Verdict)
	}
}
func canonicalDirectory(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("root is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("root is not a directory")
	}
	return filepath.Clean(canonical), nil
}

func withinRoot(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func uniqueStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}
func stableID(prefix, key string) string {
	sum := sha256.Sum256([]byte(prefix + "\x00" + key))
	return prefix + "_" + hex.EncodeToString(sum[:12])
}
func digestValue(value any, omitField string) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	var object map[string]any
	if err = json.Unmarshal(raw, &object); err == nil {
		delete(object, omitField)
		delete(object, "IdempotencyKey")
		raw, err = json.Marshal(object)
	}
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
