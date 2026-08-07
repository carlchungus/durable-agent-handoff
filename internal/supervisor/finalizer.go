package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/githubgate"
)

// FinalizationRequest is the authority-owned publication effect. Runtime
// Drivers never receive this capability; callers provide the exact repository,
// PR, named gates, idempotency key, and human approval required by policy.
type FinalizationRequest struct {
	ExecutionID    ExecutionID
	Repository     string
	PullRequest    string
	Gates          []string
	Method         string
	HumanApproved  bool
	IdempotencyKey string
}

type FinalizationResult struct {
	FinalizationID string            `json:"finalization_id"`
	PRURL          string            `json:"pr_url,omitempty"`
	HeadSHA        string            `json:"head_sha"`
	State          FinalizationState `json:"state"`
	Merged         bool              `json:"merged"`
	Summary        string            `json:"summary"`
}

type GateRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type PrepareFinalizationInput struct {
	ExecutionID    ExecutionID
	Repository     string
	PullRequest    string
	Gates          []string
	Method         string
	HumanApproved  bool
	HeadSHA        string
	PRURL          string
	IdempotencyKey string
}

type prepareFinalizationCommand struct{ Input PrepareFinalizationInput }

func (c prepareFinalizationCommand) commandType() string    { return "PrepareFinalization" }
func (c prepareFinalizationCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c prepareFinalizationCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) PrepareFinalization(ctx context.Context, input PrepareFinalizationInput) (*Finalization, Receipt, error) {
	receipt, err := s.Execute(ctx, prepareFinalizationCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	finalization := state.Finalizations[receipt.ResourceID]
	if finalization == nil {
		return nil, receipt, errors.New("committed finalization is absent")
	}
	return cloneFinalization(finalization), receipt, nil
}

func (c prepareFinalizationCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	if strings.TrimSpace(in.Repository) == "" || strings.TrimSpace(in.PullRequest) == "" || strings.TrimSpace(in.HeadSHA) == "" || len(in.Gates) == 0 {
		return nil, "", errors.New("finalization requires repository, pull request, exact head, and named gates")
	}
	execution := state.Executions[in.ExecutionID]
	if execution == nil {
		return nil, "", errors.New("execution does not exist")
	}
	workflow := state.Workflows[execution.WorkflowID]
	if workflow == nil || !workflow.Finalizer.Enabled {
		return nil, "", errors.New("finalizer is disabled")
	}
	gates, err := exactFinalizerChecks(in.Gates, workflow.Finalizer.RequiredChecks)
	if err != nil {
		return nil, "", err
	}
	if workflow.Finalizer.RequireHuman && !in.HumanApproved {
		return nil, "", errors.New("finalizer requires explicit human approval")
	}
	method := fallbackMethod(in.Method)
	if method != "merge" && method != "squash" && method != "rebase" {
		return nil, "", fmt.Errorf("unsupported merge method %q", method)
	}
	view, err := ProjectExecution(state, in.ExecutionID, workflow.CreatedAt)
	if err != nil {
		return nil, "", err
	}
	if view.Publication != PublicationEligible && !(view.Publication == PublicationAwaitingHuman && in.HumanApproved) {
		return nil, "", fmt.Errorf("publication is not eligible: %s", view.Publication)
	}
	finalizationID := stableID("finalization", in.IdempotencyKey)
	finalization := &Finalization{ID: finalizationID, ExecutionID: in.ExecutionID, WorkflowID: execution.WorkflowID, IdempotencyKey: in.IdempotencyKey, Repository: in.Repository, PullRequest: in.PullRequest, Gates: gates, Method: method, HumanApproved: in.HumanApproved, HeadSHA: in.HeadSHA, PRURL: in.PRURL, State: FinalizationPrepared, PreparedAt: now}
	return []DomainEvent{mustEvent(eventFinalizationPrepared, finalizationPreparedEvent{Finalization: finalization})}, finalizationID, nil
}

type SettleFinalizationInput struct {
	FinalizationID string
	State          FinalizationState
	Summary        string
	PRURL          string
	IdempotencyKey string
}

type settleFinalizationCommand struct{ Input SettleFinalizationInput }

func (c settleFinalizationCommand) commandType() string    { return "SettleFinalization" }
func (c settleFinalizationCommand) idempotencyKey() string { return c.Input.IdempotencyKey }
func (c settleFinalizationCommand) digest() (string, error) {
	return digestValue(c.Input, "IdempotencyKey")
}

func (s *Store) SettleFinalization(ctx context.Context, input SettleFinalizationInput) (*Finalization, Receipt, error) {
	receipt, err := s.Execute(ctx, settleFinalizationCommand{Input: input})
	if err != nil {
		return nil, receipt, err
	}
	state, err := s.Projection()
	if err != nil {
		return nil, receipt, err
	}
	finalization := state.Finalizations[input.FinalizationID]
	if finalization == nil {
		return nil, receipt, errors.New("committed finalization is absent")
	}
	return cloneFinalization(finalization), receipt, nil
}

func (c settleFinalizationCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	in := c.Input
	if in.State != FinalizationMerged && in.State != FinalizationBlocked {
		return nil, "", errors.New("finalization settlement requires merged or blocked state")
	}
	if strings.TrimSpace(in.Summary) == "" {
		return nil, "", errors.New("finalization settlement requires a summary")
	}
	finalization := state.Finalizations[in.FinalizationID]
	if finalization == nil || finalization.State != FinalizationPrepared {
		return nil, "", errors.New("finalization is not prepared")
	}
	return []DomainEvent{mustEvent(eventFinalizationSettled, finalizationSettledEvent{ID: in.FinalizationID, State: in.State, Summary: in.Summary, PRURL: in.PRURL, CompletedAt: now})}, in.FinalizationID, nil
}

// Finalize prepares an exact publication decision in the journal, performs the
// argv-only GitHub effect, and journals the terminal outcome. A retry after a
// crash resumes the prepared effect by its exact head instead of inventing a
// second publication request.
func (s *Store) Finalize(ctx context.Context, request FinalizationRequest, runner GateRunner) (FinalizationResult, error) {
	if runner == nil {
		return FinalizationResult{}, errors.New("finalizer runner is required")
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return FinalizationResult{}, errors.New("finalizer idempotency key is required")
	}
	state, err := s.Projection()
	if err != nil {
		return FinalizationResult{}, err
	}
	execution := state.Executions[request.ExecutionID]
	if execution == nil {
		return FinalizationResult{}, errors.New("execution does not exist")
	}
	workflow := state.Workflows[execution.WorkflowID]
	if workflow == nil || !workflow.Finalizer.Enabled {
		return FinalizationResult{}, errors.New("finalizer is disabled")
	}
	if workflow.Finalizer.RequireHuman && !request.HumanApproved {
		return FinalizationResult{}, errors.New("finalizer requires explicit human approval")
	}
	finalizationID := stableID("finalization", request.IdempotencyKey)
	finalization := state.Finalizations[finalizationID]
	requestedGates := append([]string(nil), request.Gates...)
	if len(requestedGates) == 0 {
		requestedGates = append(requestedGates, workflow.Finalizer.RequiredChecks...)
	}
	gates, err := exactFinalizerChecks(requestedGates, workflow.Finalizer.RequiredChecks)
	if err != nil {
		return FinalizationResult{}, err
	}
	if finalization != nil && (finalization.ExecutionID != request.ExecutionID || finalization.Repository != request.Repository || finalization.PullRequest != request.PullRequest || finalization.Method != fallbackMethod(request.Method) || finalization.HumanApproved != request.HumanApproved || !sameStrings(finalization.Gates, gates)) {
		return FinalizationResult{}, fmt.Errorf("%w: divergent finalization request", ErrIdempotencyConflict)
	}
	if finalization == nil {
		before, inspectErr := githubgate.Inspect(ctx, runner, request.Repository, request.PullRequest)
		if inspectErr != nil {
			return FinalizationResult{}, inspectErr
		}
		finalization, _, err = s.PrepareFinalization(ctx, PrepareFinalizationInput{ExecutionID: request.ExecutionID, Repository: request.Repository, PullRequest: request.PullRequest, Gates: gates, Method: request.Method, HumanApproved: request.HumanApproved, HeadSHA: before.HeadOID, PRURL: before.URL, IdempotencyKey: request.IdempotencyKey})
		if err != nil {
			return FinalizationResult{}, err
		}
	}
	if finalization.State != FinalizationPrepared {
		return finalizationResult(finalization), terminalFinalizationError(finalization)
	}
	before, inspectErr := githubgate.Inspect(ctx, runner, finalization.Repository, finalization.PullRequest)
	if inspectErr != nil {
		// The prepared decision remains retryable. A failed observation is not
		// evidence that publication was blocked or that an earlier effect did
		// not succeed.
		return finalizationResult(finalization), inspectErr
	}
	if inspectErr == nil && (strings.EqualFold(before.State, "MERGED") || before.MergedAt != "") {
		return s.settleFinalizationResult(ctx, finalization, FinalizationMerged, "GitHub already reports the prepared publication as merged", before.URL, nil)
	}
	if inspectErr == nil && before.HeadOID != finalization.HeadSHA {
		return s.settleFinalizationResult(ctx, finalization, FinalizationBlocked, fmt.Sprintf("pull request head changed from prepared %s to %s", finalization.HeadSHA, before.HeadOID), finalization.PRURL, errors.New("pull request head changed"))
	}
	if err = githubgate.Verify(before, finalization.Gates); err != nil {
		return s.settleFinalizationResult(ctx, finalization, FinalizationBlocked, err.Error(), finalization.PRURL, err)
	}
	if _, err = githubgate.MergeAtHead(ctx, runner, finalization.Repository, finalization.PullRequest, finalization.HeadSHA, finalization.Method); err != nil {
		// The effect may have reached GitHub before its response was lost. Keep
		// the prepared record so a retry can inspect the exact PR and settle the
		// actual merged/blocked outcome.
		return finalizationResult(finalization), err
	}
	return s.settleFinalizationResult(ctx, finalization, FinalizationMerged, "merged after exact named gates passed on unchanged head", finalization.PRURL, nil)
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	copyLeft, copyRight := append([]string(nil), left...), append([]string(nil), right...)
	sort.Strings(copyLeft)
	sort.Strings(copyRight)
	return fmt.Sprint(copyLeft) == fmt.Sprint(copyRight)
}

func exactFinalizerChecks(requested, configured []string) ([]string, error) {
	if len(configured) == 0 {
		return nil, errors.New("finalizer requires configured named checks")
	}
	if len(requested) == 0 {
		return nil, errors.New("finalization requires the configured named checks")
	}
	seen := make(map[string]struct{}, len(requested))
	for _, gate := range requested {
		if strings.TrimSpace(gate) == "" {
			return nil, errors.New("finalization gates must be non-empty")
		}
		if _, ok := seen[gate]; ok {
			return nil, errors.New("finalization gates must not contain duplicates")
		}
		seen[gate] = struct{}{}
	}
	canonical := append([]string(nil), configured...)
	sort.Strings(canonical)
	canonical = uniqueStrings(canonical)
	if !sameStrings(requested, canonical) {
		return nil, fmt.Errorf("finalization gates must exactly match configured checks: want %v", canonical)
	}
	return canonical, nil
}

func (s *Store) settleFinalizationResult(ctx context.Context, finalization *Finalization, status FinalizationState, summary, url string, effectErr error) (FinalizationResult, error) {
	settled, _, err := s.SettleFinalization(ctx, SettleFinalizationInput{FinalizationID: finalization.ID, State: status, Summary: summary, PRURL: url, IdempotencyKey: finalization.IdempotencyKey + "/settle"})
	if err != nil {
		return FinalizationResult{}, errors.Join(effectErr, err)
	}
	return finalizationResult(settled), effectErr
}

func finalizationResult(finalization *Finalization) FinalizationResult {
	return FinalizationResult{FinalizationID: finalization.ID, PRURL: finalization.PRURL, HeadSHA: finalization.HeadSHA, State: finalization.State, Merged: finalization.State == FinalizationMerged, Summary: finalization.Summary}
}

func terminalFinalizationError(finalization *Finalization) error {
	if finalization.State == FinalizationBlocked {
		return errors.New(finalization.Summary)
	}
	return nil
}

func fallbackMethod(method string) string {
	if method == "" {
		return "squash"
	}
	return method
}
