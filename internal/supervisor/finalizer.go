package supervisor

import (
	"context"
	"errors"
	"fmt"

	"github.com/carlchungus/durable-agent-handoff/internal/githubgate"
)

// FinalizationRequest is the authority-owned publication effect. Runtime
// Drivers never receive this capability; callers provide the exact repository,
// PR, named gates, and human approval required by the Workflow policy.
type FinalizationRequest struct {
	ExecutionID   ExecutionID
	Repository    string
	PullRequest   string
	Gates         []string
	Method        string
	HumanApproved bool
}

type FinalizationResult struct {
	PRURL   string
	HeadSHA string
	Merged  bool
	Summary string
}

type GateRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

// Finalize evaluates the pure Supervisor publication projection and then
// delegates the unchanged-head, exact-gate merge to the argv-only GitHub gate.
// It is intentionally outside Driver and Executor authority.
func (s *Store) Finalize(ctx context.Context, request FinalizationRequest, runner GateRunner) (FinalizationResult, error) {
	if runner == nil {
		return FinalizationResult{}, errors.New("finalizer runner is required")
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
	view, err := ProjectExecution(state, request.ExecutionID, workflow.CreatedAt)
	if err != nil {
		return FinalizationResult{}, err
	}
	if view.Publication != PublicationEligible && !(view.Publication == PublicationAwaitingHuman && request.HumanApproved) {
		return FinalizationResult{}, fmt.Errorf("publication is not eligible: %s", view.Publication)
	}
	gates := append([]string(nil), request.Gates...)
	if len(gates) == 0 {
		gates = append(gates, workflow.Finalizer.RequiredChecks...)
	}
	before, err := githubgate.Inspect(ctx, runner, request.Repository, request.PullRequest)
	if err != nil {
		return FinalizationResult{}, err
	}
	if err = githubgate.Verify(before, gates); err != nil {
		return FinalizationResult{PRURL: before.URL, HeadSHA: before.HeadOID, Summary: err.Error()}, err
	}
	merged, err := githubgate.Merge(ctx, runner, request.Repository, request.PullRequest, gates, request.Method)
	if err != nil {
		return FinalizationResult{}, err
	}
	return FinalizationResult{PRURL: merged.URL, HeadSHA: merged.HeadOID, Merged: true, Summary: "merged after exact named gates passed on unchanged head"}, nil
}
