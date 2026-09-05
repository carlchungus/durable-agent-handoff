package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/processidentity"
)

// ErrLiveOrphan is retained for callers that explicitly ask an older recovery
// path to fail closed. Normal startup now returns exact live Attempts so the
// service can observe them without launching a duplicate.
var ErrLiveOrphan = errors.New("live orphan Attempt requires explicit adoption")

type startupRecovery struct {
	AttemptID AttemptID        `json:"attempt_id"`
	LeaseID   LeaseID          `json:"lease_id"`
	Process   *ProcessIdentity `json:"process,omitempty"`
}

type reconcileStartupCommand struct {
	Recoveries     []startupRecovery
	IdempotencyKey string
}

func (c reconcileStartupCommand) commandType() string    { return "ReconcileStartup" }
func (c reconcileStartupCommand) idempotencyKey() string { return c.IdempotencyKey }
func (c reconcileStartupCommand) digest() (string, error) {
	return digestValue(struct {
		Recoveries []startupRecovery `json:"recoveries"`
	}{Recoveries: c.Recoveries}, "IdempotencyKey")
}

// ReconcileStartup is the authority-owned restart boundary. It checks
// inherited Attempts once before scheduling, and appends only typed lifecycle
// facts to the canonical journal. Reads and projections remain pure.
func (s *Store) ReconcileStartup(ctx context.Context) error {
	state, err := s.Projection()
	if err != nil {
		return err
	}
	recoveries := make([]startupRecovery, 0)
	ids := make([]string, 0)
	for _, attempt := range state.Attempts {
		if attemptHasExit(attempt) {
			continue
		}
		recovery := startupRecovery{AttemptID: attempt.ID, LeaseID: attempt.LeaseID}
		if attempt.Process != nil {
			process := *attempt.Process
			recovery.Process = &process
			match, inspectErr := exactProcessMatch(attempt.Process)
			if inspectErr != nil {
				return fmt.Errorf("inspect orphaned Attempt %s without releasing its lease: %w", attempt.ID, inspectErr)
			}
			if match == processidentity.MatchUnknown {
				return fmt.Errorf("inspect orphaned Attempt %s without releasing its lease: identity status is unknown", attempt.ID)
			}
			if match == processidentity.MatchExact {
				continue
			}
		}
		recoveries = append(recoveries, recovery)
		ids = append(ids, string(attempt.ID))
	}
	if len(recoveries) == 0 {
		return nil
	}
	sort.Slice(recoveries, func(i, j int) bool { return recoveries[i].AttemptID < recoveries[j].AttemptID })
	sort.Strings(ids)
	key := "startup/reconcile/" + stableID("attempts", strings.Join(ids, "\x00"))
	_, err = s.Execute(ctx, reconcileStartupCommand{Recoveries: recoveries, IdempotencyKey: key})
	return err
}

// LiveAttemptIDs returns exact live process identities that survived a
// Supervisor restart. It is a pure read used by the service to start an
// observer; it never adopts by PID alone and never mutates the journal.
func (s *Store) LiveAttemptIDs() ([]AttemptID, error) {
	state, err := s.Projection()
	if err != nil {
		return nil, err
	}
	ids := make([]AttemptID, 0)
	for _, attempt := range state.Attempts {
		if attemptHasExit(attempt) || attempt.Process == nil {
			continue
		}
		match, inspectErr := exactProcessMatch(attempt.Process)
		if inspectErr != nil {
			return nil, inspectErr
		}
		if match == processidentity.MatchUnknown {
			return nil, fmt.Errorf("inspect live Attempt %s: identity status is unknown", attempt.ID)
		}
		if match == processidentity.MatchExact {
			ids = append(ids, attempt.ID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func (c reconcileStartupCommand) decide(state *State, now time.Time) ([]DomainEvent, string, error) {
	if len(c.Recoveries) == 0 {
		return nil, "", errors.New("startup reconciliation requires inherited Attempts")
	}
	events := make([]DomainEvent, 0, len(c.Recoveries)*3)
	for _, recovery := range c.Recoveries {
		attempt := state.Attempts[recovery.AttemptID]
		lease := state.Leases[recovery.LeaseID]
		activity := (*Activity)(nil)
		if attempt != nil {
			activity = state.Activities[attempt.ActivityID]
		}
		if attempt == nil || lease == nil || activity == nil || attempt.LeaseID != lease.ID || lease.AttemptID != attempt.ID || lease.ReleasedAt.IsZero() == false || attempt.ActivityGeneration != activity.Generation || attemptHasExit(attempt) {
			return nil, "", ErrFenced
		}
		if !sameProcessIdentity(attempt.Process, recovery.Process) {
			return nil, "", ErrFenced
		}
		if attempt.Process != nil {
			match, inspectErr := exactProcessMatch(attempt.Process)
			if inspectErr != nil {
				return nil, "", fmt.Errorf("inspect orphaned Attempt %s without releasing its lease: %w", attempt.ID, inspectErr)
			}
			if match == processidentity.MatchUnknown {
				return nil, "", fmt.Errorf("inspect orphaned Attempt %s without releasing its lease: identity status is unknown", attempt.ID)
			}
			if match == processidentity.MatchExact {
				return nil, "", fmt.Errorf("%w: Attempt %s pid=%d", ErrLiveOrphan, attempt.ID, attempt.Process.PID)
			}
		}

		if attempt.Process == nil && len(attempt.Milestones) == 0 {
			failed := Milestone{Kind: MilestoneAdapterStartFailed, At: now, Failure: "prepared Attempt was never spawned before Supervisor restart", SourceType: "supervisor.recovery"}
			if err := validateMilestone(state, attempt, activity, failed); err != nil {
				return nil, "", err
			}
			events = append(events, mustEvent(eventMilestone, milestoneEvent{AttemptID: attempt.ID, Milestone: failed}))
			attempt = cloneAttempt(attempt)
			attempt.Milestones = append(attempt.Milestones, failed)
		}
		exit := Milestone{Kind: MilestoneExit, At: now, Exit: &Exit{Code: 255, Error: "orphaned Attempt was terminalized during Supervisor restart"}, SourceType: "supervisor.recovery"}
		if err := validateMilestone(state, attempt, activity, exit); err != nil {
			return nil, "", err
		}
		events = append(events, mustEvent(eventMilestone, milestoneEvent{AttemptID: recovery.AttemptID, Milestone: exit}))
		events = append(events, settleMessages(state, activity.ID, attempt.ID, false, now)...)
		events = append(events,
			mustEvent(eventLeaseReleased, leaseReleasedEvent{LeaseID: recovery.LeaseID, At: now}),
		)
	}
	return events, "startup-reconcile", nil
}

func exactProcessMatch(process *ProcessIdentity) (processidentity.MatchStatus, error) {
	if process == nil || process.PID <= 0 || strings.TrimSpace(process.StartToken) == "" {
		return processidentity.MatchUnknown, errors.New("durable process identity is incomplete")
	}
	return processidentity.InspectMatch(process.PID, process.StartToken)
}

func sameProcessIdentity(left, right *ProcessIdentity) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
