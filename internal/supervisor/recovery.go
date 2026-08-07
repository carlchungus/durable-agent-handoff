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

// ErrLiveOrphan is returned when startup finds an inherited Attempt whose
// exact recorded process is still alive. Supervisor v2 does not yet have a
// safe runtime adoption protocol, so startup fails closed instead of launching
// a duplicate or guessing at ownership.
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
		if attempt.Process != nil && exactProcessLive(attempt.Process) {
			return nil, "", fmt.Errorf("%w: Attempt %s pid=%d", ErrLiveOrphan, attempt.ID, attempt.Process.PID)
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

func exactProcessLive(process *ProcessIdentity) bool {
	if process == nil || process.PID <= 0 || strings.TrimSpace(process.StartToken) == "" {
		return false
	}
	return processidentity.ProcessMatches(process.PID, process.StartToken)
}

func sameProcessIdentity(left, right *ProcessIdentity) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
