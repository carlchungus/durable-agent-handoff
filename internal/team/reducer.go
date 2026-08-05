package team

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var identifier = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func Apply(t *Team, c Command, at time.Time) error {
	if t == nil || strings.TrimSpace(c.Actor) == "" {
		return errors.New("team and actor are required")
	}
	if c.Actor != "supervisor" && t.Members[c.Actor] == nil {
		return fmt.Errorf("actor %q is not a team member", c.Actor)
	}
	switch c.Op {
	case "add_member":
		if c.Actor != t.LeadID {
			return errors.New("only the lead may add a member")
		}
		if c.Member == nil || !identifier.MatchString(c.Member.ID) || strings.TrimSpace(c.Member.Name) == "" {
			return errors.New("member requires a valid id and name")
		}
		if t.Members[c.Member.ID] != nil {
			return fmt.Errorf("member %q already exists", c.Member.ID)
		}
		m := *c.Member
		if m.State == "" {
			m.State = MemberStarting
		}
		if m.Process == "" {
			m.Process = ProcessUnknown
		}
		if m.Plan == "" {
			m.Plan = PlanNotRequired
		}
		m.CreatedAt, m.UpdatedAt = at, at
		t.Members[m.ID] = &m
	case "set_member_state":
		m, err := member(t, c.MemberID)
		if err != nil {
			return err
		}
		if c.Actor != c.MemberID && c.Actor != t.LeadID {
			return errors.New("only the member or lead may change member state")
		}
		if !validMemberTransition(m.State, c.State) {
			return fmt.Errorf("invalid member state transition %s -> %s", m.State, c.State)
		}
		m.State, m.NeedsInputReason, m.UpdatedAt = c.State, c.Reason, at
		if c.State != MemberNeedsInput {
			m.NeedsInputReason = ""
		}
		if c.State == MemberIdle {
			appendMessage(t, Message{Kind: MessageIdle, From: c.MemberID, To: t.LeadID, Body: c.Reason}, at)
		}
	case "set_process":
		m, err := member(t, c.MemberID)
		if err != nil {
			return err
		}
		if c.Actor != c.MemberID && c.Actor != t.LeadID && c.Actor != "supervisor" {
			return errors.New("process state requires member, lead, or supervisor authority")
		}
		if c.Process != ProcessUnknown && c.Process != ProcessLive && c.Process != ProcessExited {
			return fmt.Errorf("invalid process state %q", c.Process)
		}
		m.Process, m.SessionID, m.UpdatedAt = c.Process, c.SessionID, at
	case "add_task":
		if c.Task == nil || !identifier.MatchString(c.Task.ID) || strings.TrimSpace(c.Task.Title) == "" {
			return errors.New("task requires a valid id and title")
		}
		if t.Tasks[c.Task.ID] != nil {
			return fmt.Errorf("task %q already exists", c.Task.ID)
		}
		for _, dep := range c.Task.BlockedBy {
			if t.Tasks[dep] == nil {
				return fmt.Errorf("blocking task %q does not exist", dep)
			}
		}
		task := *c.Task
		task.State, task.Claim, task.CreatedAt, task.UpdatedAt = TaskPending, nil, at, at
		t.Tasks[task.ID] = &task
		t.TaskOrder = append(t.TaskOrder, task.ID)
		if hasTaskCycle(t, task.ID, map[string]bool{}) {
			delete(t.Tasks, task.ID)
			t.TaskOrder = t.TaskOrder[:len(t.TaskOrder)-1]
			return errors.New("task dependency cycle")
		}
	case "claim_task":
		m, err := member(t, c.Actor)
		if err != nil {
			return err
		}
		if m.State == MemberStopped || m.State == MemberExited || m.ShutdownRequested {
			return errors.New("inactive member cannot claim work")
		}
		if m.Plan == PlanAwaiting || m.Plan == PlanRejected {
			return errors.New("member plan is not approved")
		}
		task, err := claimableTask(t, c.TaskID, at)
		if err != nil {
			return err
		}
		generation := uint64(1)
		if task.Claim != nil {
			generation = task.Claim.Generation + 1
		}
		lease := c.Lease
		if lease <= 0 {
			lease = 5 * time.Minute
		}
		task.Claim = &Claim{MemberID: c.Actor, Generation: generation, ClaimedAt: at, ExpiresAt: at.Add(lease)}
		task.State, task.UpdatedAt = TaskInProgress, at
	case "renew_claim":
		task, err := ownedTask(t, c.TaskID, c.Actor, c.ClaimGeneration, at)
		if err != nil {
			return err
		}
		lease := c.Lease
		if lease <= 0 {
			lease = 5 * time.Minute
		}
		task.Claim.ExpiresAt, task.UpdatedAt = at.Add(lease), at
	case "complete_task", "fail_task":
		task, err := ownedTask(t, c.TaskID, c.Actor, c.ClaimGeneration, at)
		if err != nil {
			return err
		}
		if c.Op == "complete_task" {
			task.State = TaskCompleted
		} else {
			task.State = TaskFailed
		}
		task.Result, task.UpdatedAt = c.Result, at
	case "send_message":
		if strings.TrimSpace(c.Body) == "" {
			return errors.New("message body is required")
		}
		kind := MessageBroadcast
		if c.To != "" {
			if t.Members[c.To] == nil {
				return fmt.Errorf("recipient %q is not a team member", c.To)
			}
			kind = MessageDirect
		}
		appendMessage(t, Message{Kind: kind, From: c.Actor, To: c.To, Body: c.Body, ReplyTo: c.ReplyTo}, at)
	case "submit_plan":
		m, err := member(t, c.Actor)
		if err != nil {
			return err
		}
		if strings.TrimSpace(c.Body) == "" {
			return errors.New("plan body is required")
		}
		m.Plan, m.PlanText, m.PlanReview, m.UpdatedAt = PlanAwaiting, c.Body, "", at
		appendMessage(t, Message{Kind: MessagePlanSubmitted, From: c.Actor, To: t.LeadID, Body: c.Body}, at)
	case "review_plan":
		if c.Actor != t.LeadID || c.Approved == nil {
			return errors.New("lead approval decision is required")
		}
		m, err := member(t, c.MemberID)
		if err != nil {
			return err
		}
		if m.Plan != PlanAwaiting {
			return errors.New("member has no plan awaiting approval")
		}
		m.Plan = PlanRejected
		if *c.Approved {
			m.Plan = PlanApproved
		}
		m.PlanReview, m.UpdatedAt = c.Reason, at
		appendMessage(t, Message{Kind: MessagePlanReviewed, From: c.Actor, To: c.MemberID, Body: c.Reason}, at)
	case "request_shutdown":
		if c.Actor != t.LeadID {
			return errors.New("only the lead may request shutdown")
		}
		m, err := member(t, c.MemberID)
		if err != nil {
			return err
		}
		m.ShutdownRequested, m.UpdatedAt = true, at
		appendMessage(t, Message{Kind: MessageShutdownRequest, From: c.Actor, To: c.MemberID, Body: c.Reason}, at)
	case "respond_shutdown":
		if c.Approved == nil || c.Actor == t.LeadID {
			return errors.New("member shutdown response is required")
		}
		m, err := member(t, c.Actor)
		if err != nil {
			return err
		}
		if !m.ShutdownRequested {
			return errors.New("no shutdown request is pending")
		}
		if *c.Approved {
			m.State = MemberStopped
		}
		m.ShutdownRequested, m.UpdatedAt = false, at
		appendMessage(t, Message{Kind: MessageShutdownResponse, From: c.Actor, To: t.LeadID, Body: c.Reason}, at)
	default:
		return fmt.Errorf("unknown team command %q", c.Op)
	}
	t.UpdatedAt = at
	return nil
}

func member(t *Team, id string) (*Member, error) {
	m := t.Members[id]
	if m == nil {
		return nil, fmt.Errorf("member %q does not exist", id)
	}
	return m, nil
}

func claimableTask(t *Team, id string, now time.Time) (*Task, error) {
	task := t.Tasks[id]
	if task == nil {
		return nil, fmt.Errorf("task %q does not exist", id)
	}
	for _, dep := range task.BlockedBy {
		if t.Tasks[dep] == nil || t.Tasks[dep].State != TaskCompleted {
			return nil, fmt.Errorf("task %q is blocked by %q", id, dep)
		}
	}
	if task.State == TaskCompleted || task.State == TaskFailed {
		return nil, fmt.Errorf("task %q is terminal", id)
	}
	if task.Claim != nil && task.Claim.ExpiresAt.After(now) {
		return nil, fmt.Errorf("task %q is already claimed", id)
	}
	return task, nil
}

func ownedTask(t *Team, id, actor string, generation uint64, now time.Time) (*Task, error) {
	task := t.Tasks[id]
	if task == nil || task.Claim == nil || task.Claim.MemberID != actor || task.Claim.Generation != generation {
		return nil, errors.New("task claim ownership or generation does not match")
	}
	if task.State != TaskInProgress {
		return nil, errors.New("task is not in progress")
	}
	if !task.Claim.ExpiresAt.IsZero() && !task.Claim.ExpiresAt.After(now) {
		return nil, errors.New("task claim lease expired")
	}
	return task, nil
}

func appendMessage(t *Team, m Message, at time.Time) {
	m.Sequence, m.ID, m.CreatedAt = uint64(len(t.Messages)+1), fmt.Sprintf("msg_%016x", len(t.Messages)+1), at
	t.Messages = append(t.Messages, m)
}

func validMemberTransition(from, to MemberState) bool {
	allowed := map[MemberState]map[MemberState]bool{
		MemberStarting:   {MemberWorking: true, MemberNeedsInput: true, MemberStopped: true, MemberExited: true},
		MemberWorking:    {MemberIdle: true, MemberNeedsInput: true, MemberStopped: true, MemberExited: true},
		MemberIdle:       {MemberWorking: true, MemberNeedsInput: true, MemberStopped: true, MemberExited: true},
		MemberNeedsInput: {MemberWorking: true, MemberIdle: true, MemberStopped: true, MemberExited: true},
		MemberStopped:    {MemberStarting: true, MemberExited: true},
		MemberExited:     {MemberStarting: true},
	}
	return from == to || allowed[from][to]
}

func hasTaskCycle(t *Team, id string, seen map[string]bool) bool {
	if seen[id] {
		return true
	}
	seen[id] = true
	for _, dep := range t.Tasks[id].BlockedBy {
		if hasTaskCycle(t, dep, seen) {
			return true
		}
	}
	delete(seen, id)
	return false
}
