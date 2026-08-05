package team

import (
	"testing"
	"time"
)

func testTeam(t *testing.T) (*Team, time.Time) {
	t.Helper()
	at := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	team := &Team{
		ID: "team_test", Name: "test", LeadID: "lead",
		Members: map[string]*Member{"lead": {ID: "lead", Name: "Lead", State: MemberWorking, Process: ProcessLive, Plan: PlanNotRequired}},
		Tasks:   map[string]*Task{}, CreatedAt: at, UpdatedAt: at,
	}
	return team, at
}

func TestPlanApprovalBlocksClaimsAndTaskDependencies(t *testing.T) {
	tm, at := testTeam(t)
	if err := Apply(tm, Command{Op: "add_member", Actor: "lead", Member: &Member{ID: "worker", Name: "Worker", Plan: PlanAwaiting}}, at); err != nil {
		t.Fatal(err)
	}
	if err := Apply(tm, Command{Op: "add_task", Actor: "lead", Task: &Task{ID: "discover", Title: "Discover"}}, at); err != nil {
		t.Fatal(err)
	}
	if err := Apply(tm, Command{Op: "add_task", Actor: "lead", Task: &Task{ID: "build", Title: "Build", BlockedBy: []string{"discover"}}}, at); err != nil {
		t.Fatal(err)
	}
	if err := Apply(tm, Command{Op: "claim_task", Actor: "worker", TaskID: "discover"}, at); err == nil {
		t.Fatal("claim succeeded before plan approval")
	}
	approved := true
	if err := Apply(tm, Command{Op: "review_plan", Actor: "lead", MemberID: "worker", Approved: &approved, Reason: "bounded"}, at); err != nil {
		t.Fatal(err)
	}
	if err := Apply(tm, Command{Op: "claim_task", Actor: "worker", TaskID: "build"}, at); err == nil {
		t.Fatal("blocked task was claimed")
	}
	if err := Apply(tm, Command{Op: "claim_task", Actor: "worker", TaskID: "discover", Lease: time.Minute}, at); err != nil {
		t.Fatal(err)
	}
	if err := Apply(tm, Command{Op: "complete_task", Actor: "worker", TaskID: "discover", ClaimGeneration: 1, Result: "done"}, at); err != nil {
		t.Fatal(err)
	}
	if err := Apply(tm, Command{Op: "claim_task", Actor: "worker", TaskID: "build"}, at); err != nil {
		t.Fatal(err)
	}
}

func TestClaimFencingRejectsStaleWorker(t *testing.T) {
	tm, at := testTeam(t)
	_ = Apply(tm, Command{Op: "add_member", Actor: "lead", Member: &Member{ID: "first", Name: "First"}}, at)
	_ = Apply(tm, Command{Op: "add_member", Actor: "lead", Member: &Member{ID: "second", Name: "Second"}}, at)
	_ = Apply(tm, Command{Op: "add_task", Actor: "lead", Task: &Task{ID: "work", Title: "Work"}}, at)
	if err := Apply(tm, Command{Op: "claim_task", Actor: "first", TaskID: "work", Lease: time.Second}, at); err != nil {
		t.Fatal(err)
	}
	if err := Apply(tm, Command{Op: "claim_task", Actor: "second", TaskID: "work"}, at.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := tm.Tasks["work"].Claim.Generation; got != 2 {
		t.Fatalf("generation = %d", got)
	}
	if err := Apply(tm, Command{Op: "complete_task", Actor: "first", TaskID: "work", ClaimGeneration: 1}, at.Add(3*time.Second)); err == nil {
		t.Fatal("stale worker completed a reclaimed task")
	}
}

func TestExpiredClaimCannotCommitBeforeReclaim(t *testing.T) {
	tm, at := testTeam(t)
	_ = Apply(tm, Command{Op: "add_member", Actor: "lead", Member: &Member{ID: "worker", Name: "Worker"}}, at)
	_ = Apply(tm, Command{Op: "add_task", Actor: "lead", Task: &Task{ID: "work", Title: "Work"}}, at)
	_ = Apply(tm, Command{Op: "claim_task", Actor: "worker", TaskID: "work", Lease: time.Second}, at)
	if err := Apply(tm, Command{Op: "complete_task", Actor: "worker", TaskID: "work", ClaimGeneration: 1}, at.Add(time.Second)); err == nil {
		t.Fatal("expired lease committed work")
	}
}

func TestLogicalAndProcessStateAreIndependent(t *testing.T) {
	tm, at := testTeam(t)
	_ = Apply(tm, Command{Op: "set_member_state", Actor: "lead", MemberID: "lead", State: MemberNeedsInput, Reason: "question"}, at)
	if err := Apply(tm, Command{Op: "set_process", Actor: "supervisor", MemberID: "lead", Process: ProcessExited, SessionID: "session-1"}, at); err != nil {
		t.Fatal(err)
	}
	if tm.Members["lead"].State != MemberNeedsInput || tm.Members["lead"].Process != ProcessExited {
		t.Fatalf("states collapsed: %+v", tm.Members["lead"])
	}
}

func TestCooperativeShutdownAndMailbox(t *testing.T) {
	tm, at := testTeam(t)
	_ = Apply(tm, Command{Op: "add_member", Actor: "lead", Member: &Member{ID: "worker", Name: "Worker"}}, at)
	if err := Apply(tm, Command{Op: "request_shutdown", Actor: "worker", MemberID: "lead"}, at); err == nil {
		t.Fatal("non-lead requested shutdown")
	}
	if err := Apply(tm, Command{Op: "request_shutdown", Actor: "lead", MemberID: "worker", Reason: "done"}, at); err != nil {
		t.Fatal(err)
	}
	approved := true
	if err := Apply(tm, Command{Op: "respond_shutdown", Actor: "worker", Approved: &approved, Reason: "stopping"}, at); err != nil {
		t.Fatal(err)
	}
	if tm.Members["worker"].State != MemberStopped || len(tm.Messages) != 2 {
		t.Fatalf("shutdown not durable: %+v", tm)
	}
}
