package activity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestActivityAttemptOutputAndFencedStopSurviveSnapshotLoss(t *testing.T) {
	root := t.TempDir()
	store, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Create(Descriptor{
		ID:             "activity_0123456789abcdef01234567",
		OwnerSessionID: "agent_0123456789abcdef01234567",
		Work:           WorkSpec{Kind: "command", Cwd: "/tmp/work", Intent: "test tool"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.State != StatePending || created.Generation != 1 || created.Revision != 1 || created.WorkDigest == "" {
		t.Fatalf("created=%+v", created)
	}

	attempt, stdout, stderr, err := store.PrepareAttempt(created.ID, 1, AttemptStart{Runtime: "exec"})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = store.MarkRunning(created.ID, 1, attempt.ID, ProcessIdentity{PID: 4242, ProcessStartToken: "token-a", SupervisorID: "supervisor-a", SupervisorGeneration: 7})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stdout.WriteString("one\ntwo\n"); err == nil {
		err = stdout.Sync()
	}
	if closeErr := stdout.Close(); err == nil {
		err = closeErr
	}
	if closeErr := stderr.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := store.ReadOutput(created.ID, OutputCursor{AttemptID: attempt.ID, Stream: StreamStdout, OutputID: attempt.Stdout.ID, After: 4}, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(chunk.Data) != "two\n" || chunk.Start != 4 || chunk.End != 8 || chunk.OutputID != attempt.Stdout.ID {
		t.Fatalf("chunk=%+v", chunk)
	}

	stale := ControlRequest{ExpectedGeneration: 0, ExpectedAttempt: identity(attempt)}
	intent, _, err := store.RequestStop(created.ID, stale)
	if !errors.Is(err, ErrFenced) || intent.Outcome != ControlRejected {
		t.Fatalf("stale intent=%+v err=%v", intent, err)
	}
	current, err := store.Load(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	intent, stopping, err := store.RequestStop(created.ID, ControlRequest{ExpectedGeneration: current.Generation, ExpectedAttempt: identity(attempt)})
	if err != nil || intent.Outcome != ControlAccepted || stopping.State != StateStopping {
		t.Fatalf("intent=%+v activity=%+v err=%v", intent, stopping, err)
	}
	if err = store.FinishAttempt(created.ID, stopping.Generation, identity(attempt), ExitResult{State: StateStopped}); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(root, "activities", created.ID, "state.json")); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Load(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.State != StateStopped || replayed.Revision != 6 || !replayed.Attempts[0].Stdout.Closed || replayed.Work.Intent != "test tool" {
		t.Fatalf("replayed=%+v", replayed)
	}
}

func TestActivityWorkIsImmutableAndOutputIdentityIsExact(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	created, _ := store.Create(Descriptor{ID: "activity_aaaaaaaaaaaaaaaaaaaaaaaa", Work: WorkSpec{Kind: "command", Cwd: "/tmp", Intent: "first"}})
	attempt, stdout, stderr, err := store.PrepareAttempt(created.ID, created.Generation, AttemptStart{})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err = store.MarkRunning(created.ID, created.Generation, attempt.ID, ProcessIdentity{PID: 7, ProcessStartToken: "token", SupervisorID: "supervisor-a", SupervisorGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	_ = stdout.Close()
	_ = stderr.Close()
	if _, err = store.ReadOutput(created.ID, OutputCursor{AttemptID: attempt.ID, Stream: StreamStdout, OutputID: "output_wrong"}, 10); err == nil {
		t.Fatal("accepted the wrong output identity")
	}
	loaded, _ := store.Load(created.ID)
	loaded.Work.Intent = "mutated"
	again, _ := store.Load(created.ID)
	if again.Work.Intent != "first" {
		t.Fatalf("caller mutated durable work definition: %+v", again.Work)
	}
}

func TestEnsureReusesOnlyTheExactImmutableWork(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	descriptor := Descriptor{ID: StableID("wf", "node", "1"), OwnerSessionID: "agent_owner", Work: WorkSpec{Kind: "agent", Cwd: "/tmp", Intent: "wf/node"}}
	first, err := store.Ensure(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.Ensure(descriptor)
	if err != nil || again.ID != first.ID || again.WorkDigest != first.WorkDigest {
		t.Fatalf("again=%+v err=%v", again, err)
	}
	descriptor.Work.Intent = "other"
	if _, err = store.Ensure(descriptor); err == nil {
		t.Fatal("ensure accepted a changed immutable work definition")
	}
}

func TestResolveLostIsExactFencedAndReplayable(t *testing.T) {
	root := t.TempDir()
	store, _ := OpenStore(root)
	created, _ := store.Create(Descriptor{ID: "activity_bbbbbbbbbbbbbbbbbbbbbbbb", Work: WorkSpec{Kind: "agent", Cwd: "/tmp", Intent: "resolve output"}})
	attempt, stdout, stderr, _ := store.PrepareAttempt(created.ID, created.Generation, AttemptStart{CommandDigest: "digest"})
	_ = stdout.Close()
	_ = stderr.Close()
	attempt, _ = store.MarkRunning(created.ID, created.Generation, attempt.ID, ProcessIdentity{PID: 9001, ProcessStartToken: "birth", SupervisorID: "owner", SupervisorGeneration: 1})
	identity := identity(attempt)
	if err := store.FinishAttempt(created.ID, created.Generation, identity, ExitResult{State: StateLost, Error: "supervisor died"}); err != nil {
		t.Fatal(err)
	}
	stale := identity
	stale.SupervisorGeneration++
	if err := store.ResolveLost(created.ID, created.Generation, stale, ExitResult{State: StateCompleted}); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale resolution err=%v", err)
	}
	code := 0
	if err := store.ResolveLost(created.ID, created.Generation, identity, ExitResult{State: StateCompleted, ExitCode: &code}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "activities", created.ID, "state.json")); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Load(created.ID)
	if err != nil || replayed.State != StateCompleted || replayed.Attempts[0].State != StateCompleted {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
}

func TestNaturalCompletionRacingStopDoesNotClaimControlApplied(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	created, _ := store.Create(Descriptor{ID: "activity_ffffffffffffffffffffffff", Work: WorkSpec{Kind: "command", Cwd: "/tmp", Intent: "stop race"}})
	attempt, stdout, stderr, _ := store.PrepareAttempt(created.ID, created.Generation, AttemptStart{})
	_ = stdout.Close()
	_ = stderr.Close()
	attempt, _ = store.MarkRunning(created.ID, created.Generation, attempt.ID, ProcessIdentity{PID: 42, ProcessStartToken: "birth", SupervisorID: "owner", SupervisorGeneration: 1})
	_, stopping, err := store.RequestStop(created.ID, ControlRequest{ExpectedGeneration: created.Generation, ExpectedAttempt: identity(attempt)})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.FinishAttempt(created.ID, stopping.Generation, identity(attempt), ExitResult{State: StateCompleted}); err != nil {
		t.Fatal(err)
	}
	finished, _ := store.Load(created.ID)
	if finished.State != StateCompleted || finished.Controls[0].Outcome != ControlRejected || finished.Controls[0].Reason == "" {
		t.Fatalf("finished=%+v", finished)
	}
}

func TestPrepareAttemptRecoversOrphanBlobsBeforePreparedEvent(t *testing.T) {
	root := t.TempDir()
	store, _ := OpenStore(root)
	created, _ := store.Create(Descriptor{ID: "activity_111111111111111111111111", Work: WorkSpec{Kind: "agent", Cwd: "/tmp", Intent: "orphan recovery"}})
	recordDir := filepath.Join(root, "activities", created.ID)
	if err := os.WriteFile(filepath.Join(recordDir, "attempt_1_stdout.log"), []byte("orphan stdout"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(recordDir, "attempt_1_stderr.log"), []byte("orphan stderr"), 0o600); err != nil {
		t.Fatal(err)
	}
	attempt, stdout, stderr, err := store.PrepareAttempt(created.ID, created.Generation, AttemptStart{})
	if err != nil {
		t.Fatal(err)
	}
	_ = stdout.Close()
	_ = stderr.Close()
	if attempt.ID != "attempt_1" {
		t.Fatalf("attempt=%+v", attempt)
	}
	for _, path := range []string{attempt.Stdout.Path, attempt.Stderr.Path} {
		info, statErr := os.Stat(path)
		if statErr != nil || info.Size() != 0 {
			t.Fatalf("orphan blob was not replaced: path=%s info=%+v err=%v", path, info, statErr)
		}
	}
}

func TestAssessmentPersistsProgressAndStallClassificationAcrossReplay(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	store, _ := OpenStore(state)
	created, err := store.Create(Descriptor{ID: "activity_abcdefabcdefabcdefabcdef", Work: WorkSpec{Kind: "agent", Cwd: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, stdout, stderr, err := store.PrepareAttempt(created.ID, created.Generation, AttemptStart{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stdout.WriteString("meaningful output"); err != nil {
		t.Fatal(err)
	}
	_ = stdout.Close()
	_ = stderr.Close()
	if _, err = store.MarkRunning(created.ID, created.Generation, attempt.ID, ProcessIdentity{PID: 42, ProcessStartToken: "birth", SupervisorID: "sup", SupervisorGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	first := attempt.StartedAt.Add(time.Minute)
	assessed, err := store.Assess(created.ID, first, DefaultQuietAfter, DefaultStalledAfter)
	if err != nil || assessed.ProgressState != ProgressActive {
		t.Fatalf("first assessment=%+v err=%v", assessed, err)
	}
	assessed, err = store.Assess(created.ID, first.Add(DefaultStalledAfter+time.Minute), DefaultQuietAfter, DefaultStalledAfter)
	if err != nil || assessed.ProgressState != ProgressStalled {
		t.Fatalf("stalled assessment=%+v err=%v", assessed, err)
	}
	if err = os.Remove(filepath.Join(state, "activities", created.ID, "state.json")); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Load(created.ID)
	if err != nil || replayed.ProgressState != ProgressStalled || replayed.LastProgressAt.IsZero() {
		t.Fatalf("replayed assessment=%+v err=%v", replayed, err)
	}
}

func TestStartupStallTracksRuntimeEventAndReplay(t *testing.T) {
	root := t.TempDir()
	store, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Create(Descriptor{ID: "activity_121212121212121212121212", Work: WorkSpec{Kind: "agent", Cwd: "/tmp"}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, stdout, stderr, err := store.PrepareAttempt(created.ID, created.Generation, AttemptStart{})
	if err != nil {
		t.Fatal(err)
	}
	_ = stdout.Close()
	_ = stderr.Close()
	attempt, err = store.MarkRunning(created.ID, created.Generation, attempt.ID, ProcessIdentity{PID: 77, ProcessStartToken: "startup", SupervisorID: "sup", SupervisorGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	threadStarted := attempt.StartedAt.Add(time.Second)
	if err = store.ObserveRuntime(created.ID, created.Generation, attempt.ID, "thread.started", 42, threadStarted); err != nil {
		t.Fatal(err)
	}
	assessed, err := store.Assess(created.ID, attempt.StartedAt.Add(DefaultStartupGrace+time.Second), DefaultQuietAfter, DefaultStalledAfter)
	if err != nil || assessed.ProgressState != ProgressStalledStartup || assessed.LastRuntimeEvent != "thread.started" || assessed.LastOutputBytes != 42 {
		t.Fatalf("startup assessment=%+v err=%v", assessed, err)
	}
	turnStarted := threadStarted.Add(time.Second)
	if err = store.ObserveRuntime(created.ID, created.Generation, attempt.ID, "turn.started", 84, turnStarted); err != nil {
		t.Fatal(err)
	}
	quiet, err := store.Assess(created.ID, turnStarted.Add(DefaultStalledAfter+time.Second), DefaultQuietAfter, DefaultStalledAfter)
	if err != nil || quiet.ProgressState != ProgressStalled || quiet.ProgressState == ProgressStalledStartup {
		t.Fatalf("post-turn quiet work was misclassified or auto-startup-stopped: %+v err=%v", quiet, err)
	}
	replayed, err := store.Load(created.ID)
	if err != nil || replayed.TurnStartedAt.IsZero() || replayed.LastRuntimeEvent != "turn.started" || replayed.LastProgressAt != turnStarted {
		t.Fatalf("runtime replay=%+v err=%v", replayed, err)
	}
}

func identity(attempt Attempt) AttemptIdentity {
	return AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, ProcessTreeID: attempt.ProcessTreeID, SupervisorID: attempt.SupervisorID, SupervisorGeneration: attempt.SupervisorGeneration}
}
