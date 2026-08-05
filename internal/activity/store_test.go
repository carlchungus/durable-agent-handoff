package activity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
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
		Launch:         LaunchSpec{Kind: "command", Argv: []string{"tool", "--flag"}, Cwd: "/tmp/work"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.State != StatePending || created.Generation != 1 || created.Revision != 1 || created.LaunchDigest == "" {
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
	if replayed.State != StateStopped || replayed.Revision != 6 || !replayed.Attempts[0].Stdout.Closed || replayed.Launch.Argv[0] != "tool" {
		t.Fatalf("replayed=%+v", replayed)
	}
}

func TestActivityLaunchIsImmutableAndOutputIdentityIsExact(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	created, _ := store.Create(Descriptor{ID: "activity_aaaaaaaaaaaaaaaaaaaaaaaa", Launch: LaunchSpec{Kind: "command", Argv: []string{"first"}, Cwd: "/tmp"}})
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
	loaded.Launch.Argv[0] = "mutated"
	again, _ := store.Load(created.ID)
	if again.Launch.Argv[0] != "first" {
		t.Fatalf("caller mutated durable launch: %+v", again.Launch)
	}
}

func TestEnsureReusesOnlyTheExactImmutableLaunch(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	descriptor := Descriptor{ID: StableID("wf", "node", "1"), OwnerSessionID: "agent_owner", Launch: LaunchSpec{Kind: "agent", Argv: []string{"codex", "exec"}, Cwd: "/tmp", Runtime: "codex", Model: "sol"}}
	first, err := store.Ensure(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.Ensure(descriptor)
	if err != nil || again.ID != first.ID || again.LaunchDigest != first.LaunchDigest {
		t.Fatalf("again=%+v err=%v", again, err)
	}
	descriptor.Launch.Model = "other"
	if _, err = store.Ensure(descriptor); err == nil {
		t.Fatal("ensure accepted a changed immutable launch")
	}
}

func identity(attempt Attempt) AttemptIdentity {
	return AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, SupervisorID: attempt.SupervisorID, SupervisorGeneration: attempt.SupervisorGeneration}
}
