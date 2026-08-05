package activity

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	crashHelperEnv  = "HANDOFF_ACTIVITY_CRASH_HELPER"
	workerHelperEnv = "HANDOFF_ACTIVITY_WORKER_HELPER"
	stateHelperEnv  = "HANDOFF_ACTIVITY_TEST_STATE"
)

type crashReady struct {
	ActivityID string  `json:"activity_id"`
	Attempt    Attempt `json:"attempt"`
}

func TestSupervisorCrashReattachesByCursorAndFencesStaleStop(t *testing.T) {
	state := t.TempDir()
	helper := exec.Command(os.Args[0], "-test.run=^TestActivitySupervisorCrashHelper$")
	helper.Env = append(os.Environ(), crashHelperEnv+"=1", stateHelperEnv+"="+state)
	readyPipe, err := helper.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	helper.Stderr = os.Stderr
	if err = helper.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	var ready crashReady
	t.Cleanup(func() {
		if !waited {
			_ = helper.Process.Kill()
			_ = helper.Wait()
		}
		if ready.Attempt.PID > 0 && processMatches(identityOf(ready.Attempt)) {
			if process, findErr := os.FindProcess(ready.Attempt.PID); findErr == nil {
				_ = process.Kill()
			}
		}
	})
	if err = json.NewDecoder(readyPipe).Decode(&ready); err != nil {
		t.Fatalf("read helper readiness: %v", err)
	}
	if ready.ActivityID == "" || ready.Attempt.PID <= 0 || !processMatches(identityOf(ready.Attempt)) {
		t.Fatalf("invalid ready state: %+v", ready)
	}
	store, err := OpenStore(state)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Load(ready.ActivityID)
	if err != nil || before.State != StateRunning || before.Generation != 1 {
		t.Fatalf("before=%+v err=%v", before, err)
	}

	if err = helper.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = helper.Wait(); err == nil {
		t.Fatal("killed supervisor exited successfully")
	}
	waited = true
	if !processMatches(identityOf(ready.Attempt)) {
		t.Fatal("worker did not survive supervisor crash")
	}

	supervisor := &Supervisor{Store: store}
	recovered, err := supervisor.Recover()
	if err != nil || len(recovered) != 1 {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	after := recovered[0]
	current, ok := currentAttempt(after)
	if !ok || after.Generation != 2 || current.ID != ready.Attempt.ID || current.SupervisorGeneration != 2 || current.Stdout.ID != ready.Attempt.Stdout.ID {
		t.Fatalf("adopted=%+v", after)
	}
	owned, err := supervisor.Recover()
	if err != nil || len(owned) != 0 {
		t.Fatalf("same supervisor re-adopted activity: recovered=%+v err=%v", owned, err)
	}
	unchanged, err := store.Load(after.ID)
	if err != nil || unchanged.Generation != after.Generation || unchanged.Attempts[0].SupervisorGeneration != current.SupervisorGeneration {
		t.Fatalf("same-owner recovery changed fencing tokens: activity=%+v err=%v", unchanged, err)
	}
	chunk, err := store.ReadOutput(after.ID, OutputCursor{AttemptID: current.ID, Stream: StreamStdout, OutputID: current.Stdout.ID, After: 0}, 64<<10)
	if err != nil || !strings.Contains(string(chunk.Data), "one\n") {
		t.Fatalf("chunk=%q err=%v", chunk.Data, err)
	}
	if _, _, err = store.RequestStop(after.ID, ControlRequest{ExpectedGeneration: before.Generation, ExpectedAttempt: identityOf(ready.Attempt)}); err != ErrFenced {
		t.Fatalf("stale stop err=%v", err)
	}
	stopped, err := supervisor.Stop(after.ID)
	if err != nil || stopped.State != StateStopped {
		t.Fatalf("stopped=%+v err=%v", stopped, err)
	}
	if processMatches(identityOf(current)) {
		t.Fatal("exact worker remains live after stop")
	}
	if err = os.Remove(filepath.Join(state, "activities", after.ID, "state.json")); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Load(after.ID)
	if err != nil || replayed.State != StateStopped || replayed.Generation != stopped.Generation || replayed.Revision != stopped.Revision || replayed.Attempts[0].Stdout.ID != current.Stdout.ID {
		t.Fatalf("replayed=%+v stopped=%+v err=%v", replayed, stopped, err)
	}
}

func TestActivitySupervisorCrashHelper(t *testing.T) {
	if os.Getenv(crashHelperEnv) != "1" {
		return
	}
	store, err := OpenStore(os.Getenv(stateHelperEnv))
	if err != nil {
		os.Exit(2)
	}
	supervisor := &Supervisor{Store: store, Env: []string{workerHelperEnv + "=1"}}
	activity, attempt, err := supervisor.Start(Descriptor{
		ID:      "activity_1234567890abcdef12345678",
		Work:    WorkSpec{Kind: "command", Cwd: os.TempDir(), Intent: "crash recovery test"},
		Command: []string{os.Args[0], "-test.run=^TestActivityWorkerHelper$"},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		chunk, readErr := store.ReadOutput(activity.ID, OutputCursor{AttemptID: attempt.ID, Stream: StreamStdout, OutputID: attempt.Stdout.ID, After: 0}, 64<<10)
		if readErr == nil && strings.Contains(string(chunk.Data), "one\n") {
			break
		}
		if time.Now().After(deadline) {
			fmt.Fprintln(os.Stderr, "worker output was not durable before readiness")
			os.Exit(4)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = json.NewEncoder(os.Stdout).Encode(crashReady{ActivityID: activity.ID, Attempt: attempt}); err != nil {
		os.Exit(5)
	}
	select {}
}

func TestActivityWorkerHelper(t *testing.T) {
	if os.Getenv(workerHelperEnv) != "1" {
		return
	}
	fmt.Fprint(os.Stdout, "one\n")
	for {
		time.Sleep(time.Hour)
	}
}

func TestRecoverFailsPreparedAttemptInsteadOfLeavingStarting(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	created, _ := store.Create(Descriptor{ID: "activity_cccccccccccccccccccccccc", Work: WorkSpec{Kind: "agent", Cwd: t.TempDir(), Intent: "prepared crash"}})
	_, stdout, stderr, err := store.PrepareAttempt(created.ID, created.Generation, AttemptStart{})
	if err != nil {
		t.Fatal(err)
	}
	_ = stdout.Close()
	_ = stderr.Close()
	if _, err = (&Supervisor{Store: store}).Recover(); err != nil {
		t.Fatal(err)
	}
	recovered, _ := store.Load(created.ID)
	if recovered.State != StateFailed || recovered.Attempts[0].State != StateFailed {
		t.Fatalf("recovered=%+v", recovered)
	}
}

func TestRecoverDoesNotStealFromLiveForeignSupervisor(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	owner := exec.Command(os.Args[0], "-test.run=^TestActivityWorkerHelper$")
	owner.Env = append(os.Environ(), workerHelperEnv+"=1")
	ConfigureBackgroundProcess(owner)
	if err := owner.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Process.Kill(); _, _ = owner.Process.Wait() })
	target := exec.Command(os.Args[0], "-test.run=^TestActivityWorkerHelper$")
	target.Env = append(os.Environ(), workerHelperEnv+"=1")
	ConfigureBackgroundProcess(target)
	if err := target.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Process.Kill(); _, _ = target.Process.Wait() })
	ownerToken := waitForStartToken(owner.Process.Pid, 2*time.Second)
	targetToken := waitForStartToken(target.Process.Pid, 2*time.Second)
	created, _ := store.Create(Descriptor{ID: "activity_dddddddddddddddddddddddd", Work: WorkSpec{Kind: "agent", Cwd: t.TempDir(), Intent: "foreign owner"}})
	attempt, stdout, stderr, _ := store.PrepareAttempt(created.ID, created.Generation, AttemptStart{})
	_ = stdout.Close()
	_ = stderr.Close()
	foreignOwner := fmt.Sprintf("%d:%s", owner.Process.Pid, ownerToken)
	_, err := store.MarkRunning(created.ID, created.Generation, attempt.ID, ProcessIdentity{PID: target.Process.Pid, ProcessStartToken: targetToken, SupervisorID: foreignOwner, SupervisorGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := (&Supervisor{Store: store}).Recover()
	if err != nil || len(recovered) != 0 {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	after, _ := store.Load(created.ID)
	if after.Generation != created.Generation || after.Attempts[0].SupervisorID != foreignOwner {
		t.Fatalf("live foreign ownership changed: %+v", after)
	}
}
