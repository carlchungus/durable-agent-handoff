package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestDurableAgentReplySurvivesSnapshotLoss(t *testing.T) {
	state := t.TempDir()
	store, err := OpenStore(state)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := store.Ensure(Descriptor{
		WorkflowID:       "wf_alpha",
		NodeID:           "researcher",
		ParentAgentID:    "agent_parent",
		Name:             "Researcher",
		Runtime:          "claude",
		RuntimeSessionID: "session-exact",
		Worktree:         t.TempDir(),
		LogicalState:     LogicalNeedsInput,
		ProcessState:     ProcessExited,
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := store.Queue(agent.ID, "human", "blue")
	if err != nil {
		t.Fatal(err)
	}
	if message.Sequence != 1 || message.State != MessageQueued || message.Body != "blue" {
		t.Fatalf("message=%+v", message)
	}

	if err = os.Remove(filepath.Join(state, "sessions", agent.ID, "state.json")); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Load(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID != agent.ID || replayed.RuntimeSessionID != "session-exact" || replayed.LogicalState != LogicalNeedsInput || replayed.ProcessState != ProcessExited {
		t.Fatalf("replayed=%+v", replayed)
	}
	if len(replayed.Inbox) != 1 || replayed.Inbox[0].Sequence != 1 || replayed.Inbox[0].Body != "blue" || replayed.Inbox[0].State != MessageQueued {
		t.Fatalf("replayed inbox=%+v", replayed.Inbox)
	}

	again, err := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != agent.ID || len(again.Inbox) != 1 {
		t.Fatalf("ensure changed identity or state: %+v", again)
	}
}

func TestExistingSessionLedgerBytesRemainReadable(t *testing.T) {
	state := t.TempDir()
	id := stableID("wf_legacy", "researcher")
	dir := filepath.Join(state, "sessions", id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger := fmt.Sprintf("{\"sequence\":1,\"session_id\":%q,\"type\":\"session.created\",\"at\":\"2026-08-05T12:00:00Z\",\"data\":{\"version\":1,\"id\":%q,\"workflow_id\":\"wf_legacy\",\"node_id\":\"researcher\",\"name\":\"Legacy researcher\",\"runtime\":\"codex\",\"runtime_session_id\":\"thread-exact\",\"worktree\":\"/tmp/legacy\",\"logical_state\":\"needs_input\",\"process_state\":\"exited\",\"created_at\":\"2026-08-05T12:00:00Z\",\"updated_at\":\"2026-08-05T12:00:00Z\"}}\n", id, id)
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte(ledger), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenStore(state)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != id || got.RuntimeSessionID != "thread-exact" || got.LogicalState != LogicalNeedsInput {
		t.Fatalf("legacy ledger changed meaning: %+v", got)
	}
}

func TestLedgerEventAfterSnapshotRemainsVisible(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	agent, err := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	message := Message{ID: "message-1", Sequence: 1, From: "human", Body: "after snapshot", State: MessageQueued, CreatedAt: now}
	lease, err := store.acquire(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.appendLocked(lease, agent.ID, Event{SessionID: agent.ID, Type: "message.queued", At: now, Data: message}); err != nil {
		lease.release()
		t.Fatal(err)
	}
	lease.release()
	loaded, err := store.Load(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Inbox) != 1 || loaded.Inbox[0].Body != "after snapshot" {
		t.Fatalf("ledger event hidden by stale snapshot: %+v", loaded.Inbox)
	}
}

func TestAppendRepairsPartialLedgerTail(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	agent, err := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "sessions", agent.ID, "events.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteString(`{"sequence":2,"session_id":"`); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	message, err := store.Queue(agent.ID, "human", "survives torn tail")
	if err != nil {
		t.Fatal(err)
	}
	if message.Sequence != 1 {
		t.Fatalf("message sequence=%d", message.Sequence)
	}
	loaded, err := store.Load(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Inbox) != 1 || loaded.Inbox[0].Body != "survives torn tail" {
		t.Fatalf("loaded=%+v", loaded.Inbox)
	}
}

func TestMessageDeliveryIsFencedToOneRuntimeAttempt(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	agent, err := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher", RuntimeSessionID: "session-exact"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Queue(agent.ID, "human", "blue"); err != nil {
		t.Fatal(err)
	}
	dispatched, err := store.Dispatch(agent.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(dispatched) != 1 || dispatched[0].State != MessageDispatched || dispatched[0].DeliveryAttempt != 2 {
		t.Fatalf("dispatched=%+v", dispatched)
	}
	if err = store.Deliver(agent.ID, 1); err == nil {
		t.Fatal("another attempt acknowledged the message")
	}
	if err = store.Deliver(agent.ID, 2); err != nil {
		t.Fatal(err)
	}
	delivered, err := store.Load(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if delivered.Inbox[0].State != MessageDelivered || delivered.Inbox[0].DeliveryAttempt != 2 || delivered.Inbox[0].DeliveredAt.IsZero() {
		t.Fatalf("inbox=%+v", delivered.Inbox)
	}
}

func TestInterruptedAttemptRequeuesOnlyItsOwnMessages(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	agent, err := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Queue(agent.ID, "human", "blue"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Dispatch(agent.ID, 2); err != nil {
		t.Fatal(err)
	}
	if err = store.Requeue(agent.ID, 1); err == nil {
		t.Fatal("another attempt requeued the message")
	}
	if err = store.Requeue(agent.ID, 2); err != nil {
		t.Fatal(err)
	}
	requeued, err := store.Load(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if requeued.Inbox[0].State != MessageQueued || requeued.Inbox[0].DeliveryAttempt != 2 {
		t.Fatalf("inbox=%+v", requeued.Inbox)
	}
	redispatched, err := store.Dispatch(agent.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(redispatched) != 1 || redispatched[0].DeliveryAttempt != 3 {
		t.Fatalf("refunded node attempt reused delivery fence: %+v", redispatched)
	}
}

func TestAgentViewSeparatesLogicalAndProcessState(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	agent, err := store.Ensure(Descriptor{
		WorkflowID:       "wf_alpha",
		NodeID:           "researcher",
		Runtime:          "claude",
		RuntimeSessionID: "session-exact",
		LogicalState:     LogicalWorking,
		ProcessState:     ProcessRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Observe(agent.ID, Observation{LogicalState: LogicalNeedsInput, ProcessState: ProcessExited}); err != nil {
		t.Fatal(err)
	}
	listed, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 {
		t.Fatalf("listed=%+v", listed)
	}
	got := listed[0]
	if got.ID != agent.ID || got.WorkflowID != "wf_alpha" || got.NodeID != "researcher" || got.RuntimeSessionID != "session-exact" {
		t.Fatalf("identity changed: %+v", got)
	}
	if got.LogicalState != LogicalNeedsInput || got.ProcessState != ProcessExited {
		t.Fatalf("logical=%s process=%s", got.LogicalState, got.ProcessState)
	}
	byNode, err := store.LoadByNode("wf_alpha", "researcher")
	if err != nil || byNode.ID != agent.ID {
		t.Fatalf("by node=%+v err=%v", byNode, err)
	}
}

func TestConcurrentWritersPreserveEveryMessageSequence(t *testing.T) {
	state := t.TempDir()
	left, _ := OpenStore(state)
	right, _ := OpenStore(state)
	agent, err := left.Ensure(Descriptor{WorkflowID: "wf-safe", NodeID: "lead"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			writer := left
			if i%2 == 1 {
				writer = right
			}
			_, queueErr := writer.Queue(agent.ID, "human", fmt.Sprintf("message %d", i))
			errs <- queueErr
		}(i)
	}
	wg.Wait()
	close(errs)
	for err = range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := left.Load(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Inbox) != 20 {
		t.Fatalf("messages=%d", len(got.Inbox))
	}
	seen := make(map[uint64]bool, 20)
	for _, message := range got.Inbox {
		seen[message.Sequence] = true
	}
	for sequence := uint64(1); sequence <= 20; sequence++ {
		if !seen[sequence] {
			t.Fatalf("missing sequence %d: %+v", sequence, got.Inbox)
		}
	}
}

func TestSessionIDsCannotEscapeStateDirectory(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	for _, id := range []string{"../outside", "agent_deadbeef/../../outside", "agent_ABC"} {
		if _, err := store.Load(id); err == nil {
			t.Fatalf("Load(%q) accepted unsafe id", id)
		}
		if _, err := store.Queue(id, "human", "hello"); err == nil {
			t.Fatalf("Queue(%q) accepted unsafe id", id)
		}
	}
}

func TestPreOpenIdentitySwapsAreRejected(t *testing.T) {
	t.Run("store root", func(t *testing.T) {
		base := t.TempDir()
		state := filepath.Join(base, "state")
		if err := os.Mkdir(state, 0o700); err != nil {
			t.Fatal(err)
		}
		store, _ := OpenStore(state)
		var once sync.Once
		store.safetyHooks.afterRootPrecheck = func() {
			once.Do(func() {
				if err := os.Rename(state, filepath.Join(base, "old-state")); err != nil {
					t.Error(err)
					return
				}
				if err := os.Mkdir(state, 0o700); err != nil {
					t.Error(err)
				}
			})
		}
		if _, err := store.openRoot(); err == nil {
			t.Fatal("store root replacement between Lstat and open was accepted")
		}
	})

	t.Run("session directory", func(t *testing.T) {
		state := t.TempDir()
		store, _ := OpenStore(state)
		id := stableID("wf_swap_session", "lead")
		sessionDir := filepath.Join(state, "sessions", id)
		if err := os.Mkdir(sessionDir, 0o700); err != nil {
			t.Fatal(err)
		}
		oldDir := filepath.Join(state, "sessions", id+"-old")
		var once sync.Once
		store.safetyHooks.afterChildPrecheck = func(name string) {
			if name != id {
				return
			}
			once.Do(func() {
				if err := os.Rename(sessionDir, oldDir); err != nil {
					t.Error(err)
					return
				}
				if err := os.Mkdir(sessionDir, 0o700); err != nil {
					t.Error(err)
				}
			})
		}
		if _, err := store.openSessionRoot(id, false); err == nil {
			t.Fatal("session directory replacement between Lstat and open was accepted")
		}
	})

	t.Run("storage file", func(t *testing.T) {
		state := t.TempDir()
		store, _ := OpenStore(state)
		id := stableID("wf_swap_file", "lead")
		sessionDir := filepath.Join(state, "sessions", id)
		if err := os.Mkdir(sessionDir, 0o700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(sessionDir, ".write.lock")
		oldPath := path + ".old"
		if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
			t.Fatal(err)
		}
		root, err := store.openSessionRoot(id, false)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		var once sync.Once
		store.safetyHooks.afterFilePrecheck = func(name string) {
			if name != ".write.lock" {
				return
			}
			once.Do(func() {
				if err := os.Rename(path, oldPath); err != nil {
					t.Error(err)
					return
				}
				if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
					t.Error(err)
				}
			})
		}
		if _, err = store.openRegular(root, ".write.lock", os.O_RDWR, 0); err == nil {
			t.Fatal("file replacement between Lstat and open was accepted")
		}
		assertFileContent(t, oldPath, "first")
		assertFileContent(t, path, "second")
	})
}

func TestOpenStoreRejectsGroupWritableRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows uses profile ACLs rather than Unix mode bits")
	}
	state := t.TempDir()
	if err := os.Chmod(state, 0o770); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(state); err == nil {
		t.Fatal("OpenStore accepted a group-writable control-plane root")
	}
}

func TestPostLockReplacementIsRejectedBeforeOwnerOrLedgerWrite(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	id := stableID("wf_post_lock_swap", "lead")
	sessionDir := filepath.Join(state, "sessions", id)
	if err := os.Mkdir(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(sessionDir, ".write.lock")
	retired := path + ".retired"
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	var swapErr error
	renameSucceeded := false
	store.safetyHooks.afterLock = func() {
		if swapErr = os.Rename(path, retired); swapErr != nil {
			return
		}
		renameSucceeded = true
		swapErr = os.WriteFile(path, []byte("replacement"), 0o600)
	}
	lease, err := store.acquire(id)
	if !renameSucceeded {
		if err != nil {
			t.Fatalf("platform prevented replacement but acquire failed: swap=%v acquire=%v", swapErr, err)
		}
		lease.release()
		return
	}
	if swapErr != nil {
		t.Fatal(swapErr)
	}
	if err == nil {
		lease.release()
		t.Fatal("post-lock public-entry replacement was accepted")
	}
	assertFileContent(t, retired, "original")
	assertFileContent(t, path, "replacement")
	if _, err := os.Stat(filepath.Join(sessionDir, "events.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ledger was created after lock identity drift: %v", err)
	}
}

func TestLockDriftAtMutationBoundaryLeavesStateUnchanged(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	agent, err := store.Ensure(Descriptor{WorkflowID: "wf_boundary_swap", NodeID: "lead"})
	if err != nil {
		t.Fatal(err)
	}
	sessionDir := filepath.Join(state, "sessions", agent.ID)
	lockPath := filepath.Join(sessionDir, ".write.lock")
	retired := lockPath + ".retired"
	ledgerPath := filepath.Join(sessionDir, "events.jsonl")
	statePath := filepath.Join(sessionDir, "state.json")
	ledgerBefore, _ := os.ReadFile(ledgerPath)
	stateBefore, _ := os.ReadFile(statePath)
	var once sync.Once
	var swapErr error
	renameSucceeded := false
	store.safetyHooks.beforeValidation = func(boundary string) {
		if boundary != "ledger open" {
			return
		}
		once.Do(func() {
			if swapErr = os.Rename(lockPath, retired); swapErr != nil {
				return
			}
			renameSucceeded = true
			swapErr = os.WriteFile(lockPath, []byte("replacement"), 0o600)
		})
	}
	_, err = store.Queue(agent.ID, "human", "must not append")
	if !renameSucceeded {
		if err != nil {
			t.Fatalf("platform prevented replacement but queue failed: swap=%v queue=%v", swapErr, err)
		}
		return
	}
	if swapErr != nil {
		t.Fatal(swapErr)
	}
	if err == nil {
		t.Fatal("queue continued after lock drift")
	}
	assertFileContent(t, lockPath, "replacement")
	ledgerAfter, _ := os.ReadFile(ledgerPath)
	stateAfter, _ := os.ReadFile(statePath)
	if !bytes.Equal(ledgerBefore, ledgerAfter) || !bytes.Equal(stateBefore, stateAfter) {
		t.Fatal("session state changed after lock drift was detected")
	}
}

func TestOpenStoreRejectsLinkedSessionsDirectory(t *testing.T) {
	state := t.TempDir()
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "sentinel")
	if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireSymlink(t, outside, filepath.Join(state, "sessions"))
	if _, err := OpenStore(state); err == nil {
		t.Fatal("OpenStore accepted a linked sessions directory")
	}
	assertFileContent(t, sentinel, "unchanged")
}

func TestStoreRejectsLinkedSessionDirectory(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "sentinel")
	if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	id := stableID("wf_linked_session", "lead")
	requireSymlink(t, outside, filepath.Join(state, "sessions", id))
	if _, err := store.acquire(id); err == nil {
		t.Fatal("acquire accepted a linked session directory")
	}
	assertFileContent(t, sentinel, "unchanged")
}

func TestStoreFilesCannotRedirectOutsideSessionDirectory(t *testing.T) {
	for _, tc := range []struct {
		name     string
		filename string
		operate  func(*testing.T, *Store, string) error
	}{
		{
			name:     "lock",
			filename: ".write.lock",
			operate: func(_ *testing.T, store *Store, id string) error {
				_, err := store.acquire(id)
				return err
			},
		},
		{
			name:     "ledger",
			filename: "events.jsonl",
			operate: func(_ *testing.T, store *Store, _ string) error {
				_, err := store.Ensure(Descriptor{WorkflowID: "wf_linked_file", NodeID: "lead"})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := t.TempDir()
			store, _ := OpenStore(state)
			id := stableID("wf_linked_file", "lead")
			sessionDir := filepath.Join(state, "sessions", id)
			if err := os.Mkdir(sessionDir, 0o700); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
				t.Fatal(err)
			}
			requireSymlink(t, sentinel, filepath.Join(sessionDir, tc.filename))
			if err := tc.operate(t, store, id); err == nil {
				t.Fatalf("operation accepted linked %s", tc.filename)
			}
			assertFileContent(t, sentinel, "unchanged")
		})
	}
}

func TestSnapshotCannotFollowStateOrTemporaryLink(t *testing.T) {
	t.Run("state destination", func(t *testing.T) {
		state := t.TempDir()
		store, _ := OpenStore(state)
		id := stableID("wf_state_link", "lead")
		sessionDir := filepath.Join(state, "sessions", id)
		if err := os.Mkdir(sessionDir, 0o700); err != nil {
			t.Fatal(err)
		}
		sentinel := filepath.Join(t.TempDir(), "sentinel")
		if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
			t.Fatal(err)
		}
		requireSymlink(t, sentinel, filepath.Join(sessionDir, "state.json"))
		if _, err := store.Ensure(Descriptor{WorkflowID: "wf_state_link", NodeID: "lead"}); err != nil {
			t.Fatal(err)
		}
		assertFileContent(t, sentinel, "unchanged")
		info, err := os.Lstat(filepath.Join(sessionDir, "state.json"))
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("snapshot did not replace link with a regular file: info=%v err=%v", info, err)
		}
	})

	t.Run("temporary file", func(t *testing.T) {
		state := t.TempDir()
		store, _ := OpenStore(state)
		fixed := time.Unix(123, 456)
		store.now = func() time.Time { return fixed }
		id := stableID("wf_temp_link", "lead")
		sessionDir := filepath.Join(state, "sessions", id)
		if err := os.Mkdir(sessionDir, 0o700); err != nil {
			t.Fatal(err)
		}
		sentinel := filepath.Join(t.TempDir(), "sentinel")
		if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
			t.Fatal(err)
		}
		tmp := fmt.Sprintf(".state.json.tmp-%d-%d", os.Getpid(), fixed.UnixNano())
		requireSymlink(t, sentinel, filepath.Join(sessionDir, tmp))
		if _, err := store.Ensure(Descriptor{WorkflowID: "wf_temp_link", NodeID: "lead"}); err == nil {
			t.Fatal("snapshot accepted a planted temporary-file link")
		}
		assertFileContent(t, sentinel, "unchanged")
	})
}

func TestLockHardLinkCannotTruncateOutsideFile(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	id := stableID("wf_hard_link", "lead")
	sessionDir := filepath.Join(state, "sessions", id)
	if err := os.Mkdir(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(sentinel, filepath.Join(sessionDir, ".write.lock")); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if _, err := store.acquire(id); err == nil {
		t.Fatal("acquire accepted a multiply-linked lock file")
	}
	assertFileContent(t, sentinel, "unchanged")
}

func TestLockContentionTimesOutWithBoundedBackoff(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	current := time.Unix(100, 0)
	attempts := 0
	sleeps := 0
	slept := time.Duration(0)
	store.newFileLock = func(*os.File) sessionFileLock {
		return &alwaysContendedLock{attempts: &attempts}
	}
	store.lockTimeout = 25 * time.Millisecond
	store.lockRetry = 10 * time.Millisecond
	store.now = func() time.Time { return current }
	store.sleep = func(delay time.Duration) {
		sleeps++
		slept += delay
		current = current.Add(delay)
	}
	if _, err := store.acquire(stableID("wf_timeout", "lead")); err == nil {
		t.Fatal("persistent contention did not return a timeout")
	}
	if attempts != 3 || sleeps != 3 || slept != 25*time.Millisecond {
		t.Fatalf("attempts=%d sleeps=%d slept=%s", attempts, sleeps, slept)
	}
}

func TestSeparateStoresContendOnStableKernelLock(t *testing.T) {
	state := t.TempDir()
	owner, _ := OpenStore(state)
	waiter, _ := OpenStore(state)
	waiter.lockTimeout = 25 * time.Millisecond
	waiter.lockRetry = time.Millisecond
	id := stableID("wf_process_local", "lead")
	ownerLease, err := owner.acquire(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = waiter.acquire(id); err == nil {
		t.Fatal("a separate Store acquired the same kernel lock")
	}
	ownerLease.release()
	nextLease, err := waiter.acquire(id)
	if err != nil {
		t.Fatalf("released kernel lock was not reacquired: %v", err)
	}
	nextLease.release()
	ownerRecord := readLockOwner(t, filepath.Join(state, "sessions", id, ".write.lock"))
	if ownerRecord.State != sessionLockReleased || ownerRecord.PID != os.Getpid() {
		t.Fatalf("release metadata was not durable: %+v", ownerRecord)
	}
}

func TestOldReleaseCannotAffectSuccessorLock(t *testing.T) {
	state := t.TempDir()
	first, _ := OpenStore(state)
	second, _ := OpenStore(state)
	id := stableID("wf_fenced", "lead")
	beforeUnlock := make(chan struct{})
	allowUnlock := make(chan struct{})
	first.newFileLock = func(file *os.File) sessionFileLock {
		return &barrierUnlockLock{
			sessionFileLock: newPlatformFileLock(file),
			beforeUnlock:    beforeUnlock,
			allowUnlock:     allowUnlock,
		}
	}
	observedContention := make(chan struct{})
	second.newFileLock = func(file *os.File) sessionFileLock {
		return &contentionObserverLock{
			sessionFileLock: newPlatformFileLock(file),
			observed:        observedContention,
		}
	}
	oldLease, err := first.acquire(id)
	if err != nil {
		t.Fatal(err)
	}
	releaseDone := make(chan struct{})
	go func() {
		oldLease.release()
		close(releaseDone)
	}()
	<-beforeUnlock
	successorResult := make(chan struct {
		lease *sessionLease
		err   error
	}, 1)
	go func() {
		lease, acquireErr := second.acquire(id)
		successorResult <- struct {
			lease *sessionLease
			err   error
		}{lease: lease, err: acquireErr}
	}()
	<-observedContention
	close(allowUnlock)
	<-releaseDone
	successor := <-successorResult
	if successor.err != nil {
		t.Fatal(successor.err)
	}
	successorLeaseID := successor.lease.owner.LeaseID
	oldLease.release()
	blocked, _ := OpenStore(state)
	blocked.lockTimeout = 25 * time.Millisecond
	blocked.lockRetry = time.Millisecond
	if _, err = blocked.acquire(id); err == nil {
		t.Fatal("old release unlocked the successor's kernel lock")
	}
	successor.lease.release()
	after := readLockOwner(t, filepath.Join(state, "sessions", id, ".write.lock"))
	if after.LeaseID != successorLeaseID || after.State != sessionLockReleased {
		t.Fatalf("old release mutated successor metadata: %+v", after)
	}
}

func TestKernelLockReleasedWhenOwnerProcessExits(t *testing.T) {
	state := t.TempDir()
	id := stableID("wf_crash", "lead")
	ready := filepath.Join(state, "lock-ready")
	command := exec.Command(os.Args[0], "-test.run=^TestSessionLockCrashHelper$")
	command.Env = append(os.Environ(),
		"HANDOFF_SESSION_LOCK_CRASH_HELPER=1",
		"HANDOFF_SESSION_LOCK_STATE="+state,
		"HANDOFF_SESSION_LOCK_ID="+id,
		"HANDOFF_SESSION_LOCK_READY="+ready,
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("crash helper did not acquire the lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	store, _ := OpenStore(state)
	store.lockTimeout = 25 * time.Millisecond
	store.lockRetry = time.Millisecond
	if unexpectedLease, err := store.acquire(id); err == nil {
		unexpectedLease.release()
		t.Fatal("parent acquired a lock held by a separate process")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed crash helper exited successfully")
	}
	waited = true
	store.lockTimeout = defaultLockTimeout
	lease, err := store.acquire(id)
	if err != nil {
		t.Fatalf("process exit did not release kernel lock: %v", err)
	}
	lease.release()
}

func TestSessionLockCrashHelper(t *testing.T) {
	if os.Getenv("HANDOFF_SESSION_LOCK_CRASH_HELPER") != "1" {
		return
	}
	store, err := OpenStore(os.Getenv("HANDOFF_SESSION_LOCK_STATE"))
	if err != nil {
		os.Exit(2)
	}
	lease, err := store.acquire(os.Getenv("HANDOFF_SESSION_LOCK_ID"))
	if err != nil {
		os.Exit(3)
	}
	_ = lease
	if err = os.WriteFile(os.Getenv("HANDOFF_SESSION_LOCK_READY"), []byte("ready\n"), 0o600); err != nil {
		os.Exit(4)
	}
	time.Sleep(time.Hour)
}

type alwaysContendedLock struct {
	attempts *int
}

func (l *alwaysContendedLock) TryLock() (bool, error) {
	(*l.attempts)++
	return false, nil
}

func (l *alwaysContendedLock) Unlock() error { return nil }

type barrierUnlockLock struct {
	sessionFileLock
	beforeUnlock chan struct{}
	allowUnlock  chan struct{}
}

func (l *barrierUnlockLock) Unlock() error {
	close(l.beforeUnlock)
	<-l.allowUnlock
	return l.sessionFileLock.Unlock()
}

type contentionObserverLock struct {
	sessionFileLock
	observed chan struct{}
	once     sync.Once
}

func (l *contentionObserverLock) TryLock() (bool, error) {
	locked, err := l.sessionFileLock.TryLock()
	if !locked && err == nil {
		l.once.Do(func() { close(l.observed) })
	}
	return locked, err
}

func readLockOwner(t *testing.T, path string) sessionLockOwner {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var owner sessionLockOwner
	if err = json.Unmarshal(b, &owner); err != nil {
		t.Fatal(err)
	}
	return owner
}

func requireSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("creating links requires Windows Developer Mode or elevation: %v", err)
		}
		t.Fatal(err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != want {
		t.Fatalf("%s content=%q, want %q", path, b, want)
	}
}
