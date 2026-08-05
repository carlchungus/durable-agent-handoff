package session

import (
	"fmt"
	"os"
	"path/filepath"
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

func TestLedgerEventAfterSnapshotRemainsVisible(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	agent, err := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	message := Message{ID: "message-1", Sequence: 1, From: "human", Body: "after snapshot", State: MessageQueued, CreatedAt: now}
	if err = store.appendLocked(agent.ID, Event{SessionID: agent.ID, Type: "message.queued", At: now, Data: message}); err != nil {
		t.Fatal(err)
	}
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
	if requeued.Inbox[0].State != MessageQueued || requeued.Inbox[0].DeliveryAttempt != 0 {
		t.Fatalf("inbox=%+v", requeued.Inbox)
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
