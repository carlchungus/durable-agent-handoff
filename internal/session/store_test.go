package session

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
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
	if len(replayed.Inbox) != 1 || replayed.Inbox[0].Body != "blue" || replayed.Inbox[0].State != MessageQueued {
		t.Fatalf("replayed inbox=%+v", replayed.Inbox)
	}
	again, err := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	if err != nil || again.ID != agent.ID || len(again.Inbox) != 1 {
		t.Fatalf("ensure changed identity or state: agent=%+v err=%v", again, err)
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

func TestAppendRepairsPartialLedgerTail(t *testing.T) {
	state := t.TempDir()
	store, _ := OpenStore(state)
	agent, err := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "sessions", agent.ID, "events.jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString(`{"sequence":2,"session_id":"`); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	message, err := store.Queue(agent.ID, "human", "survives torn tail")
	if err != nil || message.Sequence != 1 {
		t.Fatalf("message=%+v err=%v", message, err)
	}
	loaded, err := store.Load(agent.ID)
	if err != nil || len(loaded.Inbox) != 1 || loaded.Inbox[0].Body != "survives torn tail" {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
}

func TestMessageDeliveryIsFencedToOneRuntimeAttempt(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	agent, _ := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher", RuntimeSessionID: "session-exact"})
	_, _ = store.Queue(agent.ID, "human", "blue")
	dispatched, err := store.Dispatch(agent.ID, 2)
	if err != nil || len(dispatched) != 1 || dispatched[0].DeliveryAttempt != 2 {
		t.Fatalf("dispatched=%+v err=%v", dispatched, err)
	}
	if err = store.Deliver(agent.ID, 1); err == nil {
		t.Fatal("another attempt acknowledged the message")
	}
	if err = store.Deliver(agent.ID, 2); err != nil {
		t.Fatal(err)
	}
	delivered, _ := store.Load(agent.ID)
	if delivered.Inbox[0].State != MessageDelivered || delivered.Inbox[0].DeliveredAt.IsZero() {
		t.Fatalf("inbox=%+v", delivered.Inbox)
	}
}

func TestInterruptedAttemptRequeuesWithoutReusingItsFence(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	agent, _ := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	_, _ = store.Queue(agent.ID, "human", "blue")
	_, _ = store.Dispatch(agent.ID, 2)
	if err := store.Requeue(agent.ID, 1); err == nil {
		t.Fatal("another attempt requeued the message")
	}
	if err := store.Requeue(agent.ID, 2); err != nil {
		t.Fatal(err)
	}
	redispatched, err := store.Dispatch(agent.ID, 1)
	if err != nil || len(redispatched) != 1 || redispatched[0].DeliveryAttempt != 3 {
		t.Fatalf("redispatched=%+v err=%v", redispatched, err)
	}
}

func TestAgentViewSeparatesLogicalAndProcessState(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	agent, _ := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher", Runtime: "claude", RuntimeSessionID: "session-exact", LogicalState: LogicalWorking, ProcessState: ProcessRunning})
	if err := store.Observe(agent.ID, Observation{LogicalState: LogicalNeedsInput, ProcessState: ProcessExited}); err != nil {
		t.Fatal(err)
	}
	listed, err := store.List()
	if err != nil || len(listed) != 1 {
		t.Fatalf("listed=%+v err=%v", listed, err)
	}
	got := listed[0]
	if got.LogicalState != LogicalNeedsInput || got.ProcessState != ProcessExited || got.RuntimeSessionID != "session-exact" {
		t.Fatalf("state=%+v", got)
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
	if err != nil || len(got.Inbox) != 20 {
		t.Fatalf("messages=%d err=%v", len(got.Inbox), err)
	}
	seen := make(map[uint64]bool, 20)
	for _, message := range got.Inbox {
		seen[message.Sequence] = true
	}
	for sequence := uint64(1); sequence <= 20; sequence++ {
		if !seen[sequence] {
			t.Fatalf("missing sequence %d", sequence)
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

func TestObservationValidationFailsBeforeMutation(t *testing.T) {
	store, _ := OpenStore(t.TempDir())
	agent, _ := store.Ensure(Descriptor{WorkflowID: "wf_alpha", NodeID: "researcher"})
	if err := store.Observe(agent.ID, Observation{LogicalState: "maybe"}); err == nil {
		t.Fatal("accepted invalid logical state")
	}
	if err := store.Observe(agent.ID, Observation{ProcessState: "zombie"}); err == nil {
		t.Fatal("accepted invalid process state")
	}
	got, err := store.Load(agent.ID)
	if err != nil || got.LogicalState != LogicalWorking || got.ProcessState != ProcessExited {
		t.Fatalf("invalid observation mutated state: %+v err=%v", got, err)
	}
}
