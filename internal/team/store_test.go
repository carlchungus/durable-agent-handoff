package team

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStoreReplaysWithoutSnapshotAndFiltersInbox(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	tm, err := store.Create("review", "wf_test", Member{ID: "lead", Name: "Lead"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Apply(tm.ID, Command{Op: "add_member", Actor: "lead", Member: &Member{ID: "a", Name: "A"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Apply(tm.ID, Command{Op: "add_member", Actor: "lead", Member: &Member{ID: "b", Name: "B"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Apply(tm.ID, Command{Op: "send_message", Actor: "a", To: "lead", Body: "private"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Apply(tm.ID, Command{Op: "send_message", Actor: "b", Body: "broadcast"}); err != nil {
		t.Fatal(err)
	}
	inbox, err := store.Inbox(tm.ID, "a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 2 || inbox[0].Body != "private" || inbox[1].Body != "broadcast" {
		t.Fatalf("unexpected inbox: %+v", inbox)
	}
	if err = os.Remove(filepath.Join(dir, "teams", tm.ID, "state.json")); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Load(tm.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed.Members) != 3 || len(replayed.Messages) != 2 {
		t.Fatalf("replay lost state: %+v", replayed)
	}
}

func TestRejectedCommandDoesNotMutateSnapshotOrLedger(t *testing.T) {
	dir := t.TempDir()
	store, _ := OpenStore(dir)
	tm, _ := store.Create("review", "", Member{ID: "lead", Name: "Lead"})
	before, _ := os.ReadFile(filepath.Join(dir, "teams", tm.ID, "events.jsonl"))
	if _, err := store.Apply(tm.ID, Command{Op: "add_member", Actor: "intruder", Member: &Member{ID: "x", Name: "X"}}); err == nil {
		t.Fatal("intruder command accepted")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "teams", tm.ID, "events.jsonl"))
	if string(before) != string(after) {
		t.Fatal("rejected command changed ledger")
	}
	loaded, _ := store.Load(tm.ID)
	if loaded.Members["x"] != nil {
		t.Fatal("rejected command changed state")
	}
}
