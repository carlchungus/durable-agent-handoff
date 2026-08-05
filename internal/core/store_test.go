package core

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStorePersistsLedgerAndSnapshot(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Create("goal", t.TempDir(), DefaultBudget())
	if err != nil {
		t.Fatal(err)
	}
	w, err = st.Apply(Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: "lead", Title: "lead", Kind: "agent"}}}})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := st.Load(w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Nodes["lead"].State != NodeReady {
		t.Fatalf("state=%s", loaded.Nodes["lead"].State)
	}
	events, err := st.Events(w.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("events=%#v", events)
	}
}

func TestStoreReplaysLedgerWhenSnapshotIsMissing(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Create("recover me", t.TempDir(), DefaultBudget())
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Apply(Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: "lead", Title: "lead", Kind: "agent"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, "workflows", w.ID, "state.json")); err != nil {
		t.Fatal(err)
	}
	replayed, err := st.Load(w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Goal != "recover me" || replayed.Nodes["lead"] == nil {
		t.Fatalf("replayed=%#v", replayed)
	}
}

func TestSeparateStoresSerializeConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Create("goal", t.TempDir(), DefaultBudget())
	if err != nil {
		t.Fatal(err)
	}
	const writers = 12
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			other, err := OpenStore(dir)
			if err != nil {
				errs <- err
				return
			}
			id := fmt.Sprintf("n%d", i)
			_, err = other.Apply(Proposal{WorkflowID: w.ID, Actor: "human", Mutations: []Mutation{{Op: "add_node", Node: &Node{ID: id, Title: id, Kind: "observe"}}}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := st.Load(w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Nodes) != writers {
		t.Fatalf("nodes=%d", len(loaded.Nodes))
	}
	events, err := st.Events(w.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != writers+1 {
		t.Fatalf("events=%d", len(events))
	}
	for i, e := range events {
		if e.Sequence != uint64(i+1) {
			t.Fatalf("sequence[%d]=%d", i, e.Sequence)
		}
	}
}
