package core

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Store struct {
	dir string
	mu  sync.Mutex
}

func OpenStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store directory is required")
	}
	if err := os.MkdirAll(filepath.Join(dir, "workflows"), 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Dir() string { return s.dir }

func (s *Store) Create(goal, root string, budget Budget) (*Workflow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if goal == "" {
		return nil, errors.New("goal is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	w := &Workflow{ID: newID("wf"), Goal: goal, Root: abs, Status: WorkflowActive, Budget: budget, Nodes: map[string]*Node{}, CreatedAt: now, UpdatedAt: now}
	e := Event{ID: newID("ev"), WorkflowID: w.ID, Type: "workflow.created", Actor: "human", At: now, Data: w}
	if err := s.appendLocked(w.ID, e); err != nil {
		return nil, err
	}
	if err := s.snapshotLocked(w); err != nil {
		return nil, err
	}
	return w, nil
}

func (s *Store) Apply(p Proposal) (*Workflow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquireWorkflowLock(p.WorkflowID)
	if err != nil {
		return nil, err
	}
	defer release()
	w, err := s.loadLocked(p.WorkflowID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err := ApplyProposal(w, p, now); err != nil {
		rejected := Event{ID: newID("ev"), WorkflowID: w.ID, Type: "proposal.rejected", Actor: p.Actor, At: now, Data: map[string]any{"proposal": p, "error": err.Error()}}
		_ = s.appendLocked(w.ID, rejected)
		return nil, err
	}
	e := Event{ID: newID("ev"), WorkflowID: w.ID, Type: "proposal.applied", Actor: p.Actor, At: now, Data: p}
	if err := s.appendLocked(w.ID, e); err != nil {
		return nil, err
	}
	if err := s.snapshotLocked(w); err != nil {
		return nil, err
	}
	return w, nil
}

func (s *Store) acquireWorkflowLock(id string) (func(), error) {
	path := filepath.Join(s.dir, "workflows", id, ".write.lock")
	deadline := time.Now().Add(10 * time.Second)
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = fmt.Fprintf(f, "%d %s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano))
			_ = f.Close()
			return func() { _ = os.Remove(path) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if info, statErr := os.Stat(path); statErr == nil && time.Since(info.ModTime()) > time.Minute {
			_ = os.Remove(path)
			continue
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for workflow %s write lock", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Store) Load(id string) (*Workflow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(id)
}

func (s *Store) loadLocked(id string) (*Workflow, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, "workflows", id, "state.json"))
	if err == nil {
		var w Workflow
		if json.Unmarshal(b, &w) == nil {
			return &w, nil
		}
	}
	return s.replayLocked(id)
}

func (s *Store) replayLocked(id string) (*Workflow, error) {
	f, err := os.Open(filepath.Join(s.dir, "workflows", id, "events.jsonl"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	type rawEvent struct {
		Type string          `json:"type"`
		At   time.Time       `json:"at"`
		Data json.RawMessage `json:"data"`
	}
	dec := json.NewDecoder(f)
	var w *Workflow
	for {
		var event rawEvent
		err = dec.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("replay event ledger: %w", err)
		}
		switch event.Type {
		case "workflow.created":
			var created Workflow
			if err = json.Unmarshal(event.Data, &created); err != nil {
				return nil, err
			}
			w = &created
		case "proposal.applied":
			if w == nil {
				return nil, errors.New("proposal event preceded workflow creation")
			}
			var p Proposal
			if err = json.Unmarshal(event.Data, &p); err != nil {
				return nil, err
			}
			if err = ApplyProposal(w, p, event.At); err != nil {
				return nil, fmt.Errorf("replay proposal: %w", err)
			}
		}
	}
	if w == nil {
		return nil, errors.New("workflow ledger has no creation event")
	}
	return w, nil
}

func (s *Store) List() ([]*Workflow, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "workflows"))
	if err != nil {
		return nil, err
	}
	var out []*Workflow
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		w, err := s.Load(e.Name())
		if err == nil {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out, nil
}

func (s *Store) Events(id string, after uint64) ([]Event, error) {
	f, err := os.Open(filepath.Join(s.dir, "workflows", id, "events.jsonl"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Event
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 4<<20)
	for scan.Scan() {
		var e Event
		if json.Unmarshal(scan.Bytes(), &e) == nil && e.Sequence > after {
			out = append(out, e)
		}
	}
	return out, scan.Err()
}

func (s *Store) appendLocked(id string, event Event) error {
	dir := filepath.Join(s.dir, "workflows", id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, "events.jsonl")
	seq, err := lastSequence(path)
	if err != nil {
		return err
	}
	event.Sequence = seq + 1
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	b, err := json.Marshal(event)
	if err == nil {
		_, err = f.Write(append(b, '\n'))
	}
	if syncErr := f.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (s *Store) snapshotLocked(w *Workflow) error {
	dir := filepath.Join(s.dir, "workflows", w.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := filepath.Join(dir, "state.json.tmp")
	if err = os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "state.json"))
}

func lastSequence(path string) (uint64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var seq uint64
	d := json.NewDecoder(f)
	for {
		var e Event
		err = d.Decode(&e)
		if errors.Is(err, io.EOF) {
			return seq, nil
		}
		if err != nil {
			return 0, fmt.Errorf("decode event ledger: %w", err)
		}
		seq = e.Sequence
	}
}

func newID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}
