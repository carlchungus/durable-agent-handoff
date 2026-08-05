package team

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

	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
)

type Store struct {
	dir string
	mu  sync.Mutex
}

func OpenStore(stateDir string) (*Store, error) {
	if stateDir == "" {
		return nil, errors.New("state directory is required")
	}
	dir := filepath.Join(stateDir, "teams")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Create(name, workflowID string, lead Member) (*Team, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == "" || !identifier.MatchString(lead.ID) || lead.Name == "" {
		return nil, errors.New("team name and valid lead are required")
	}
	now := time.Now().UTC()
	lead.State, lead.Process = MemberWorking, ProcessUnknown
	if lead.Plan == "" {
		lead.Plan = PlanNotRequired
	}
	lead.CreatedAt, lead.UpdatedAt = now, now
	t := &Team{ID: newID("team"), WorkflowID: workflowID, Name: name, LeadID: lead.ID, Members: map[string]*Member{lead.ID: &lead}, Tasks: map[string]*Task{}, CreatedAt: now, UpdatedAt: now}
	data, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	e := Event{Sequence: 1, ID: newID("ev"), TeamID: t.ID, Type: "team.created", Actor: lead.ID, At: now, Data: data}
	if err = s.append(t.ID, e); err != nil {
		return nil, err
	}
	if err = s.snapshot(t); err != nil {
		return nil, err
	}
	return t, nil
}

func (s *Store) Apply(teamID string, c Command) (*Team, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquireLock(teamID)
	if err != nil {
		return nil, err
	}
	defer release()
	t, err := s.load(teamID)
	if err != nil {
		return nil, err
	}
	clone, err := cloneTeam(t)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err = Apply(clone, c, now); err != nil {
		return nil, err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	seq, err := s.lastSequence(teamID)
	if err != nil {
		return nil, err
	}
	e := Event{Sequence: seq + 1, ID: newID("ev"), TeamID: teamID, Type: "team.command", Actor: c.Actor, At: now, Data: data}
	if err = s.append(teamID, e); err != nil {
		return nil, err
	}
	if err = s.snapshot(clone); err != nil {
		return nil, err
	}
	return clone, nil
}

func (s *Store) Load(id string) (*Team, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.load(id)
}

func (s *Store) load(id string) (*Team, error) {
	if b, err := os.ReadFile(filepath.Join(s.dir, id, "state.json")); err == nil {
		var t Team
		if json.Unmarshal(b, &t) == nil {
			return &t, nil
		}
	}
	return s.replay(id)
}

func (s *Store) replay(id string) (*Team, error) {
	f, err := os.Open(filepath.Join(s.dir, id, "events.jsonl"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var t *Team
	for {
		var e Event
		err = dec.Decode(&e)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("replay team ledger: %w", err)
		}
		switch e.Type {
		case "team.created":
			var created Team
			if err = json.Unmarshal(e.Data, &created); err != nil {
				return nil, err
			}
			t = &created
		case "team.command":
			if t == nil {
				return nil, errors.New("team command preceded creation")
			}
			var c Command
			if err = json.Unmarshal(e.Data, &c); err != nil {
				return nil, err
			}
			if err = Apply(t, c, e.At); err != nil {
				return nil, fmt.Errorf("replay team command: %w", err)
			}
		}
	}
	if t == nil {
		return nil, errors.New("team ledger has no creation event")
	}
	return t, nil
}

func (s *Store) List() ([]*Team, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	out := make([]*Team, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		t, loadErr := s.load(entry.Name())
		if loadErr == nil {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out, nil
}

func (s *Store) Inbox(id, memberID string, after uint64) ([]Message, error) {
	t, err := s.Load(id)
	if err != nil {
		return nil, err
	}
	if t.Members[memberID] == nil {
		return nil, fmt.Errorf("member %q does not exist", memberID)
	}
	var out []Message
	for _, m := range t.Messages {
		if m.Sequence > after && (m.To == "" || m.To == memberID || m.From == memberID) {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *Store) append(id string, e Event) error {
	dir := filepath.Join(s.dir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (s *Store) snapshot(t *Team) error {
	dir := filepath.Join(s.dir, t.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, ".state."+newID("tmp"))
	if err = os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(dir, "state.json")); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (s *Store) lastSequence(id string) (uint64, error) {
	f, err := os.Open(filepath.Join(s.dir, id, "events.jsonl"))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var seq uint64
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var e Event
		if json.Unmarshal(scan.Bytes(), &e) == nil && e.Sequence > seq {
			seq = e.Sequence
		}
	}
	return seq, scan.Err()
}

func (s *Store) acquireLock(id string) (func(), error) {
	path := filepath.Join(s.dir, id, ".write.lock")
	deadline := time.Now().Add(10 * time.Second)
	for {
		owner := struct {
			PID        int    `json:"pid"`
			StartToken string `json:"start_token"`
		}{os.Getpid(), runstate.ProcessStartToken(os.Getpid())}
		b, _ := json.Marshal(owner)
		candidate := path + "." + newID("owner")
		if err := os.WriteFile(candidate, append(b, '\n'), 0o600); err != nil {
			return nil, err
		}
		err := os.Link(candidate, path)
		_ = os.Remove(candidate)
		if err == nil {
			return func() { _ = os.Remove(path) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			if _, statErr := os.Stat(path); statErr != nil {
				return nil, err
			}
		}
		if staleLock(path) {
			_ = os.Remove(path)
			continue
		}
		if time.Now().After(deadline) {
			return nil, errors.New("timed out waiting for team write lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func staleLock(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var owner struct {
		PID        int    `json:"pid"`
		StartToken string `json:"start_token"`
	}
	if json.Unmarshal(b, &owner) == nil && owner.PID > 0 {
		return !runstate.ProcessMatches(runstate.Manifest{PID: owner.PID, ProcessStartToken: owner.StartToken})
	}
	info, err := os.Stat(path)
	return err == nil && time.Since(info.ModTime()) > time.Minute
}

func cloneTeam(t *Team) (*Team, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	var clone Team
	if err = json.Unmarshal(b, &clone); err != nil {
		return nil, err
	}
	return &clone, nil
}

func newID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}
