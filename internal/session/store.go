package session

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
)

var sessionIdentifier = regexp.MustCompile(`^agent_[a-f0-9]{24}$`)

type Store struct {
	dir string
	mu  sync.Mutex
}

func OpenStore(dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("session store directory is required")
	}
	if err := os.MkdirAll(filepath.Join(dir, "sessions"), 0o700); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Ensure(descriptor Descriptor) (*Session, error) {
	if strings.TrimSpace(descriptor.WorkflowID) == "" || strings.TrimSpace(descriptor.NodeID) == "" {
		return nil, errors.New("workflow and node identities are required")
	}
	id := stableID(descriptor.WorkflowID, descriptor.NodeID)
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquire(id)
	if err != nil {
		return nil, err
	}
	defer release()
	if existing, loadErr := s.loadLocked(id); loadErr == nil {
		return existing, nil
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return nil, loadErr
	}
	now := time.Now().UTC()
	logical := descriptor.LogicalState
	if logical == "" {
		logical = LogicalWorking
	}
	process := descriptor.ProcessState
	if process == "" {
		process = ProcessExited
	}
	name := strings.TrimSpace(descriptor.Name)
	if name == "" {
		name = descriptor.NodeID
	}
	agent := &Session{
		Version:          Version,
		ID:               id,
		WorkflowID:       descriptor.WorkflowID,
		NodeID:           descriptor.NodeID,
		ParentAgentID:    descriptor.ParentAgentID,
		Name:             name,
		Runtime:          descriptor.Runtime,
		RuntimeSessionID: descriptor.RuntimeSessionID,
		Worktree:         descriptor.Worktree,
		LogicalState:     logical,
		ProcessState:     process,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err = s.appendLocked(id, Event{SessionID: id, Type: "session.created", At: now, Data: agent}); err != nil {
		return nil, err
	}
	if err = s.snapshotLocked(agent); err != nil {
		return nil, err
	}
	return clone(agent), nil
}

func (s *Store) Queue(id, from, body string) (Message, error) {
	if !sessionIdentifier.MatchString(id) {
		return Message{}, fmt.Errorf("invalid agent session id %q", id)
	}
	from, body = strings.TrimSpace(from), strings.TrimSpace(body)
	if from == "" || body == "" {
		return Message{}, errors.New("message sender and body are required")
	}
	if len(body) > 32<<10 {
		return Message{}, errors.New("agent message exceeds 32 KiB")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquire(id)
	if err != nil {
		return Message{}, err
	}
	defer release()
	agent, err := s.loadLocked(id)
	if err != nil {
		return Message{}, err
	}
	now := time.Now().UTC()
	message := Message{Sequence: uint64(len(agent.Inbox) + 1), From: from, Body: body, State: MessageQueued, CreatedAt: now}
	message.ID = fmt.Sprintf("message-%d", message.Sequence)
	if err = s.appendLocked(id, Event{SessionID: id, Type: "message.queued", At: now, Data: message}); err != nil {
		return Message{}, err
	}
	applyMessageQueued(agent, message, now)
	if err = s.snapshotLocked(agent); err != nil {
		return Message{}, err
	}
	return message, nil
}

func (s *Store) Dispatch(id string, attempt int) ([]Message, error) {
	if !sessionIdentifier.MatchString(id) {
		return nil, fmt.Errorf("invalid agent session id %q", id)
	}
	if attempt < 1 {
		return nil, errors.New("delivery attempt must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquire(id)
	if err != nil {
		return nil, err
	}
	defer release()
	agent, err := s.loadLocked(id)
	if err != nil {
		return nil, err
	}
	var dispatched []Message
	for _, message := range agent.Inbox {
		if message.State == MessageQueued {
			message.State = MessageDispatched
			message.DeliveryAttempt = attempt
			dispatched = append(dispatched, message)
		}
	}
	if len(dispatched) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	data := deliveryEvent{Attempt: attempt}
	if err = s.appendLocked(id, Event{SessionID: id, Type: "messages.dispatched", At: now, Data: data}); err != nil {
		return nil, err
	}
	applyMessagesDispatched(agent, attempt, now)
	if err = s.snapshotLocked(agent); err != nil {
		return nil, err
	}
	return dispatched, nil
}

func (s *Store) Deliver(id string, attempt int) error {
	if !sessionIdentifier.MatchString(id) {
		return fmt.Errorf("invalid agent session id %q", id)
	}
	if attempt < 1 {
		return errors.New("delivery attempt must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquire(id)
	if err != nil {
		return err
	}
	defer release()
	agent, err := s.loadLocked(id)
	if err != nil {
		return err
	}
	matched := false
	for _, message := range agent.Inbox {
		if message.State == MessageDispatched && message.DeliveryAttempt == attempt {
			matched = true
			break
		}
	}
	if !matched {
		return fmt.Errorf("no messages are dispatched to attempt %d", attempt)
	}
	now := time.Now().UTC()
	data := deliveryEvent{Attempt: attempt}
	if err = s.appendLocked(id, Event{SessionID: id, Type: "messages.delivered", At: now, Data: data}); err != nil {
		return err
	}
	applyMessagesDelivered(agent, attempt, now)
	return s.snapshotLocked(agent)
}

func (s *Store) Requeue(id string, attempt int) error {
	if !sessionIdentifier.MatchString(id) {
		return fmt.Errorf("invalid agent session id %q", id)
	}
	if attempt < 1 {
		return errors.New("delivery attempt must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquire(id)
	if err != nil {
		return err
	}
	defer release()
	agent, err := s.loadLocked(id)
	if err != nil {
		return err
	}
	matched := false
	for _, message := range agent.Inbox {
		if message.State == MessageDispatched && message.DeliveryAttempt == attempt {
			matched = true
			break
		}
	}
	if !matched {
		return fmt.Errorf("no messages are dispatched to attempt %d", attempt)
	}
	now := time.Now().UTC()
	data := deliveryEvent{Attempt: attempt}
	if err = s.appendLocked(id, Event{SessionID: id, Type: "messages.requeued", At: now, Data: data}); err != nil {
		return err
	}
	applyMessagesRequeued(agent, attempt, now)
	return s.snapshotLocked(agent)
}

func (s *Store) Observe(id string, observation Observation) error {
	if !sessionIdentifier.MatchString(id) {
		return fmt.Errorf("invalid agent session id %q", id)
	}
	if observation.LogicalState != "" && !validLogicalState(observation.LogicalState) {
		return fmt.Errorf("invalid logical agent state %q", observation.LogicalState)
	}
	if observation.ProcessState != "" && !validProcessState(observation.ProcessState) {
		return fmt.Errorf("invalid process state %q", observation.ProcessState)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.acquire(id)
	if err != nil {
		return err
	}
	defer release()
	agent, err := s.loadLocked(id)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err = s.appendLocked(id, Event{SessionID: id, Type: "session.observed", At: now, Data: observation}); err != nil {
		return err
	}
	applyObservation(agent, observation, now)
	return s.snapshotLocked(agent)
}

func (s *Store) Load(id string) (*Session, error) {
	if !sessionIdentifier.MatchString(id) {
		return nil, fmt.Errorf("invalid agent session id %q", id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(id)
}

func (s *Store) LoadByNode(workflowID, nodeID string) (*Session, error) {
	if strings.TrimSpace(workflowID) == "" || strings.TrimSpace(nodeID) == "" {
		return nil, errors.New("workflow and node identities are required")
	}
	return s.Load(stableID(workflowID, nodeID))
}

func (s *Store) List() ([]*Session, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "sessions"))
	if err != nil {
		return nil, err
	}
	out := make([]*Session, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !sessionIdentifier.MatchString(entry.Name()) {
			continue
		}
		agent, loadErr := s.Load(entry.Name())
		if loadErr == nil {
			out = append(out, agent)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

func (s *Store) loadLocked(id string) (*Session, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, "sessions", id, "state.json"))
	if err == nil {
		var agent Session
		if json.Unmarshal(b, &agent) == nil && agent.Version == Version && agent.ID == id {
			return clone(&agent), nil
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return s.replayLocked(id)
}

func (s *Store) replayLocked(id string) (*Session, error) {
	f, err := os.Open(filepath.Join(s.dir, "sessions", id, "events.jsonl"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var agent *Session
	dec := json.NewDecoder(f)
	for {
		var raw struct {
			Type string          `json:"type"`
			At   time.Time       `json:"at"`
			Data json.RawMessage `json:"data"`
		}
		if err = dec.Decode(&raw); errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("replay agent session ledger: %w", err)
		}
		switch raw.Type {
		case "session.created":
			var created Session
			if err = json.Unmarshal(raw.Data, &created); err != nil {
				return nil, err
			}
			agent = &created
		case "message.queued":
			if agent == nil {
				return nil, errors.New("message event preceded session creation")
			}
			var message Message
			if err = json.Unmarshal(raw.Data, &message); err != nil {
				return nil, err
			}
			applyMessageQueued(agent, message, raw.At)
		case "messages.dispatched":
			if agent == nil {
				return nil, errors.New("delivery event preceded session creation")
			}
			var delivery deliveryEvent
			if err = json.Unmarshal(raw.Data, &delivery); err != nil {
				return nil, err
			}
			applyMessagesDispatched(agent, delivery.Attempt, raw.At)
		case "messages.delivered":
			if agent == nil {
				return nil, errors.New("delivery event preceded session creation")
			}
			var delivery deliveryEvent
			if err = json.Unmarshal(raw.Data, &delivery); err != nil {
				return nil, err
			}
			applyMessagesDelivered(agent, delivery.Attempt, raw.At)
		case "messages.requeued":
			if agent == nil {
				return nil, errors.New("delivery event preceded session creation")
			}
			var delivery deliveryEvent
			if err = json.Unmarshal(raw.Data, &delivery); err != nil {
				return nil, err
			}
			applyMessagesRequeued(agent, delivery.Attempt, raw.At)
		case "session.observed":
			if agent == nil {
				return nil, errors.New("observation event preceded session creation")
			}
			var observation Observation
			if err = json.Unmarshal(raw.Data, &observation); err != nil {
				return nil, err
			}
			applyObservation(agent, observation, raw.At)
		}
	}
	if agent == nil {
		return nil, errors.New("agent session ledger has no creation event")
	}
	return clone(agent), nil
}

func applyMessageQueued(agent *Session, message Message, at time.Time) {
	agent.Inbox = append(agent.Inbox, message)
	agent.UpdatedAt = at
}

type deliveryEvent struct {
	Attempt int `json:"attempt"`
}

func applyMessagesDispatched(agent *Session, attempt int, at time.Time) {
	for i := range agent.Inbox {
		if agent.Inbox[i].State == MessageQueued {
			agent.Inbox[i].State = MessageDispatched
			agent.Inbox[i].DeliveryAttempt = attempt
		}
	}
	agent.UpdatedAt = at
}

func applyMessagesDelivered(agent *Session, attempt int, at time.Time) {
	for i := range agent.Inbox {
		if agent.Inbox[i].State == MessageDispatched && agent.Inbox[i].DeliveryAttempt == attempt {
			agent.Inbox[i].State = MessageDelivered
			agent.Inbox[i].DeliveredAt = at
		}
	}
	agent.UpdatedAt = at
}

func applyMessagesRequeued(agent *Session, attempt int, at time.Time) {
	for i := range agent.Inbox {
		if agent.Inbox[i].State == MessageDispatched && agent.Inbox[i].DeliveryAttempt == attempt {
			agent.Inbox[i].State = MessageQueued
			agent.Inbox[i].DeliveryAttempt = 0
		}
	}
	agent.UpdatedAt = at
}

func applyObservation(agent *Session, observation Observation, at time.Time) {
	if observation.Runtime != "" {
		agent.Runtime = observation.Runtime
	}
	if observation.RuntimeSessionID != "" {
		agent.RuntimeSessionID = observation.RuntimeSessionID
	}
	if observation.Worktree != "" {
		agent.Worktree = observation.Worktree
	}
	if observation.LogicalState != "" {
		agent.LogicalState = observation.LogicalState
	}
	if observation.ProcessState != "" {
		agent.ProcessState = observation.ProcessState
	}
	agent.UpdatedAt = at
}

func validLogicalState(state LogicalState) bool {
	return state == LogicalWorking || state == LogicalNeedsInput || state == LogicalCompleted || state == LogicalStopped
}

func validProcessState(state ProcessState) bool {
	return state == ProcessStarting || state == ProcessRunning || state == ProcessExited
}

func (s *Store) appendLocked(id string, event Event) error {
	dir := filepath.Join(s.dir, "sessions", id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "events.jsonl")
	sequence, err := lastSequence(path)
	if err != nil {
		return err
	}
	event.Sequence = sequence + 1
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

func (s *Store) snapshotLocked(agent *Session) error {
	dir := filepath.Join(s.dir, "sessions", agent.ID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(agent, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "state.json.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(dir, "state.json")); err != nil {
		return err
	}
	if d, openErr := os.Open(dir); openErr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (s *Store) acquire(id string) (func(), error) {
	dir := filepath.Join(s.dir, "sessions", id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, ".write.lock")
	deadline := time.Now().Add(10 * time.Second)
	for {
		owner := struct {
			PID        int    `json:"pid"`
			StartToken string `json:"start_token,omitempty"`
		}{PID: os.Getpid(), StartToken: runstate.ProcessStartToken(os.Getpid())}
		candidate := fmt.Sprintf("%s.%d.%d", path, os.Getpid(), time.Now().UnixNano())
		b, _ := json.Marshal(owner)
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
			return nil, fmt.Errorf("timed out waiting for agent session %s lock", id)
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
	info, statErr := os.Stat(path)
	return statErr == nil && time.Since(info.ModTime()) > time.Minute
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
	var sequence uint64
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 4<<20)
	for scan.Scan() {
		var event Event
		if err = json.Unmarshal(scan.Bytes(), &event); err != nil {
			return 0, err
		}
		sequence = event.Sequence
	}
	return sequence, scan.Err()
}

func stableID(workflowID, nodeID string) string {
	sum := sha256.Sum256([]byte(workflowID + "\x00" + nodeID))
	return "agent_" + hex.EncodeToString(sum[:12])
}

func clone(agent *Session) *Session {
	copy := *agent
	copy.Inbox = append([]Message{}, agent.Inbox...)
	return &copy
}
