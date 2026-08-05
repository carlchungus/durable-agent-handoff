package session

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
)

var sessionIdentifier = regexp.MustCompile(`^agent_[a-f0-9]{24}$`)

type Store struct {
	dir         string
	mu          sync.Mutex
	newFileLock func(*os.File) sessionFileLock
	lockTimeout time.Duration
	lockRetry   time.Duration
	now         func() time.Time
	sleep       func(time.Duration)
}

const (
	defaultLockTimeout = 10 * time.Second
	defaultLockRetry   = 10 * time.Millisecond
)

type sessionFileLock interface {
	TryLock() (bool, error)
	Unlock() error
}

func OpenStore(dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("session store directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	store := &Store{
		dir:         dir,
		newFileLock: newPlatformFileLock,
		lockTimeout: defaultLockTimeout,
		lockRetry:   defaultLockRetry,
		now:         time.Now,
		sleep:       time.Sleep,
	}
	root, err := store.openRoot()
	if err != nil {
		return nil, err
	}
	defer root.Close()
	sessions, err := openChildRoot(root, "sessions", true)
	if err != nil {
		return nil, err
	}
	_ = sessions.Close()
	return store, nil
}

func (s *Store) openRoot() (*os.Root, error) {
	before, err := os.Lstat(s.dir)
	if err != nil {
		return nil, err
	}
	if !actualDirectory(before) {
		return nil, fmt.Errorf("session store root %q is not an actual directory", s.dir)
	}
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return nil, err
	}
	after, pathErr := os.Lstat(s.dir)
	opened, openedErr := root.Stat(".")
	if pathErr != nil || openedErr != nil || !actualDirectory(after) || !actualDirectory(opened) || !os.SameFile(after, opened) {
		_ = root.Close()
		if pathErr != nil {
			return nil, pathErr
		}
		if openedErr != nil {
			return nil, openedErr
		}
		return nil, fmt.Errorf("session store root %q changed while opening", s.dir)
	}
	return root, nil
}

func (s *Store) openSessionsRoot() (*os.Root, error) {
	root, err := s.openRoot()
	if err != nil {
		return nil, err
	}
	sessions, err := openChildRoot(root, "sessions", true)
	_ = root.Close()
	return sessions, err
}

func (s *Store) openSessionRoot(id string, create bool) (*os.Root, error) {
	if !sessionIdentifier.MatchString(id) {
		return nil, fmt.Errorf("invalid agent session id %q", id)
	}
	sessions, err := s.openSessionsRoot()
	if err != nil {
		return nil, err
	}
	session, err := openChildRoot(sessions, id, create)
	_ = sessions.Close()
	return session, err
}

func openChildRoot(parent *os.Root, name string, create bool) (*os.Root, error) {
	if create {
		if err := parent.Mkdir(name, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	before, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !actualDirectory(before) {
		return nil, fmt.Errorf("session storage component %q is not an actual directory", name)
	}
	child, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	after, pathErr := parent.Lstat(name)
	opened, openedErr := child.Stat(".")
	if pathErr != nil || openedErr != nil || !actualDirectory(after) || !actualDirectory(opened) || !os.SameFile(after, opened) {
		_ = child.Close()
		if pathErr != nil {
			return nil, pathErr
		}
		if openedErr != nil {
			return nil, openedErr
		}
		return nil, fmt.Errorf("session storage component %q changed while opening", name)
	}
	return child, nil
}

func actualDirectory(info os.FileInfo) bool {
	return info != nil && info.Mode().Type() == os.ModeDir
}

func openRegular(root *os.Root, name string, flag int, perm os.FileMode) (*os.File, error) {
	before, err := root.Lstat(name)
	if err == nil {
		if !before.Mode().IsRegular() {
			return nil, fmt.Errorf("session storage file %q is not a regular file", name)
		}
	} else if !errors.Is(err, os.ErrNotExist) || flag&os.O_CREATE == 0 {
		return nil, err
	}
	file, err := root.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	after, pathErr := root.Lstat(name)
	opened, openedErr := file.Stat()
	safetyErr := validateRegularFile(file)
	if pathErr != nil || openedErr != nil || safetyErr != nil || !after.Mode().IsRegular() || !opened.Mode().IsRegular() || !os.SameFile(after, opened) {
		_ = file.Close()
		if pathErr != nil {
			return nil, pathErr
		}
		if openedErr != nil {
			return nil, openedErr
		}
		if safetyErr != nil {
			return nil, fmt.Errorf("unsafe session storage file %q: %w", name, safetyErr)
		}
		return nil, fmt.Errorf("session storage file %q changed while opening", name)
	}
	return file, nil
}

func (s *Store) Ensure(descriptor Descriptor) (*Session, error) {
	if strings.TrimSpace(descriptor.WorkflowID) == "" || strings.TrimSpace(descriptor.NodeID) == "" {
		return nil, errors.New("workflow and node identities are required")
	}
	id := stableID(descriptor.WorkflowID, descriptor.NodeID)
	s.mu.Lock()
	defer s.mu.Unlock()
	root, release, err := s.acquire(id)
	if err != nil {
		return nil, err
	}
	defer release()
	if existing, loadErr := s.loadLocked(root, id); loadErr == nil {
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
	if err = s.appendLocked(root, id, Event{SessionID: id, Type: "session.created", At: now, Data: agent}); err != nil {
		return nil, err
	}
	if err = s.snapshotLocked(root, agent); err != nil {
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
	root, release, err := s.acquire(id)
	if err != nil {
		return Message{}, err
	}
	defer release()
	agent, err := s.loadLocked(root, id)
	if err != nil {
		return Message{}, err
	}
	now := time.Now().UTC()
	message := Message{Sequence: uint64(len(agent.Inbox) + 1), From: from, Body: body, State: MessageQueued, CreatedAt: now}
	message.ID = fmt.Sprintf("message-%d", message.Sequence)
	if err = s.appendLocked(root, id, Event{SessionID: id, Type: "message.queued", At: now, Data: message}); err != nil {
		return Message{}, err
	}
	applyMessageQueued(agent, message, now)
	if err = s.snapshotLocked(root, agent); err != nil {
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
	root, release, err := s.acquire(id)
	if err != nil {
		return nil, err
	}
	defer release()
	agent, err := s.loadLocked(root, id)
	if err != nil {
		return nil, err
	}
	// Node retry counters may be refunded after a provider limit. Delivery
	// attempts never rewind: they fence one inbox batch independently.
	deliveryAttempt := attempt
	for _, message := range agent.Inbox {
		if message.DeliveryAttempt >= deliveryAttempt {
			deliveryAttempt = message.DeliveryAttempt + 1
		}
	}
	var dispatched []Message
	for _, message := range agent.Inbox {
		if message.State == MessageQueued {
			message.State = MessageDispatched
			message.DeliveryAttempt = deliveryAttempt
			dispatched = append(dispatched, message)
		}
	}
	if len(dispatched) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	data := deliveryEvent{Attempt: deliveryAttempt}
	if err = s.appendLocked(root, id, Event{SessionID: id, Type: "messages.dispatched", At: now, Data: data}); err != nil {
		return nil, err
	}
	applyMessagesDispatched(agent, deliveryAttempt, now)
	if err = s.snapshotLocked(root, agent); err != nil {
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
	root, release, err := s.acquire(id)
	if err != nil {
		return err
	}
	defer release()
	agent, err := s.loadLocked(root, id)
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
	if err = s.appendLocked(root, id, Event{SessionID: id, Type: "messages.delivered", At: now, Data: data}); err != nil {
		return err
	}
	applyMessagesDelivered(agent, attempt, now)
	return s.snapshotLocked(root, agent)
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
	root, release, err := s.acquire(id)
	if err != nil {
		return err
	}
	defer release()
	agent, err := s.loadLocked(root, id)
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
	if err = s.appendLocked(root, id, Event{SessionID: id, Type: "messages.requeued", At: now, Data: data}); err != nil {
		return err
	}
	applyMessagesRequeued(agent, attempt, now)
	return s.snapshotLocked(root, agent)
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
	root, release, err := s.acquire(id)
	if err != nil {
		return err
	}
	defer release()
	agent, err := s.loadLocked(root, id)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err = s.appendLocked(root, id, Event{SessionID: id, Type: "session.observed", At: now, Data: observation}); err != nil {
		return err
	}
	applyObservation(agent, observation, now)
	return s.snapshotLocked(root, agent)
}

func (s *Store) Load(id string) (*Session, error) {
	if !sessionIdentifier.MatchString(id) {
		return nil, fmt.Errorf("invalid agent session id %q", id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	root, err := s.openSessionRoot(id, false)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return s.loadLocked(root, id)
}

func (s *Store) LoadByNode(workflowID, nodeID string) (*Session, error) {
	if strings.TrimSpace(workflowID) == "" || strings.TrimSpace(nodeID) == "" {
		return nil, errors.New("workflow and node identities are required")
	}
	return s.Load(stableID(workflowID, nodeID))
}

func (s *Store) List() ([]*Session, error) {
	sessions, err := s.openSessionsRoot()
	if err != nil {
		return nil, err
	}
	defer sessions.Close()
	directory, err := sessions.Open(".")
	if err != nil {
		return nil, err
	}
	entries, err := directory.ReadDir(-1)
	_ = directory.Close()
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

func (s *Store) loadLocked(root *os.Root, id string) (*Session, error) {
	// The snapshot is only a replaceable view. The fsynced ledger is always
	// authoritative, including events appended just before a process crash.
	return s.replayLocked(root, id)
}

func (s *Store) replayLocked(root *os.Root, id string) (*Session, error) {
	var agent *Session
	file, err := openRegular(root, "events.jsonl", os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open agent session ledger: %w", err)
	}
	defer file.Close()
	_, err = walkLedger(file, true, func(raw ledgerEvent) error {
		switch raw.Type {
		case "session.created":
			var created Session
			if decodeErr := json.Unmarshal(raw.Data, &created); decodeErr != nil {
				return decodeErr
			}
			agent = &created
		case "message.queued":
			if agent == nil {
				return errors.New("message event preceded session creation")
			}
			var message Message
			if decodeErr := json.Unmarshal(raw.Data, &message); decodeErr != nil {
				return decodeErr
			}
			applyMessageQueued(agent, message, raw.At)
		case "messages.dispatched":
			if agent == nil {
				return errors.New("delivery event preceded session creation")
			}
			var delivery deliveryEvent
			if decodeErr := json.Unmarshal(raw.Data, &delivery); decodeErr != nil {
				return decodeErr
			}
			applyMessagesDispatched(agent, delivery.Attempt, raw.At)
		case "messages.delivered":
			if agent == nil {
				return errors.New("delivery event preceded session creation")
			}
			var delivery deliveryEvent
			if decodeErr := json.Unmarshal(raw.Data, &delivery); decodeErr != nil {
				return decodeErr
			}
			applyMessagesDelivered(agent, delivery.Attempt, raw.At)
		case "messages.requeued":
			if agent == nil {
				return errors.New("delivery event preceded session creation")
			}
			var delivery deliveryEvent
			if decodeErr := json.Unmarshal(raw.Data, &delivery); decodeErr != nil {
				return decodeErr
			}
			applyMessagesRequeued(agent, delivery.Attempt, raw.At)
		case "session.observed":
			if agent == nil {
				return errors.New("observation event preceded session creation")
			}
			var observation Observation
			if decodeErr := json.Unmarshal(raw.Data, &observation); decodeErr != nil {
				return decodeErr
			}
			applyObservation(agent, observation, raw.At)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("replay agent session ledger: %w", err)
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

func (s *Store) appendLocked(root *os.Root, id string, event Event) (err error) {
	f, err := openRegular(root, "events.jsonl", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	sequence, err := repairLedgerTail(f)
	if err != nil {
		return err
	}
	event.Sequence = sequence + 1
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	b, err := json.Marshal(event)
	if err == nil {
		_, err = f.Write(append(b, '\n'))
	}
	if syncErr := f.Sync(); err == nil {
		err = syncErr
	}
	return err
}

func (s *Store) snapshotLocked(root *os.Root, agent *Session) error {
	b, err := json.MarshalIndent(agent, "", "  ")
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf(".state.json.tmp-%d-%d", os.Getpid(), s.now().UnixNano())
	f, err := openRegular(root, tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer root.Remove(tmp)
	if _, err = f.Write(append(b, '\n')); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = root.Rename(tmp, "state.json"); err != nil {
		return err
	}
	if d, openErr := root.Open("."); openErr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (s *Store) acquire(id string) (*os.Root, func(), error) {
	root, err := s.openSessionRoot(id, true)
	if err != nil {
		return nil, nil, err
	}
	file, err := openRegular(root, ".write.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	lock := s.newFileLock(file)
	deadline := s.now().Add(s.lockTimeout)
	firstAttempt := true
	for {
		if !firstAttempt && !s.now().Before(deadline) {
			_ = file.Close()
			_ = root.Close()
			return nil, nil, fmt.Errorf("timed out waiting for agent session %s lock", id)
		}
		firstAttempt = false
		locked, lockErr := lock.TryLock()
		if lockErr != nil {
			_ = file.Close()
			_ = root.Close()
			return nil, nil, fmt.Errorf("lock agent session %s: %w", id, lockErr)
		}
		if locked {
			owner := sessionLockOwner{
				PID:        os.Getpid(),
				StartToken: runstate.ProcessStartToken(os.Getpid()),
				LeaseID:    fmt.Sprintf("%d-%d", os.Getpid(), s.now().UnixNano()),
				State:      sessionLockActive,
				UpdatedAt:  s.now().UTC(),
			}
			if writeErr := writeLockOwner(file, owner); writeErr != nil {
				_ = lock.Unlock()
				_ = file.Close()
				_ = root.Close()
				return nil, nil, writeErr
			}
			var once sync.Once
			return root, func() {
				once.Do(func() {
					owner.State = sessionLockReleased
					owner.UpdatedAt = s.now().UTC()
					_ = writeLockOwner(file, owner)
					_ = lock.Unlock()
					_ = file.Close()
					_ = root.Close()
				})
			}, nil
		}
		remaining := deadline.Sub(s.now())
		if remaining <= 0 {
			continue
		}
		backoff := s.lockRetry
		if backoff > remaining {
			backoff = remaining
		}
		s.sleep(backoff)
	}
}

const (
	sessionLockActive   = "active"
	sessionLockReleased = "released"
)

type sessionLockOwner struct {
	PID        int       `json:"pid"`
	StartToken string    `json:"start_token,omitempty"`
	LeaseID    string    `json:"lease_id"`
	State      string    `json:"state"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func writeLockOwner(file *os.File, owner sessionLockOwner) error {
	b, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	if err = file.Truncate(0); err != nil {
		return err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = file.Write(append(b, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

type ledgerEvent struct {
	Sequence  uint64          `json:"sequence"`
	SessionID string          `json:"session_id"`
	Type      string          `json:"type"`
	At        time.Time       `json:"at"`
	Data      json.RawMessage `json:"data"`
}

const maxLedgerEventBytes = 4 << 20

func walkLedger(file *os.File, toleratePartialTail bool, visit func(ledgerEvent) error) (uint64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReaderSize(file, 64<<10)
	var sequence uint64
	for {
		line, readErr := readLedgerLine(reader)
		if len(line) > 0 {
			complete := line[len(line)-1] == '\n'
			line = bytes.TrimSpace(line)
			if len(line) > 0 {
				var event ledgerEvent
				decodeErr := json.Unmarshal(line, &event)
				if decodeErr != nil && !complete && errors.Is(readErr, io.EOF) && toleratePartialTail {
					return sequence, nil
				}
				if decodeErr != nil {
					return 0, decodeErr
				}
				if event.Sequence != sequence+1 {
					return 0, fmt.Errorf("event sequence %d followed %d", event.Sequence, sequence)
				}
				if visitErr := visit(event); visitErr != nil {
					return 0, visitErr
				}
				sequence = event.Sequence
			}
		}
		if errors.Is(readErr, io.EOF) {
			return sequence, nil
		}
		if readErr != nil {
			return 0, readErr
		}
	}
}

func repairLedgerTail(file *os.File) (uint64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReaderSize(file, 64<<10)
	var sequence uint64
	var goodOffset int64
	for {
		line, readErr := readLedgerLine(reader)
		if len(line) > 0 {
			complete := line[len(line)-1] == '\n'
			var event ledgerEvent
			decodeErr := json.Unmarshal(bytes.TrimSpace(line), &event)
			if decodeErr != nil && !complete && errors.Is(readErr, io.EOF) {
				truncateErr := file.Truncate(goodOffset)
				if truncateErr == nil {
					truncateErr = file.Sync()
				}
				return sequence, truncateErr
			}
			if decodeErr != nil {
				return 0, decodeErr
			}
			if event.Sequence != sequence+1 {
				return 0, fmt.Errorf("event sequence %d followed %d", event.Sequence, sequence)
			}
			sequence = event.Sequence
			goodOffset += int64(len(line))
			if !complete && errors.Is(readErr, io.EOF) {
				_, writeErr := file.WriteAt([]byte{'\n'}, goodOffset)
				if writeErr == nil {
					writeErr = file.Sync()
				}
				return sequence, writeErr
			}
		}
		if errors.Is(readErr, io.EOF) {
			return sequence, nil
		}
		if readErr != nil {
			return 0, readErr
		}
	}
}

func readLedgerLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > maxLedgerEventBytes {
			return nil, errors.New("agent session ledger event exceeds 4 MiB")
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
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
