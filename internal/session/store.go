package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/secureledger"
)

var sessionIdentifier = regexp.MustCompile(`^agent_[a-f0-9]{24}$`)

type Store struct {
	ledger *secureledger.Ledger
}

func OpenStore(dir string) (*Store, error) {
	ledger, err := secureledger.Open(dir, secureledger.Options{
		Namespace: "sessions",
		ValidateID: func(id string) error {
			if !sessionIdentifier.MatchString(id) {
				return fmt.Errorf("invalid agent session id %q", id)
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return &Store{ledger: ledger}, nil
}

func (s *Store) Ensure(descriptor Descriptor) (*Session, error) {
	if strings.TrimSpace(descriptor.WorkflowID) == "" || strings.TrimSpace(descriptor.NodeID) == "" {
		return nil, errors.New("workflow and node identities are required")
	}
	id := stableID(descriptor.WorkflowID, descriptor.NodeID)
	var result *Session
	err := s.ledger.Update(id, func(tx *secureledger.Txn) error {
		existing, loadErr := replay(tx.Replay)
		if loadErr == nil {
			result = existing
			return nil
		}
		if !errors.Is(loadErr, os.ErrNotExist) {
			return loadErr
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
		if err := appendEvent(tx, Event{SessionID: id, Type: "session.created", At: now, Data: agent}); err != nil {
			return err
		}
		if err := snapshot(tx, agent); err != nil {
			return err
		}
		result = clone(agent)
		return nil
	})
	return result, err
}

func (s *Store) Queue(id, from, body string) (Message, error) {
	from, body = strings.TrimSpace(from), strings.TrimSpace(body)
	if from == "" || body == "" {
		return Message{}, errors.New("message sender and body are required")
	}
	if len(body) > 32<<10 {
		return Message{}, errors.New("agent message exceeds 32 KiB")
	}
	var queued Message
	err := s.mutate(id, func(tx *secureledger.Txn, agent *Session) error {
		now := time.Now().UTC()
		queued = Message{Sequence: uint64(len(agent.Inbox) + 1), From: from, Body: body, State: MessageQueued, CreatedAt: now}
		queued.ID = fmt.Sprintf("message-%d", queued.Sequence)
		if err := appendEvent(tx, Event{SessionID: id, Type: "message.queued", At: now, Data: queued}); err != nil {
			return err
		}
		applyMessageQueued(agent, queued, now)
		return nil
	})
	return queued, err
}

func (s *Store) Dispatch(id string, attempt int) ([]Message, error) {
	if attempt < 1 {
		return nil, errors.New("delivery attempt must be positive")
	}
	var dispatched []Message
	err := s.ledger.Update(id, func(tx *secureledger.Txn) error {
		agent, err := replay(tx.Replay)
		if err != nil {
			return err
		}
		deliveryAttempt := attempt
		for _, message := range agent.Inbox {
			if message.DeliveryAttempt >= deliveryAttempt {
				deliveryAttempt = message.DeliveryAttempt + 1
			}
		}
		for _, message := range agent.Inbox {
			if message.State == MessageQueued {
				message.State = MessageDispatched
				message.DeliveryAttempt = deliveryAttempt
				dispatched = append(dispatched, message)
			}
		}
		if len(dispatched) == 0 {
			return nil
		}
		now := time.Now().UTC()
		if err = appendEvent(tx, Event{SessionID: id, Type: "messages.dispatched", At: now, Data: deliveryEvent{Attempt: deliveryAttempt}}); err != nil {
			return err
		}
		applyMessagesDispatched(agent, deliveryAttempt, now)
		return snapshot(tx, agent)
	})
	return dispatched, err
}

func (s *Store) Deliver(id string, attempt int) error {
	return s.finishDelivery(id, attempt, "messages.delivered", applyMessagesDelivered)
}

func (s *Store) Requeue(id string, attempt int) error {
	return s.finishDelivery(id, attempt, "messages.requeued", applyMessagesRequeued)
}

func (s *Store) finishDelivery(id string, attempt int, eventType string, apply func(*Session, int, time.Time)) error {
	if attempt < 1 {
		return errors.New("delivery attempt must be positive")
	}
	return s.ledger.Update(id, func(tx *secureledger.Txn) error {
		agent, err := replay(tx.Replay)
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
		if err = appendEvent(tx, Event{SessionID: id, Type: eventType, At: now, Data: deliveryEvent{Attempt: attempt}}); err != nil {
			return err
		}
		apply(agent, attempt, now)
		return snapshot(tx, agent)
	})
}

func (s *Store) Observe(id string, observation Observation) error {
	if observation.LogicalState != "" && !validLogicalState(observation.LogicalState) {
		return fmt.Errorf("invalid logical agent state %q", observation.LogicalState)
	}
	if observation.ProcessState != "" && !validProcessState(observation.ProcessState) {
		return fmt.Errorf("invalid process state %q", observation.ProcessState)
	}
	return s.mutate(id, func(tx *secureledger.Txn, agent *Session) error {
		now := time.Now().UTC()
		if err := appendEvent(tx, Event{SessionID: id, Type: "session.observed", At: now, Data: observation}); err != nil {
			return err
		}
		applyObservation(agent, observation, now)
		return nil
	})
}

func (s *Store) mutate(id string, change func(*secureledger.Txn, *Session) error) error {
	return s.ledger.Update(id, func(tx *secureledger.Txn) error {
		agent, err := replay(tx.Replay)
		if err != nil {
			return err
		}
		if err = change(tx, agent); err != nil {
			return err
		}
		return snapshot(tx, agent)
	})
}

func (s *Store) Load(id string) (*Session, error) {
	var agent *Session
	err := s.ledger.View(id, func(_ uint64, raw []byte) error {
		return reduceRaw(&agent, raw)
	})
	if err != nil {
		return nil, err
	}
	if agent == nil {
		return nil, errors.New("agent session ledger has no creation event")
	}
	return clone(agent), nil
}

func (s *Store) LoadByNode(workflowID, nodeID string) (*Session, error) {
	if strings.TrimSpace(workflowID) == "" || strings.TrimSpace(nodeID) == "" {
		return nil, errors.New("workflow and node identities are required")
	}
	return s.Load(stableID(workflowID, nodeID))
}

func (s *Store) List() ([]*Session, error) {
	ids, err := s.ledger.IDs()
	if err != nil {
		return nil, err
	}
	out := make([]*Session, 0, len(ids))
	for _, id := range ids {
		agent, loadErr := s.Load(id)
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

type replayFn func(func(uint64, []byte) error) error

func replay(run replayFn) (*Session, error) {
	var agent *Session
	if err := run(func(_ uint64, raw []byte) error { return reduceRaw(&agent, raw) }); err != nil {
		return nil, err
	}
	if agent == nil {
		return nil, errors.New("agent session ledger has no creation event")
	}
	return clone(agent), nil
}

type rawEvent struct {
	Sequence  uint64          `json:"sequence"`
	SessionID string          `json:"session_id"`
	Type      string          `json:"type"`
	At        time.Time       `json:"at"`
	Data      json.RawMessage `json:"data"`
}

func reduceRaw(agent **Session, raw []byte) error {
	var event rawEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	switch event.Type {
	case "session.created":
		var created Session
		if err := json.Unmarshal(event.Data, &created); err != nil {
			return err
		}
		*agent = &created
	case "message.queued":
		if *agent == nil {
			return errors.New("message event preceded session creation")
		}
		var message Message
		if err := json.Unmarshal(event.Data, &message); err != nil {
			return err
		}
		applyMessageQueued(*agent, message, event.At)
	case "messages.dispatched", "messages.delivered", "messages.requeued":
		if *agent == nil {
			return errors.New("delivery event preceded session creation")
		}
		var delivery deliveryEvent
		if err := json.Unmarshal(event.Data, &delivery); err != nil {
			return err
		}
		switch event.Type {
		case "messages.dispatched":
			applyMessagesDispatched(*agent, delivery.Attempt, event.At)
		case "messages.delivered":
			applyMessagesDelivered(*agent, delivery.Attempt, event.At)
		case "messages.requeued":
			applyMessagesRequeued(*agent, delivery.Attempt, event.At)
		}
	case "session.observed":
		if *agent == nil {
			return errors.New("observation event preceded session creation")
		}
		var observation Observation
		if err := json.Unmarshal(event.Data, &observation); err != nil {
			return err
		}
		applyObservation(*agent, observation, event.At)
	}
	return nil
}

func appendEvent(tx *secureledger.Txn, event Event) error {
	_, err := tx.Append(func(next uint64) ([]byte, error) {
		event.Sequence = next
		return json.Marshal(event)
	})
	return err
}

func snapshot(tx *secureledger.Txn, agent *Session) error {
	raw, err := json.MarshalIndent(agent, "", "  ")
	if err != nil {
		return err
	}
	return tx.ReplaceSnapshot(append(raw, '\n'))
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

func stableID(workflowID, nodeID string) string {
	sum := sha256.Sum256([]byte(workflowID + "\x00" + nodeID))
	return "agent_" + hex.EncodeToString(sum[:12])
}

func clone(agent *Session) *Session {
	copy := *agent
	copy.Inbox = append([]Message{}, agent.Inbox...)
	return &copy
}
