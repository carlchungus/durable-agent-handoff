package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/secureledger"
)

const canonicalRecord = "canonical"

var commandKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{7,255}$`)

type Boundary string

const (
	BoundaryAfterValidation Boundary = "after_validation"
	BoundaryAfterAppend     Boundary = "after_append"
	BoundaryAfterSnapshot   Boundary = "after_snapshot"
)

type Options struct {
	Now   func() time.Time
	Fault func(Boundary) error
}

type Store struct {
	ledger *secureledger.Ledger
	root   string
	now    func() time.Time
	fault  func(Boundary) error
}

func Open(root string, options Options) (*Store, error) {
	ledger, err := secureledger.Open(root, secureledger.Options{
		Namespace:      "supervisor-v2",
		MaxRecordBytes: 16 << 20,
		ValidateID: func(id string) error {
			if id != canonicalRecord {
				return fmt.Errorf("invalid supervisor journal id %q", id)
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	canonicalRoot, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Store{ledger: ledger, root: filepath.Clean(canonicalRoot), now: options.Now, fault: options.Fault}, nil
}

type DomainEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type JournalEntry struct {
	SchemaVersion  int           `json:"schema_version"`
	Sequence       uint64        `json:"sequence"`
	CommandType    string        `json:"command_type"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	InputDigest    string        `json:"input_digest"`
	ResourceID     string        `json:"resource_id,omitempty"`
	At             time.Time     `json:"at"`
	Events         []DomainEvent `json:"events"`
}

type Receipt struct {
	Sequence   uint64 `json:"sequence"`
	ResourceID string `json:"resource_id,omitempty"`
	Existing   bool   `json:"existing"`
}

type command interface {
	commandType() string
	idempotencyKey() string
	digest() (string, error)
	decide(*State, time.Time) ([]DomainEvent, string, error)
}

// Execute is the only state-changing interface. It validates a command and all
// of its events against a cloned projection before one journal append.
func (s *Store) Execute(ctx context.Context, cmd command) (Receipt, error) {
	if s == nil || s.ledger == nil {
		return Receipt{}, errors.New("supervisor store is required")
	}
	if cmd == nil {
		return Receipt{}, errors.New("supervisor command is required")
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	key := cmd.idempotencyKey()
	if !commandKeyPattern.MatchString(key) {
		return Receipt{}, fmt.Errorf("invalid idempotency key %q", key)
	}
	digest, err := cmd.digest()
	if err != nil {
		return Receipt{}, err
	}
	var receipt Receipt
	err = s.ledger.Update(canonicalRecord, func(tx *secureledger.Txn) error {
		state, replayErr := replayTxn(tx)
		if replayErr != nil && !errors.Is(replayErr, os.ErrNotExist) {
			return replayErr
		}
		if state == nil {
			state = emptyState()
		}
		if prior, ok := state.Idempotency[key]; ok {
			if prior.CommandType != cmd.commandType() || prior.InputDigest != digest {
				return fmt.Errorf("%w: key %q already names %s with digest %s", ErrIdempotencyConflict, key, prior.CommandType, prior.InputDigest)
			}
			receipt = Receipt{Sequence: prior.Sequence, ResourceID: prior.ResourceID, Existing: true}
			return nil
		}
		now := s.now().UTC()
		events, resourceID, decideErr := cmd.decide(state, now)
		if decideErr != nil {
			return decideErr
		}
		if len(events) == 0 {
			return errors.New("supervisor command produced no events")
		}
		entry := JournalEntry{SchemaVersion: SchemaVersion, CommandType: cmd.commandType(), IdempotencyKey: key, InputDigest: digest, ResourceID: resourceID, At: now, Events: events}
		clone, cloneErr := cloneState(state)
		if cloneErr != nil {
			return cloneErr
		}
		// Sequence-independent validation is intentionally completed before the
		// journal is touched.
		if cloneErr = applyEntry(clone, entry); cloneErr != nil {
			return cloneErr
		}
		if cloneErr = validateState(clone); cloneErr != nil {
			return cloneErr
		}
		if faultErr := s.inject(BoundaryAfterValidation); faultErr != nil {
			return faultErr
		}
		sequence, appendErr := tx.Append(func(next uint64) ([]byte, error) {
			entry.Sequence = next
			return json.Marshal(entry)
		})
		if appendErr != nil {
			return appendErr
		}
		entry.Sequence = sequence
		if faultErr := s.inject(BoundaryAfterAppend); faultErr != nil {
			return faultErr
		}
		if err := applyEntry(state, entry); err != nil {
			return err
		}
		if err := snapshot(tx, state); err != nil {
			return err
		}
		if faultErr := s.inject(BoundaryAfterSnapshot); faultErr != nil {
			return faultErr
		}
		receipt = Receipt{Sequence: sequence, ResourceID: resourceID}
		return nil
	})
	return receipt, err
}

func (s *Store) inject(boundary Boundary) error {
	if s.fault == nil {
		return nil
	}
	return s.fault(boundary)
}

func (s *Store) Projection() (*State, error) {
	if s == nil || s.ledger == nil {
		return nil, errors.New("supervisor store is required")
	}
	state := emptyState()
	err := s.ledger.View(canonicalRecord, func(_ uint64, raw []byte) error {
		var entry JournalEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return err
		}
		return applyEntry(state, entry)
	})
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return nil, err
	}
	if err = validateState(state); err != nil {
		return nil, err
	}
	return state, nil
}

func (s *Store) Events(after uint64) ([]JournalEntry, error) {
	var entries []JournalEntry
	err := s.ledger.View(canonicalRecord, func(sequence uint64, raw []byte) error {
		if sequence <= after {
			return nil
		}
		var entry JournalEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return err
		}
		entries = append(entries, entry)
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return entries, nil
	}
	return entries, err
}

func replayTxn(tx *secureledger.Txn) (*State, error) {
	state := emptyState()
	err := tx.Replay(func(_ uint64, raw []byte) error {
		var entry JournalEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return err
		}
		return applyEntry(state, entry)
	})
	return state, err
}

func snapshot(tx *secureledger.Txn, state *State) error {
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return tx.ReplaceSnapshot(append(raw, '\n'))
}

func event(kind string, data any) (DomainEvent, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return DomainEvent{}, err
	}
	return DomainEvent{Type: kind, Data: raw}, nil
}

func mustEvent(kind string, data any) DomainEvent {
	e, err := event(kind, data)
	if err != nil {
		panic(err)
	}
	return e
}

func cloneState(state *State) (*State, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	var clone State
	if err = json.Unmarshal(raw, &clone); err != nil {
		return nil, err
	}
	return &clone, nil
}

var (
	ErrIdempotencyConflict    = errors.New("idempotency conflict")
	ErrFenced                 = errors.New("command fenced by newer supervisor state")
	ErrControlAlreadyAccepted = errors.New("an accepted control already exists for this exact Attempt")
	ErrDuplicateAttestation   = errors.New("verifier already attested this exact Result")
	ErrLeaseHeld              = errors.New("canonical worktree writer lease is held")
	ErrPausePending           = errors.New("pause is still draining")
)
