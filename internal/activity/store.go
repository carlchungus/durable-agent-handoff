package activity

import (
	"crypto/rand"
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

var activityIdentifier = regexp.MustCompile(`^activity_[a-f0-9]{24}$`)

var ErrFenced = errors.New("activity control was fenced by newer state")

type Store struct{ ledger *secureledger.Ledger }

func OpenStore(root string) (*Store, error) {
	ledger, err := secureledger.Open(root, secureledger.Options{
		Namespace: "activities",
		ValidateID: func(id string) error {
			if !activityIdentifier.MatchString(id) {
				return fmt.Errorf("invalid activity id %q", id)
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return &Store{ledger: ledger}, nil
}

func (s *Store) Create(descriptor Descriptor) (*Activity, error) {
	if strings.TrimSpace(descriptor.Launch.Kind) == "" || len(descriptor.Launch.Argv) == 0 || strings.TrimSpace(descriptor.Launch.Argv[0]) == "" {
		return nil, errors.New("activity launch kind and argv are required")
	}
	id := descriptor.ID
	if id == "" {
		var err error
		id, err = newID("activity")
		if err != nil {
			return nil, err
		}
	}
	launch := cloneLaunch(descriptor.Launch)
	digest, err := launchDigest(launch)
	if err != nil {
		return nil, err
	}
	var result *Activity
	err = s.ledger.Update(id, func(tx *secureledger.Txn) error {
		if _, loadErr := replay(tx.Replay); loadErr == nil {
			return fmt.Errorf("activity %s already exists", id)
		} else if !errors.Is(loadErr, os.ErrNotExist) {
			return loadErr
		}
		now := time.Now().UTC()
		activity := &Activity{Version: Version, ID: id, OwnerSessionID: descriptor.OwnerSessionID, Launch: launch, LaunchDigest: digest, State: StatePending, Generation: 1, Revision: 1, CreatedAt: now, UpdatedAt: now}
		if err := appendEvent(tx, Event{ActivityID: id, Type: "activity.created", At: now, Data: activity}); err != nil {
			return err
		}
		if err := snapshot(tx, activity); err != nil {
			return err
		}
		result = cloneActivity(activity)
		return nil
	})
	return result, err
}

func (s *Store) Ensure(descriptor Descriptor) (*Activity, error) {
	if descriptor.ID == "" {
		return s.Create(descriptor)
	}
	existing, err := s.Load(descriptor.ID)
	if errors.Is(err, os.ErrNotExist) {
		return s.Create(descriptor)
	}
	if err != nil {
		return nil, err
	}
	digest, err := launchDigest(cloneLaunch(descriptor.Launch))
	if err != nil {
		return nil, err
	}
	if existing.OwnerSessionID != descriptor.OwnerSessionID || existing.LaunchDigest != digest {
		return nil, fmt.Errorf("activity %s immutable launch does not match", descriptor.ID)
	}
	return existing, nil
}

func (s *Store) PrepareAttempt(id string, expectedGeneration uint64, start AttemptStart) (Attempt, *os.File, *os.File, error) {
	var attempt Attempt
	var stdout, stderr *os.File
	err := s.ledger.Update(id, func(tx *secureledger.Txn) error {
		activity, err := replay(tx.Replay)
		if err != nil {
			return err
		}
		if activity.Generation != expectedGeneration || !canStart(activity.State) {
			return ErrFenced
		}
		ordinal := len(activity.Attempts) + 1
		attemptID := fmt.Sprintf("attempt_%d", ordinal)
		stdoutName := attemptID + "_stdout.log"
		stderrName := attemptID + "_stderr.log"
		stdoutPath, err := s.ledger.BlobPath(id, stdoutName)
		if err != nil {
			return err
		}
		stderrPath, err := s.ledger.BlobPath(id, stderrName)
		if err != nil {
			return err
		}
		stdout, err = tx.CreateBlob(stdoutName)
		if err != nil {
			return err
		}
		stderr, err = tx.CreateBlob(stderrName)
		if err != nil {
			_ = stdout.Close()
			return err
		}
		now := time.Now().UTC()
		attempt = Attempt{
			ID: attemptID, Ordinal: ordinal, Runtime: start.Runtime, Model: start.Model, LaunchDigest: start.LaunchDigest,
			State: StateStarting, StartedAt: now,
			Stdout: outputRef(id, attemptID, StreamStdout, stdoutName, stdoutPath),
			Stderr: outputRef(id, attemptID, StreamStderr, stderrName, stderrPath),
		}
		if err = appendEvent(tx, Event{ActivityID: id, Type: "attempt.prepared", At: now, Data: attempt}); err != nil {
			_ = stdout.Close()
			_ = stderr.Close()
			return err
		}
		activity.Attempts = append(activity.Attempts, attempt)
		activity.State = StateStarting
		activity.Revision++
		activity.UpdatedAt = now
		return snapshot(tx, activity)
	})
	if err != nil {
		return Attempt{}, nil, nil, err
	}
	return attempt, stdout, stderr, nil
}

func (s *Store) MarkRunning(id string, expectedGeneration uint64, attemptID string, process ProcessIdentity) (Attempt, error) {
	if process.PID <= 0 || strings.TrimSpace(process.ProcessStartToken) == "" || process.SupervisorGeneration == 0 {
		return Attempt{}, errors.New("running attempt requires exact pid, process start token, and supervisor generation")
	}
	var result Attempt
	err := s.ledger.Update(id, func(tx *secureledger.Txn) error {
		activity, err := replay(tx.Replay)
		if err != nil {
			return err
		}
		current, ok := currentAttempt(activity)
		if !ok || current.ID != attemptID || current.State != StateStarting || activity.State != StateStarting || activity.Generation != expectedGeneration {
			return ErrFenced
		}
		now := time.Now().UTC()
		data := runningEvent{AttemptID: attemptID, Process: process}
		if err = appendEvent(tx, Event{ActivityID: id, Type: "attempt.running", At: now, Data: data}); err != nil {
			return err
		}
		applyRunning(activity, data, now)
		result = activity.Attempts[len(activity.Attempts)-1]
		return snapshot(tx, activity)
	})
	return result, err
}

func (s *Store) FailPrepared(id string, expectedGeneration uint64, attemptID, reason string) error {
	return s.ledger.Update(id, func(tx *secureledger.Txn) error {
		activity, err := replay(tx.Replay)
		if err != nil {
			return err
		}
		current, ok := currentAttempt(activity)
		if !ok || current.ID != attemptID || current.State != StateStarting || activity.State != StateStarting || activity.Generation != expectedGeneration {
			return ErrFenced
		}
		now := time.Now().UTC()
		data := preparedFailureEvent{AttemptID: attemptID, Error: reason}
		if err = appendEvent(tx, Event{ActivityID: id, Type: "attempt.prepare_failed", At: now, Data: data}); err != nil {
			return err
		}
		applyPreparedFailure(activity, data, now)
		return snapshot(tx, activity)
	})
}

func (s *Store) AdoptAttempt(id string, expectedGeneration uint64, expected AttemptIdentity) (Attempt, *Activity, error) {
	var adopted Attempt
	var result *Activity
	err := s.ledger.Update(id, func(tx *secureledger.Txn) error {
		activity, err := replay(tx.Replay)
		if err != nil {
			return err
		}
		current, ok := currentAttempt(activity)
		if !ok || activity.State != StateRunning || activity.Generation != expectedGeneration || !sameIdentity(current, expected) {
			return ErrFenced
		}
		now := time.Now().UTC()
		data := adoptEvent{Expected: expected, Generation: activity.Generation + 1, SupervisorGeneration: current.SupervisorGeneration + 1}
		if err = appendEvent(tx, Event{ActivityID: id, Type: "attempt.adopted", At: now, Data: data}); err != nil {
			return err
		}
		applyAdopt(activity, data, now)
		adopted = activity.Attempts[len(activity.Attempts)-1]
		result = cloneActivity(activity)
		return snapshot(tx, activity)
	})
	return adopted, result, err
}

func (s *Store) RequestStop(id string, request ControlRequest) (ControlIntent, *Activity, error) {
	var intent ControlIntent
	var result *Activity
	var fenced bool
	err := s.ledger.Update(id, func(tx *secureledger.Txn) error {
		activity, err := replay(tx.Replay)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		intent = ControlIntent{ID: fmt.Sprintf("control_%d", len(activity.Controls)+1), Kind: "stop", ExpectedGeneration: request.ExpectedGeneration, ExpectedAttempt: request.ExpectedAttempt, CreatedAt: now}
		current, ok := currentAttempt(activity)
		if !ok || activity.Generation != request.ExpectedGeneration || !sameIdentity(current, request.ExpectedAttempt) || activity.State != StateRunning {
			intent.Outcome = ControlRejected
			intent.Reason = "activity generation, state, or exact attempt identity changed"
			fenced = true
		} else {
			intent.Outcome = ControlAccepted
		}
		if err = appendEvent(tx, Event{ActivityID: id, Type: "control.requested", At: now, Data: intent}); err != nil {
			return err
		}
		activity.Controls = append(activity.Controls, intent)
		if !fenced {
			activity.State = StateStopping
		}
		activity.Revision++
		activity.UpdatedAt = now
		if err = snapshot(tx, activity); err != nil {
			return err
		}
		result = cloneActivity(activity)
		return nil
	})
	if err != nil {
		return ControlIntent{}, nil, err
	}
	if fenced {
		return intent, result, ErrFenced
	}
	return intent, result, nil
}

func (s *Store) FinishAttempt(id string, expectedGeneration uint64, identity AttemptIdentity, result ExitResult) error {
	if !terminal(result.State) {
		return fmt.Errorf("attempt result state %q is not terminal", result.State)
	}
	return s.ledger.Update(id, func(tx *secureledger.Txn) error {
		activity, err := replay(tx.Replay)
		if err != nil {
			return err
		}
		current, ok := currentAttempt(activity)
		if !ok || terminal(activity.State) || terminal(current.State) || activity.Generation != expectedGeneration || !sameIdentity(current, identity) {
			return ErrFenced
		}
		now := time.Now().UTC()
		data := finishEvent{Identity: identity, Result: result}
		if err = appendEvent(tx, Event{ActivityID: id, Type: "attempt.finished", At: now, Data: data}); err != nil {
			return err
		}
		applyFinish(activity, data, now)
		return snapshot(tx, activity)
	})
}

func (s *Store) ReadOutput(id string, cursor OutputCursor, maxBytes int) (OutputChunk, error) {
	activity, err := s.Load(id)
	if err != nil {
		return OutputChunk{}, err
	}
	attempt, ok := findAttempt(activity, cursor.AttemptID)
	if !ok {
		return OutputChunk{}, fmt.Errorf("attempt %q not found", cursor.AttemptID)
	}
	output, err := selectOutput(attempt, cursor.Stream)
	if err != nil {
		return OutputChunk{}, err
	}
	if output.ID != cursor.OutputID {
		return OutputChunk{}, errors.New("output identity changed")
	}
	chunk, err := s.ledger.ReadBlob(id, output.FileName, cursor.After, maxBytes)
	if err != nil {
		return OutputChunk{}, err
	}
	return OutputChunk{ActivityID: id, AttemptID: attempt.ID, Stream: output.Stream, OutputID: output.ID, Start: chunk.Start, End: chunk.End, Size: chunk.Size, Data: chunk.Data, Closed: output.Closed, Revision: activity.Revision}, nil
}

func (s *Store) Load(id string) (*Activity, error) {
	return replay(func(visit func(uint64, []byte) error) error { return s.ledger.View(id, visit) })
}

func (s *Store) List() ([]*Activity, error) {
	ids, err := s.ledger.IDs()
	if err != nil {
		return nil, err
	}
	activities := make([]*Activity, 0, len(ids))
	for _, id := range ids {
		activity, loadErr := s.Load(id)
		if loadErr == nil {
			activities = append(activities, activity)
		}
	}
	sort.Slice(activities, func(i, j int) bool {
		if activities[i].UpdatedAt.Equal(activities[j].UpdatedAt) {
			return activities[i].ID < activities[j].ID
		}
		return activities[i].UpdatedAt.After(activities[j].UpdatedAt)
	})
	return activities, nil
}

type Event struct {
	Sequence   uint64    `json:"sequence"`
	ActivityID string    `json:"activity_id"`
	Type       string    `json:"type"`
	At         time.Time `json:"at"`
	Data       any       `json:"data,omitempty"`
}

type rawEvent struct {
	Sequence   uint64          `json:"sequence"`
	ActivityID string          `json:"activity_id"`
	Type       string          `json:"type"`
	At         time.Time       `json:"at"`
	Data       json.RawMessage `json:"data"`
}

type replayFn func(func(uint64, []byte) error) error

func replay(run replayFn) (*Activity, error) {
	var activity *Activity
	if err := run(func(_ uint64, raw []byte) error {
		var event rawEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		switch event.Type {
		case "activity.created":
			var created Activity
			if err := json.Unmarshal(event.Data, &created); err != nil {
				return err
			}
			activity = &created
		case "attempt.prepared":
			if activity == nil {
				return errors.New("attempt preceded activity creation")
			}
			var attempt Attempt
			if err := json.Unmarshal(event.Data, &attempt); err != nil {
				return err
			}
			activity.Attempts = append(activity.Attempts, attempt)
			activity.State = StateStarting
			activity.Revision = event.Sequence
			activity.UpdatedAt = event.At
		case "attempt.running":
			if activity == nil {
				return errors.New("running event preceded activity creation")
			}
			var data runningEvent
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return err
			}
			applyRunning(activity, data, event.At)
		case "attempt.prepare_failed":
			if activity == nil {
				return errors.New("prepare failure preceded activity creation")
			}
			var data preparedFailureEvent
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return err
			}
			applyPreparedFailure(activity, data, event.At)
		case "attempt.adopted":
			if activity == nil {
				return errors.New("adoption preceded activity creation")
			}
			var data adoptEvent
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return err
			}
			applyAdopt(activity, data, event.At)
		case "control.requested":
			if activity == nil {
				return errors.New("control preceded activity creation")
			}
			var intent ControlIntent
			if err := json.Unmarshal(event.Data, &intent); err != nil {
				return err
			}
			activity.Controls = append(activity.Controls, intent)
			if intent.Outcome == ControlAccepted {
				activity.State = StateStopping
			}
			activity.Revision = event.Sequence
			activity.UpdatedAt = event.At
		case "attempt.finished":
			if activity == nil {
				return errors.New("finish preceded activity creation")
			}
			var data finishEvent
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return err
			}
			applyFinish(activity, data, event.At)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if activity == nil {
		return nil, errors.New("activity ledger has no creation event")
	}
	return cloneActivity(activity), nil
}

type finishEvent struct {
	Identity AttemptIdentity `json:"identity"`
	Result   ExitResult      `json:"result"`
}

type runningEvent struct {
	AttemptID string          `json:"attempt_id"`
	Process   ProcessIdentity `json:"process"`
}

type adoptEvent struct {
	Expected             AttemptIdentity `json:"expected"`
	Generation           uint64          `json:"generation"`
	SupervisorGeneration uint64          `json:"supervisor_generation"`
}

type preparedFailureEvent struct {
	AttemptID string `json:"attempt_id"`
	Error     string `json:"error"`
}

func appendEvent(tx *secureledger.Txn, event Event) error {
	_, err := tx.Append(func(next uint64) ([]byte, error) {
		event.Sequence = next
		return json.Marshal(event)
	})
	return err
}

func snapshot(tx *secureledger.Txn, activity *Activity) error {
	raw, err := json.MarshalIndent(activity, "", "  ")
	if err != nil {
		return err
	}
	return tx.ReplaceSnapshot(append(raw, '\n'))
}

func applyFinish(activity *Activity, data finishEvent, at time.Time) {
	for i := range activity.Attempts {
		if activity.Attempts[i].ID == data.Identity.ID {
			activity.Attempts[i].State = data.Result.State
			activity.Attempts[i].ExitCode = data.Result.ExitCode
			activity.Attempts[i].Error = data.Result.Error
			activity.Attempts[i].FinishedAt = at
			activity.Attempts[i].Stdout.Closed = true
			activity.Attempts[i].Stderr.Closed = true
			break
		}
	}
	for i := range activity.Controls {
		if activity.Controls[i].Outcome == ControlAccepted && activity.Controls[i].ExpectedAttempt.ID == data.Identity.ID {
			activity.Controls[i].Outcome = ControlApplied
			activity.Controls[i].AppliedAt = at
		}
	}
	activity.State = data.Result.State
	activity.Revision++
	activity.UpdatedAt = at
}

func applyRunning(activity *Activity, data runningEvent, at time.Time) {
	for i := range activity.Attempts {
		if activity.Attempts[i].ID == data.AttemptID {
			activity.Attempts[i].PID = data.Process.PID
			activity.Attempts[i].ProcessStartToken = data.Process.ProcessStartToken
			activity.Attempts[i].SupervisorGeneration = data.Process.SupervisorGeneration
			activity.Attempts[i].State = StateRunning
			break
		}
	}
	activity.State = StateRunning
	activity.Revision++
	activity.UpdatedAt = at
}

func applyAdopt(activity *Activity, data adoptEvent, at time.Time) {
	for i := range activity.Attempts {
		if activity.Attempts[i].ID == data.Expected.ID {
			activity.Attempts[i].SupervisorGeneration = data.SupervisorGeneration
			break
		}
	}
	activity.Generation = data.Generation
	activity.Revision++
	activity.UpdatedAt = at
}

func applyPreparedFailure(activity *Activity, data preparedFailureEvent, at time.Time) {
	for i := range activity.Attempts {
		if activity.Attempts[i].ID == data.AttemptID {
			activity.Attempts[i].State = StateFailed
			activity.Attempts[i].Error = data.Error
			activity.Attempts[i].FinishedAt = at
			activity.Attempts[i].Stdout.Closed = true
			activity.Attempts[i].Stderr.Closed = true
			break
		}
	}
	activity.State = StateFailed
	activity.Revision++
	activity.UpdatedAt = at
}

func currentAttempt(activity *Activity) (Attempt, bool) {
	if len(activity.Attempts) == 0 {
		return Attempt{}, false
	}
	return activity.Attempts[len(activity.Attempts)-1], true
}

func findAttempt(activity *Activity, id string) (Attempt, bool) {
	for _, attempt := range activity.Attempts {
		if attempt.ID == id {
			return attempt, true
		}
	}
	return Attempt{}, false
}

func selectOutput(attempt Attempt, stream Stream) (OutputRef, error) {
	switch stream {
	case StreamStdout:
		return attempt.Stdout, nil
	case StreamStderr:
		return attempt.Stderr, nil
	default:
		return OutputRef{}, fmt.Errorf("invalid output stream %q", stream)
	}
}

func sameIdentity(attempt Attempt, identity AttemptIdentity) bool {
	return attempt.ID == identity.ID && attempt.PID == identity.PID && attempt.ProcessStartToken == identity.ProcessStartToken && attempt.SupervisorGeneration == identity.SupervisorGeneration
}

func canStart(state State) bool {
	return state == StatePending || state == StateLost || state == StateFailed
}

func terminal(state State) bool {
	return state == StateCompleted || state == StateFailed || state == StateStopped || state == StateLost
}

func outputRef(activityID, attemptID string, stream Stream, fileName, path string) OutputRef {
	sum := sha256.Sum256([]byte(activityID + "\x00" + attemptID + "\x00" + string(stream)))
	return OutputRef{ID: "output_" + hex.EncodeToString(sum[:12]), Stream: stream, FileName: fileName, Path: path}
}

func launchDigest(spec LaunchSpec) (string, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func newID(prefix string) (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(bytes), nil
}

func StableID(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "activity_" + hex.EncodeToString(sum[:12])
}

func cloneLaunch(launch LaunchSpec) LaunchSpec {
	launch.Argv = append([]string(nil), launch.Argv...)
	return launch
}

func cloneActivity(activity *Activity) *Activity {
	copy := *activity
	copy.Launch = cloneLaunch(activity.Launch)
	copy.Attempts = append([]Attempt(nil), activity.Attempts...)
	copy.Controls = append([]ControlIntent(nil), activity.Controls...)
	return &copy
}
