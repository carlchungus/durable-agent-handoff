package activity

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
)

type Supervisor struct {
	Store    *Store
	Env      []string
	children sync.Map
}

func (s *Supervisor) ownerID() string { return runstate.SupervisorIdentity() }

func (s *Supervisor) Start(descriptor Descriptor) (*Activity, Attempt, error) {
	if s == nil || s.Store == nil {
		return nil, Attempt{}, errors.New("activity supervisor store is required")
	}
	if len(descriptor.Command) == 0 || descriptor.Command[0] == "" {
		return nil, Attempt{}, errors.New("activity command is required")
	}
	activity, err := s.Store.Create(descriptor)
	if err != nil {
		return nil, Attempt{}, err
	}
	attempt, stdout, stderr, err := s.Store.PrepareAttempt(activity.ID, activity.Generation, AttemptStart{
		Runtime: descriptor.Runtime, Model: descriptor.Model,
		CommandDigest: runstate.CommandDigest(descriptor.Command[0], descriptor.Command[1:]),
	})
	if err != nil {
		return nil, Attempt{}, err
	}
	gated, err := PrepareGatedCommand(descriptor.Command, descriptor.Work.Cwd, s.Env, nil)
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		_ = s.Store.FailPrepared(activity.ID, activity.Generation, attempt.ID, err.Error())
		return nil, Attempt{}, err
	}
	command := gated.Command
	command.Stdout = stdout
	command.Stderr = stderr
	if err = command.Start(); err != nil {
		gated.Abort()
		_ = stdout.Close()
		_ = stderr.Close()
		_ = s.Store.FailPrepared(activity.ID, activity.Generation, attempt.ID, err.Error())
		return nil, Attempt{}, err
	}
	_ = stdout.Close()
	_ = stderr.Close()
	token := waitForStartToken(command.Process.Pid, 2*time.Second)
	if token == "" {
		gated.Abort()
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = s.Store.FailPrepared(activity.ID, activity.Generation, attempt.ID, "could not establish exact process start token")
		return nil, Attempt{}, errors.New("could not establish exact process start token")
	}
	treeID, treeErr := gated.BindProcessTree(command.Process.Pid, token)
	if treeErr != nil {
		gated.Abort()
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = s.Store.FailPrepared(activity.ID, activity.Generation, attempt.ID, treeErr.Error())
		return nil, Attempt{}, treeErr
	}
	attempt, err = s.Store.MarkRunning(activity.ID, activity.Generation, attempt.ID, ProcessIdentity{PID: command.Process.Pid, ProcessStartToken: token, ProcessTreeID: treeID, SupervisorID: s.ownerID(), SupervisorGeneration: 1})
	if err != nil {
		gated.Abort()
		_ = command.Process.Kill()
		_ = command.Wait()
		return nil, Attempt{}, err
	}
	gated.CompleteActivity(s.Store.Root(), activity.ID, activity.Generation, identityOf(attempt))
	if err = gated.Release(); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = s.Store.FinishAttempt(activity.ID, activity.Generation, identityOf(attempt), ExitResult{State: StateFailed, Error: "release gated activity command: " + err.Error()})
		return nil, Attempt{}, err
	}
	activity, err = s.Store.Load(activity.ID)
	if err != nil {
		return nil, Attempt{}, err
	}
	done := make(chan struct{})
	s.children.Store(activity.ID, done)
	go s.waitChild(activity.ID, command, identityOf(attempt), done)
	return activity, attempt, nil
}

func (s *Supervisor) Recover() ([]*Activity, error) {
	activities, err := s.Store.List()
	if err != nil {
		return nil, err
	}
	recovered := make([]*Activity, 0)
	for _, activity := range activities {
		attempt, ok := currentAttempt(activity)
		if !ok {
			continue
		}
		identity := identityOf(attempt)
		if activity.State == StateStarting {
			if failErr := s.Store.FailPrepared(activity.ID, activity.Generation, attempt.ID, "supervisor exited before an exact process identity was committed"); failErr != nil && !errors.Is(failErr, ErrFenced) {
				return nil, failErr
			}
			continue
		}
		if activity.State != StateRunning && activity.State != StateStopping {
			continue
		}
		if !processMatches(identity) {
			if cleanupErr := cleanupOrphanedProcessTree(identity); cleanupErr != nil && !errors.Is(cleanupErr, ErrFenced) {
				return nil, cleanupErr
			}
			state := StateLost
			reason := "exact process identity is no longer live"
			if activity.State == StateStopping {
				state, reason = StateStopped, "accepted stop completed while the supervisor was unavailable"
			}
			if finishErr := s.Store.FinishAttempt(activity.ID, activity.Generation, identity, ExitResult{State: state, Error: reason}); finishErr != nil && !errors.Is(finishErr, ErrFenced) {
				return nil, finishErr
			}
			continue
		}
		if attempt.SupervisorID == s.ownerID() {
			continue
		}
		if runstate.SupervisorMatches(attempt.SupervisorID) {
			continue
		}
		adoptedAttempt, adopted, adoptErr := s.Store.AdoptAttempt(activity.ID, activity.Generation, identity, s.ownerID())
		if adoptErr != nil {
			return nil, adoptErr
		}
		recovered = append(recovered, adopted)
		adoptedIdentity := identityOf(adoptedAttempt)
		if adopted.State == StateStopping {
			if killErr := killProcessGroup(adoptedIdentity); killErr != nil && !errors.Is(killErr, ErrFenced) {
				return nil, killErr
			}
			if finishErr := s.Store.FinishAttempt(adopted.ID, adopted.Generation, adoptedIdentity, ExitResult{State: StateStopped}); finishErr != nil && !errors.Is(finishErr, ErrFenced) {
				return nil, finishErr
			}
			continue
		}
	}
	return recovered, nil
}

func (s *Supervisor) Stop(id string) (*Activity, error) {
	activity, err := s.Store.Load(id)
	if err != nil {
		return nil, err
	}
	attempt, ok := currentAttempt(activity)
	if !ok || activity.State != StateRunning {
		return nil, fmt.Errorf("activity %s is not running", id)
	}
	identity := identityOf(attempt)
	return s.StopExpected(id, ControlRequest{ExpectedGeneration: activity.Generation, ExpectedAttempt: identity})
}

func (s *Supervisor) StopExpected(id string, request ControlRequest) (*Activity, error) {
	_, stopping, err := s.Store.RequestStop(id, request)
	if err != nil {
		return stopping, err
	}
	identity := request.ExpectedAttempt
	if !processMatches(identity) {
		current, loadErr := s.Store.Load(id)
		if loadErr == nil && terminal(current.State) {
			return current, nil
		}
		return stopping, ErrFenced
	}
	if err = killProcessGroup(identity); err != nil && processMatches(identity) {
		return stopping, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for processMatches(identity) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	current, loadErr := s.Store.Load(id)
	if loadErr != nil {
		return nil, loadErr
	}
	if terminal(current.State) {
		s.waitTrackedChild(id)
		return current, nil
	}
	if processMatches(identity) {
		return current, errors.New("exact activity process did not exit after stop")
	}
	if err = s.Store.FinishAttempt(id, current.Generation, identity, ExitResult{State: StateStopped}); err != nil && !errors.Is(err, ErrFenced) {
		return current, err
	}
	current, err = s.Store.Load(id)
	if err == nil {
		s.waitTrackedChild(id)
	}
	return current, err
}

func (s *Supervisor) waitChild(activityID string, command *exec.Cmd, identity AttemptIdentity, done chan struct{}) {
	defer func() {
		close(done)
		s.children.Delete(activityID)
	}()
	err := command.Wait()
	activity, loadErr := s.Store.Load(activityID)
	if loadErr != nil || terminal(activity.State) {
		return
	}
	state := StateFailed
	if activity.State == StateStopping {
		state = StateStopped
	} else if err == nil {
		state = StateCompleted
	}
	result := ExitResult{State: state}
	if command.ProcessState != nil {
		code := command.ProcessState.ExitCode()
		result.ExitCode = &code
	}
	if err != nil && state != StateStopped {
		result.Error = err.Error()
	}
	_ = s.Store.FinishAttempt(activityID, activity.Generation, identity, result)
}

func (s *Supervisor) waitTrackedChild(activityID string) {
	if value, ok := s.children.Load(activityID); ok {
		<-value.(chan struct{})
	}
}

func waitForStartToken(pid int, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for {
		if token := runstate.ProcessStartToken(pid); token != "" {
			return token
		}
		if time.Now().After(deadline) {
			return ""
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func identityOf(attempt Attempt) AttemptIdentity {
	return AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, ProcessTreeID: attempt.ProcessTreeID, SupervisorID: attempt.SupervisorID, SupervisorGeneration: attempt.SupervisorGeneration}
}

func processMatches(identity AttemptIdentity) bool {
	return runstate.ProcessMatches(runstate.Manifest{PID: identity.PID, ProcessStartToken: identity.ProcessStartToken})
}
