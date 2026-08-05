package activity

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/carlchungus/durable-agent-handoff/internal/runstate"
)

type Supervisor struct {
	Store *Store
	Env   []string
}

func (s *Supervisor) Start(descriptor Descriptor) (*Activity, Attempt, error) {
	if s == nil || s.Store == nil {
		return nil, Attempt{}, errors.New("activity supervisor store is required")
	}
	activity, err := s.Store.Create(descriptor)
	if err != nil {
		return nil, Attempt{}, err
	}
	attempt, stdout, stderr, err := s.Store.PrepareAttempt(activity.ID, activity.Generation, AttemptStart{
		Runtime: descriptor.Launch.Runtime, Model: descriptor.Launch.Model,
		LaunchDigest: runstate.CommandDigest(descriptor.Launch.Argv[0], descriptor.Launch.Argv[1:]),
	})
	if err != nil {
		return nil, Attempt{}, err
	}
	command := exec.Command(descriptor.Launch.Argv[0], descriptor.Launch.Argv[1:]...)
	command.Dir = descriptor.Launch.Cwd
	command.Env = append(os.Environ(), s.Env...)
	command.Stdout = stdout
	command.Stderr = stderr
	configureBackgroundProcess(command)
	if err = command.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, Attempt{}, err
	}
	_ = stdout.Close()
	_ = stderr.Close()
	token := waitForStartToken(command.Process.Pid, 2*time.Second)
	if token == "" {
		_ = command.Process.Kill()
		_ = command.Wait()
		return nil, Attempt{}, errors.New("could not establish exact process start token")
	}
	attempt, err = s.Store.MarkRunning(activity.ID, activity.Generation, attempt.ID, ProcessIdentity{PID: command.Process.Pid, ProcessStartToken: token, SupervisorGeneration: 1})
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return nil, Attempt{}, err
	}
	activity, err = s.Store.Load(activity.ID)
	if err != nil {
		return nil, Attempt{}, err
	}
	go s.waitChild(activity.ID, command, identityOf(attempt))
	return activity, attempt, nil
}

func (s *Supervisor) Recover() ([]*Activity, error) {
	activities, err := s.Store.List()
	if err != nil {
		return nil, err
	}
	recovered := make([]*Activity, 0)
	for _, activity := range activities {
		if activity.State != StateRunning {
			continue
		}
		attempt, ok := currentAttempt(activity)
		if !ok {
			continue
		}
		identity := identityOf(attempt)
		if !processMatches(identity) {
			if finishErr := s.Store.FinishAttempt(activity.ID, activity.Generation, identity, ExitResult{State: StateLost, Error: "exact process identity is no longer live"}); finishErr != nil && !errors.Is(finishErr, ErrFenced) {
				return nil, finishErr
			}
			continue
		}
		_, adopted, adoptErr := s.Store.AdoptAttempt(activity.ID, activity.Generation, identity)
		if adoptErr != nil {
			return nil, adoptErr
		}
		recovered = append(recovered, adopted)
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
		return stopping, ErrFenced
	}
	process, err := os.FindProcess(identity.PID)
	if err != nil {
		return stopping, err
	}
	if err = process.Kill(); err != nil && processMatches(identity) {
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
		return current, nil
	}
	if processMatches(identity) {
		return current, errors.New("exact activity process did not exit after stop")
	}
	if err = s.Store.FinishAttempt(id, current.Generation, identity, ExitResult{State: StateStopped}); err != nil && !errors.Is(err, ErrFenced) {
		return current, err
	}
	return s.Store.Load(id)
}

func (s *Supervisor) waitChild(activityID string, command *exec.Cmd, identity AttemptIdentity) {
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
	return AttemptIdentity{ID: attempt.ID, PID: attempt.PID, ProcessStartToken: attempt.ProcessStartToken, SupervisorGeneration: attempt.SupervisorGeneration}
}

func processMatches(identity AttemptIdentity) bool {
	return runstate.ProcessMatches(runstate.Manifest{PID: identity.PID, ProcessStartToken: identity.ProcessStartToken})
}
