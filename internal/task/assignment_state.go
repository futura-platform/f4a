package task

import (
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// AssignmentState captures the lifecycle + runner assignment fields that must
// move in lockstep for task scheduling correctness.
type AssignmentState struct {
	LifecycleStatusFuture *dbutil.Future[LifecycleStatus]
	RunnerIDFuture        *dbutil.Future[*string]
}

var (
	// ErrRunningTaskMissingRunnerID indicates a running task without a runner_id.
	ErrRunningTaskMissingRunnerID = errors.New("running task missing runner id")
	// ErrNonRunningTaskHasRunnerID indicates stale runner_id on a non-running task.
	ErrNonRunningTaskHasRunnerID = errors.New("non-running task has runner id")
)

// ReadAssignmentState reads the task assignment state atomically from one read
// transaction snapshot.
func ReadAssignmentState(tx fdb.ReadTransaction, taskKey TaskKey) (AssignmentState, error) {
	statusFuture := taskKey.LifecycleStatus().Get(tx)
	runnerIDFuture := taskKey.RunnerId().Get(tx)

	return AssignmentState{
		LifecycleStatusFuture: statusFuture,
		RunnerIDFuture:        runnerIDFuture,
	}, nil
}

// ValidateRunnerLifecycleInvariant enforces the canonical invariant for task
// assignment metadata:
//   - running => non-empty runner_id
//   - non-running => nil runner_id
func (s AssignmentState) ValidateRunnerLifecycleInvariant() error {
	lifecycleStatus, err := s.LifecycleStatusFuture.Get()
	if err != nil {
		return fmt.Errorf("failed to get lifecycle status: %w", err)
	}
	runnerId, err := s.RunnerIDFuture.Get()
	if err != nil {
		return fmt.Errorf("failed to get runner id: %w", err)
	}
	switch lifecycleStatus {
	case LifecycleStatusRunning:
		if runnerId == nil || *runnerId == "" {
			return ErrRunningTaskMissingRunnerID
		}
	default:
		if runnerId != nil {
			return ErrNonRunningTaskHasRunnerID
		}
	}
	return nil
}

// IsRunningOn reports whether the task is currently running on runnerID.
func (s AssignmentState) IsRunningOn(runnerID string) (bool, error) {
	lifecycleStatus, err := s.LifecycleStatusFuture.Get()
	if err != nil {
		return false, fmt.Errorf("failed to get lifecycle status: %w", err)
	}
	runnerId, err := s.RunnerIDFuture.Get()
	if err != nil {
		return false, fmt.Errorf("failed to get runner id: %w", err)
	}
	return lifecycleStatus == LifecycleStatusRunning &&
		runnerId != nil &&
		*runnerId == runnerID, nil
}
