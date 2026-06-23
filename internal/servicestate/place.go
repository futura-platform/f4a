package servicestate

import (
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
)

type PlacementLocation int

const (
	PlacementLocationPending PlacementLocation = iota
	PlacementLocationSuspended
	PlacementLocationNowhere
)

type PlacementPrecondition func(task.AssignmentState) error

var (
	ErrDuplicatePlacement    = errors.New("task is already in the desired placement location")
	ErrTaskNotInPendingState = errors.New("task is not in the pending state")
)

func (p *TaskPlacer) PlaceTaskIn(
	tx fdb.Transaction,
	location PlacementLocation,
	t task.TaskKey,
) error {
	var inSet *reliableset.Set
	var newLifecycleStatus task.LifecycleStatus
	switch location {
	case PlacementLocationPending:
		inSet = p.pendingSet
		newLifecycleStatus = task.LifecycleStatusPending
	case PlacementLocationSuspended:
		inSet = p.suspendedSet
		newLifecycleStatus = task.LifecycleStatusSuspended
	case PlacementLocationNowhere:
		newLifecycleStatus = task.LifecycleStatusNone
	default:
		return fmt.Errorf("invalid placement location: %d", location)
	}

	oldLifecycleStatus, err := p.removeFromCurrentQueue(tx, t, func(as task.AssignmentState) error {
		lifecycleStatus, err := as.LifecycleStatusFuture.Get()
		if err != nil {
			return fmt.Errorf("failed to get task lifecycle status: %w", err)
		}
		if lifecycleStatus == newLifecycleStatus {
			return ErrDuplicatePlacement
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrDuplicatePlacement) {
			return nil // task is already in the desired placement location, no need to do any duplicate work
		}
		return err
	}
	if inSet != nil {
		err = inSet.Add(tx, []byte(t.Id()))
		if err != nil {
			return err
		}
	}
	t.LifecycleStatus().Set(tx, newLifecycleStatus)
	// handle aggregate changes
	return p.handleAggregateUpdate(tx, t, oldLifecycleStatus, newLifecycleStatus)
}

func (p *TaskPlacer) PlaceTaskOnRunner(
	tx fdb.Transaction,
	runnerId string,
	runnerSet *RunnerSet,
	t task.TaskKey,
) error {
	oldLifecycleStatus, err := p.removeFromCurrentQueue(tx, t, func(as task.AssignmentState) error {
		currentRunnerId, err := as.RunnerIDFuture.Get()
		if err != nil {
			return fmt.Errorf("failed to get task runner id: %w", err)
		}
		if currentRunnerId != nil && *currentRunnerId != runnerId {
			return ErrDuplicatePlacement
		}
		lifecycleStatus, err := as.LifecycleStatusFuture.Get()
		if err != nil {
			return fmt.Errorf("failed to get task lifecycle status: %w", err)
		}
		if lifecycleStatus != task.LifecycleStatusPending {
			return fmt.Errorf("%w: %s", ErrTaskNotInPendingState, lifecycleStatus)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrDuplicatePlacement) {
			return nil
		}
		return err
	}
	err = runnerSet.Add(tx, t)
	if err != nil {
		return err
	}
	t.LifecycleStatus().Set(tx, task.LifecycleStatusRunning)
	t.RunnerId().Set(tx, &runnerId)

	return p.handleAggregateUpdate(tx, t, oldLifecycleStatus, task.LifecycleStatusRunning)
}

func (p *TaskPlacer) handleAggregateUpdate(
	tx fdb.Transaction,
	t task.TaskKey,
	oldLifecycleStatus, newLifecycleStatus task.LifecycleStatus,
) error {
	changeMultiplier := int64(0)
	if oldLifecycleStatus == task.LifecycleStatusNone &&
		newLifecycleStatus != task.LifecycleStatusNone { // task was added to a queue
		changeMultiplier = 1
	} else if oldLifecycleStatus != task.LifecycleStatusNone &&
		newLifecycleStatus == task.LifecycleStatusNone { // task was removed from a queue
		changeMultiplier = -1
	}
	if changeMultiplier != 0 {
		resourceRequest, err := t.ResourceRequest().Get(tx).Get()
		if err != nil {
			return err
		}
		p.globalUtilization.add(tx, UtilizationDimensionCPU, changeMultiplier*int64(resourceRequest.CpuMillis))
		p.globalUtilization.add(tx, UtilizationDimensionMemory, changeMultiplier*int64(resourceRequest.MemoryBytes))
	}
	return nil
}

var (
	ErrRunningTaskMissingRunnerID = errors.New("running task missing runner id")
	ErrRunnerSetDoesNotExist      = errors.New("linked runner set does not exist")
)

// removeFromCurrentQueue removes the task from whichever queue is implied by its lifecycle state.
func (p *TaskPlacer) removeFromCurrentQueue(t fdb.Transaction, tkey task.TaskKey, precondition PlacementPrecondition) (task.LifecycleStatus, error) {
	state, err := task.ReadAssignmentState(t, tkey)
	if err != nil {
		return task.LifecycleStatusNone, err
	}
	if err = state.ValidateRunnerIdInvariant(); err != nil {
		return task.LifecycleStatusNone, err
	}
	if precondition != nil {
		if err = precondition(state); err != nil {
			return task.LifecycleStatusNone, err
		}
	}
	lifecycleStatus, err := state.LifecycleStatusFuture.Get()
	if err != nil {
		return task.LifecycleStatusNone, fmt.Errorf("failed to get task lifecycle status: %w", err)
	}
	switch lifecycleStatus {
	case task.LifecycleStatusNone:
		return lifecycleStatus, nil
	case task.LifecycleStatusRunning:
		// Lifecycle invariant: a running task must be in its runner queue.
		// If the queue is missing, surface this as an invariant violation.
		runnerID, err := state.RunnerIDFuture.Get()
		if err != nil {
			return lifecycleStatus, fmt.Errorf("failed to get task runner id: %w", err)
		}
		taskSet, err := OpenTaskSetForRunner(t, p.db, *runnerID)
		if err != nil {
			if errors.Is(err, directory.ErrDirNotExists) {
				return lifecycleStatus, fmt.Errorf("%w: %s", ErrRunnerSetDoesNotExist, *runnerID)
			}
			return lifecycleStatus, fmt.Errorf("failed to open task set: %w", err)
		}
		if err := taskSet.Remove(t, tkey); err != nil {
			return lifecycleStatus, fmt.Errorf("failed to remove task from task set: %w", err)
		}

		// now that the task is removed from the runner's queue, we must also clear the task's runner_id state
		tkey.RunnerId().Set(t, nil)
	case task.LifecycleStatusPending:
		if err := p.pendingSet.Remove(t, []byte(tkey.Id())); err != nil {
			return lifecycleStatus, fmt.Errorf("failed to remove task from ready set: %w", err)
		}
	case task.LifecycleStatusSuspended:
		if err := p.suspendedSet.Remove(t, []byte(tkey.Id())); err != nil {
			return lifecycleStatus, fmt.Errorf("failed to remove task from suspended set: %w", err)
		}
	default:
		return lifecycleStatus, fmt.Errorf("unknown status '%s'", lifecycleStatus)
	}
	return lifecycleStatus, nil
}
