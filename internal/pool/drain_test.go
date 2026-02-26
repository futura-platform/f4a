package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func stringPointer(v string) *string {
	return &v
}

func setRunnerActive(t testing.TB, db dbutil.DbRoot, activeRunners ActiveRunners, runnerID string, active bool) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		activeRunners.SetActive(tx, runnerID, active)
		return nil, nil
	})
	require.NoError(t, err)
}

func isRunnerActive(t testing.TB, db dbutil.DbRoot, activeRunners ActiveRunners, runnerID string) bool {
	t.Helper()

	var active bool
	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		var err error
		active, err = activeRunners.IsActive(tx, runnerID).Get()
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	require.NoError(t, err)
	return active
}

func enqueueTaskWithAssignment(
	t testing.TB,
	db dbutil.DbRoot,
	taskSet *reliableset.Set,
	taskDir task.TasksDirectory,
	id task.Id,
	status task.LifecycleStatus,
	runnerID *string,
) {
	t.Helper()

	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		tkey, err := taskDir.Create(tx, id)
		if err != nil {
			return nil, err
		}
		tkey.LifecycleStatus().Set(tx, status)
		tkey.RunnerId().Set(tx, runnerID)
		if err := taskSet.Add(tx, []byte(id)); err != nil {
			return nil, err
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func enqueueTasksWithAssignmentInBatches(
	t testing.TB,
	db dbutil.DbRoot,
	taskSet *reliableset.Set,
	taskDir task.TasksDirectory,
	ids []task.Id,
	status task.LifecycleStatus,
	runnerID *string,
) {
	t.Helper()

	const createBatchSize = 64
	for batchStart := 0; batchStart < len(ids); batchStart += createBatchSize {
		batchEnd := batchStart + createBatchSize
		if batchEnd > len(ids) {
			batchEnd = len(ids)
		}
		batch := ids[batchStart:batchEnd]

		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			for _, id := range batch {
				tkey, err := taskDir.Create(tx, id)
				if err != nil {
					return nil, err
				}
				tkey.LifecycleStatus().Set(tx, status)
				tkey.RunnerId().Set(tx, runnerID)
				if err := taskSet.Add(tx, []byte(id)); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		require.NoError(t, err)
	}
}

func readSetItems(t testing.TB, db dbutil.DbRoot, set *reliableset.Set) mapset.Set[string] {
	t.Helper()

	var items mapset.Set[string]
	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		var err error
		items, _, err = set.Items(tx)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	require.NoError(t, err)
	return items
}

func readTaskAssignment(
	t testing.TB,
	db dbutil.DbRoot,
	taskDir task.TasksDirectory,
	id task.Id,
) (task.LifecycleStatus, *string) {
	t.Helper()

	var status task.LifecycleStatus
	var runnerID *string
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		tkey, err := taskDir.Open(tx, id)
		if err != nil {
			return nil, err
		}

		state, err := task.ReadAssignmentState(tx, tkey)
		if err != nil {
			return nil, err
		}
		status, err = state.LifecycleStatusFuture.Get()
		if err != nil {
			return nil, err
		}
		runnerID, err = state.RunnerIDFuture.Get()
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	require.NoError(t, err)
	return status, runnerID
}

type taskAssignmentSnapshot struct {
	Exists   bool
	Status   task.LifecycleStatus
	RunnerID *string
}

func readTaskAssignments(
	t testing.TB,
	db dbutil.DbRoot,
	taskDir task.TasksDirectory,
	ids []task.Id,
) map[task.Id]taskAssignmentSnapshot {
	t.Helper()

	snapshots := make(map[task.Id]taskAssignmentSnapshot, len(ids))
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, id := range ids {
			tkey, err := taskDir.Open(tx, id)
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					snapshots[id] = taskAssignmentSnapshot{Exists: false}
					continue
				}
				return nil, err
			}

			state, err := task.ReadAssignmentState(tx, tkey)
			if err != nil {
				return nil, err
			}
			if err := state.ValidateRunnerLifecycleInvariant(); err != nil {
				return nil, err
			}
			status, err := state.LifecycleStatusFuture.Get()
			if err != nil {
				return nil, err
			}
			runnerID, err := state.RunnerIDFuture.Get()
			if err != nil {
				return nil, err
			}
			snapshots[id] = taskAssignmentSnapshot{
				Exists:   true,
				Status:   status,
				RunnerID: runnerID,
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
	return snapshots
}

func TestDrainTaskRunner_DrainsAllTasksAcrossMultipleBatches(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		runnerID := "runner-a"
		activeRunners, err := CreateOrOpenActiveRunners(db)
		require.NoError(t, err)

		taskSet, cancelTaskSet, err := CreateOrOpenTaskSetForRunner(db, runnerID)
		require.NoError(t, err)
		defer cancelTaskSet()

		pendingSet, cancelPendingSet, err := servicestate.CreateOrOpenReadySet(db)
		require.NoError(t, err)
		defer cancelPendingSet()

		taskDir, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)

		setRunnerActive(t, db, activeRunners, runnerID, true)

		const taskCount = 600
		taskIDs := make([]task.Id, 0, taskCount)
		for i := range taskCount {
			id := task.Id(fmt.Sprintf("drain-task-%03d", i))
			taskIDs = append(taskIDs, id)
			enqueueTaskWithAssignment(t, db, taskSet, taskDir, id, task.LifecycleStatusRunning, stringPointer(runnerID))
		}

		err = DrainTaskRunner(db, runnerID, activeRunners, taskSet, pendingSet, taskDir)
		require.NoError(t, err)

		require.False(t, isRunnerActive(t, db, activeRunners, runnerID))

		remainingTaskSetItems := readSetItems(t, db, taskSet)
		pendingItems := readSetItems(t, db, pendingSet)
		require.Equal(t, 0, remainingTaskSetItems.Cardinality())
		require.Equal(t, len(taskIDs), pendingItems.Cardinality())

		for _, id := range taskIDs {
			status, assignedRunnerID := readTaskAssignment(t, db, taskDir, id)
			require.Equal(t, task.LifecycleStatusPending, status, "task %s should be pending", id)
			require.Nil(t, assignedRunnerID, "task %s should have no runner", id)
			require.True(t, pendingItems.Contains(string(id)), "task %s should be in pending set", id)
		}
	})
}

func TestDrainTaskRunner_BatchFailureDoesNotLeaveMixedTaskState(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		runnerID := "runner-a"
		activeRunners, err := CreateOrOpenActiveRunners(db)
		require.NoError(t, err)

		taskSet, cancelTaskSet, err := CreateOrOpenTaskSetForRunner(db, runnerID)
		require.NoError(t, err)
		defer cancelTaskSet()

		pendingSet, cancelPendingSet, err := servicestate.CreateOrOpenReadySet(db)
		require.NoError(t, err)
		defer cancelPendingSet()

		taskDir, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)

		setRunnerActive(t, db, activeRunners, runnerID, true)

		const validTaskCount = 700
		validTaskIDs := make([]task.Id, 0, validTaskCount)
		for i := range validTaskCount {
			id := task.Id(fmt.Sprintf("valid-drain-task-%03d", i))
			validTaskIDs = append(validTaskIDs, id)
			enqueueTaskWithAssignment(t, db, taskSet, taskDir, id, task.LifecycleStatusRunning, stringPointer(runnerID))
		}

		invalidTaskID := task.Id("invalid-running-task")
		enqueueTaskWithAssignment(t, db, taskSet, taskDir, invalidTaskID, task.LifecycleStatusRunning, nil)

		err = DrainTaskRunner(db, runnerID, activeRunners, taskSet, pendingSet, taskDir)
		require.Error(t, err)
		require.ErrorContains(t, err, "task assignment invariant violation")

		require.False(t, isRunnerActive(t, db, activeRunners, runnerID))

		taskSetItems := readSetItems(t, db, taskSet)
		pendingItems := readSetItems(t, db, pendingSet)

		drainedCount := 0
		untouchedCount := 0
		for _, id := range validTaskIDs {
			status, assignedRunnerID := readTaskAssignment(t, db, taskDir, id)
			inTaskSet := taskSetItems.Contains(string(id))
			inPending := pendingItems.Contains(string(id))

			isDrainedState := status == task.LifecycleStatusPending &&
				assignedRunnerID == nil &&
				!inTaskSet &&
				inPending
			if isDrainedState {
				drainedCount++
				continue
			}

			isUntouchedState := status == task.LifecycleStatusRunning &&
				assignedRunnerID != nil &&
				*assignedRunnerID == runnerID &&
				inTaskSet &&
				!inPending
			if isUntouchedState {
				untouchedCount++
				continue
			}

			t.Fatalf(
				"task %s ended in mixed state: status=%s runner=%v inTaskSet=%t inPending=%t",
				id,
				status.String(),
				assignedRunnerID,
				inTaskSet,
				inPending,
			)
		}
		require.Equal(t, len(validTaskIDs), drainedCount+untouchedCount)

		invalidStatus, invalidRunnerID := readTaskAssignment(t, db, taskDir, invalidTaskID)
		require.Equal(t, task.LifecycleStatusRunning, invalidStatus)
		require.Nil(t, invalidRunnerID)
		require.True(t, taskSetItems.Contains(string(invalidTaskID)))
		require.False(t, pendingItems.Contains(string(invalidTaskID)))
	})
}

func TestDrainTaskRunner_ConcurrentMutationsFuzzStyle(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		// This test intentionally creates high write contention. Raise retry budget
		// to better reflect production behavior under concurrent drain/reassignment.
		require.NoError(t, db.Options().SetTransactionRetryLimit(100))
		require.NoError(t, db.Options().SetTransactionTimeout(60000))

		activeRunners, err := CreateOrOpenActiveRunners(db)
		require.NoError(t, err)

		taskDir, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)

		const rounds = 4
		for round := range rounds {
			seed1 := uint64(100 + 17*round)
			seed2 := uint64(200 + 31*round)
			t.Run(fmt.Sprintf("round_%d_seed_%d_%d", round, seed1, seed2), func(t *testing.T) {
				runnerID := fmt.Sprintf("runner-a-%d", round)
				reassignedRunnerID := fmt.Sprintf("runner-b-%d", round)

				taskSet, cancelTaskSet, err := CreateOrOpenTaskSetForRunner(db, runnerID)
				require.NoError(t, err)
				defer cancelTaskSet()

				reassignedTaskSet, cancelReassignedTaskSet, err := CreateOrOpenTaskSetForRunner(db, reassignedRunnerID)
				require.NoError(t, err)
				defer cancelReassignedTaskSet()

				pendingSetPath := []string{"drain_test_pending", fmt.Sprintf("round-%d", round)}
				pendingSet, cancelPendingSet, err := reliableset.CreateOrOpen(db, pendingSetPath)
				require.NoError(t, err)
				defer cancelPendingSet()

				setRunnerActive(t, db, activeRunners, runnerID, true)

				scenarioRng := rand.New(rand.NewPCG(seed1, seed2))
				taskCount := 320 + scenarioRng.IntN(120)
				taskIDs := make([]task.Id, 0, taskCount)
				for i := range taskCount {
					taskIDs = append(taskIDs, task.Id(fmt.Sprintf("drain-%02d-%03d", round, i)))
				}
				enqueueTasksWithAssignmentInBatches(
					t,
					db,
					taskSet,
					taskDir,
					taskIDs,
					task.LifecycleStatusRunning,
					stringPointer(runnerID),
				)

				mutatorCtx, cancelMutator := context.WithCancel(t.Context())
				defer cancelMutator()
				mutatorErrCh := make(chan error, 1)
				var mutationCount atomic.Int64

				go func() {
					mutatorRng := rand.New(rand.NewPCG(seed1^0x9e3779b97f4a7c15, seed2^0x517cc1b727220a95))
					for {
						select {
						case <-mutatorCtx.Done():
							mutatorErrCh <- nil
							return
						default:
						}

						id := taskIDs[mutatorRng.IntN(len(taskIDs))]
						op := mutatorRng.IntN(3)

						changedAny, err := db.Transact(func(tx fdb.Transaction) (any, error) {
							tkey, err := taskDir.Open(tx, id)
							if err != nil {
								if errors.Is(err, directory.ErrDirNotExists) {
									return false, nil
								}
								return false, err
							}

							state, err := task.ReadAssignmentState(tx, tkey)
							if err != nil {
								return false, err
							}
							if err := state.ValidateRunnerLifecycleInvariant(); err != nil {
								return false, err
							}
							isOnRunnerA, err := state.IsRunningOn(runnerID)
							if err != nil {
								return false, err
							}
							if !isOnRunnerA {
								return false, nil
							}

							switch op {
							case 0:
								if err := taskSet.Remove(tx, []byte(id)); err != nil {
									return false, err
								}
								if err := reassignedTaskSet.Add(tx, []byte(id)); err != nil {
									return false, err
								}
								tkey.LifecycleStatus().Set(tx, task.LifecycleStatusRunning)
								tkey.RunnerId().Set(tx, stringPointer(reassignedRunnerID))
							case 1:
								if err := taskSet.Remove(tx, []byte(id)); err != nil {
									return false, err
								}
								if err := pendingSet.Add(tx, []byte(id)); err != nil {
									return false, err
								}
								tkey.LifecycleStatus().Set(tx, task.LifecycleStatusPending)
								tkey.RunnerId().Set(tx, nil)
							default:
								if err := taskSet.Remove(tx, []byte(id)); err != nil {
									return false, err
								}
								if err := tkey.Clear(tx); err != nil {
									return false, err
								}
							}
							return true, nil
						})
						if err != nil {
							mutatorErrCh <- err
							return
						}
						if changedAny.(bool) {
							mutationCount.Add(1)
						}
						// Keep overlap high enough to trigger races, but avoid infinite
						// conflict storms that make progress impossible.
						time.Sleep(time.Duration(mutatorRng.IntN(3)+1) * time.Millisecond)
					}
				}()

				require.Eventually(
					t,
					func() bool { return mutationCount.Load() > 0 },
					2*time.Second,
					10*time.Millisecond,
					"mutator did not apply any changes before draining",
				)

				drainErrCh := make(chan error, 1)
				go func() {
					drainErrCh <- DrainTaskRunner(db, runnerID, activeRunners, taskSet, pendingSet, taskDir)
				}()
				// Maintain concurrent overlap for a bounded window, then let drain complete.
				time.Sleep(250 * time.Millisecond)
				cancelMutator()
				mutatorErr := <-mutatorErrCh
				var drainErr error
				select {
				case drainErr = <-drainErrCh:
				case <-time.After(30 * time.Second):
					t.Fatal("timeout waiting for drain to complete")
				}

				require.NoError(t, drainErr)
				require.NoError(t, mutatorErr)
				require.Greater(t, mutationCount.Load(), int64(0))
				require.False(t, isRunnerActive(t, db, activeRunners, runnerID))

				taskSetItems := readSetItems(t, db, taskSet)
				reassignedItems := readSetItems(t, db, reassignedTaskSet)
				pendingItems := readSetItems(t, db, pendingSet)
				assignments := readTaskAssignments(t, db, taskDir, taskIDs)

				for _, id := range taskIDs {
					snapshot, ok := assignments[id]
					require.True(t, ok)

					inTaskSet := taskSetItems.Contains(string(id))
					inReassignedSet := reassignedItems.Contains(string(id))
					inPendingSet := pendingItems.Contains(string(id))

					if !snapshot.Exists {
						require.Falsef(
							t,
							inTaskSet || inReassignedSet || inPendingSet,
							"deleted task %s should not remain in any set",
							id,
						)
						continue
					}

					switch snapshot.Status {
					case task.LifecycleStatusPending:
						require.Nilf(t, snapshot.RunnerID, "pending task %s must have nil runner", id)
						require.Falsef(t, inTaskSet, "pending task %s must not remain on drained runner set", id)
						require.Falsef(t, inReassignedSet, "pending task %s must not be on reassigned runner set", id)
						require.Truef(t, inPendingSet, "pending task %s must be in pending set", id)
					case task.LifecycleStatusRunning:
						require.NotNilf(t, snapshot.RunnerID, "running task %s must have runner", id)
						switch *snapshot.RunnerID {
						case runnerID:
							t.Fatalf("task %s still running on drained runner", id)
						case reassignedRunnerID:
							require.Falsef(t, inTaskSet, "task %s moved to runner-b but still in runner-a set", id)
							require.Truef(t, inReassignedSet, "task %s moved to runner-b but missing from runner-b set", id)
							require.Falsef(t, inPendingSet, "task %s running on runner-b must not be pending", id)
						default:
							t.Fatalf("task %s has unexpected runner id: %q", id, *snapshot.RunnerID)
						}
					default:
						t.Fatalf("task %s has unexpected lifecycle status: %s", id, snapshot.Status.String())
					}
				}
			})
		}
	})
}
