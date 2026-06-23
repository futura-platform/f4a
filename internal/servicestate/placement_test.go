package servicestate

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestTaskPlacer(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		placer, runCompactor, err := CreateOrOpenTaskPlacer(db)
		require.NoError(t, err)
		require.NotNil(t, placer)
		require.NotNil(t, runCompactor)

		taskDir, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)
		require.NotNil(t, taskDir)

		suspendedSet, err := createOrOpenSuspendedSet(db, db)
		require.NoError(t, err)

		pendingSet, err := createOrOpenReadySet(db, db)
		require.NoError(t, err)

		const testingTaskCpuMillis = 100
		const testingTaskMemoryBytes = 1024
		makeTestingTask := func(t *testing.T) (task.TaskKey, error) {
			tkey, err := taskDir.Create(db, task.Id(t.Name()))
			if err != nil {
				return task.TaskKey{}, err
			}

			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				tkey.ResourceRequest().Set(tx, &taskv1.TaskResourceRequest{
					CpuMillis:   testingTaskCpuMillis,
					MemoryBytes: testingTaskMemoryBytes,
				})
				return nil, nil
			})
			if err != nil {
				return task.TaskKey{}, err
			}
			t.Cleanup(func() {
				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					err := placer.PlaceTaskIn(tx, PlacementLocationNowhere, tkey)
					if err != nil {
						return nil, err
					}
					requireTaskPlacerUtilization(t, tx, placer, 0, 0, 0, 0)

					return nil, tkey.Clear(tx)
				})
				require.NoError(t, err)
			})
			return tkey, nil
		}

		t.Run("should start with zero utilization", func(t *testing.T) {
			_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				requireTaskPlacerUtilization(t, tx, placer, 0, 0, 0, 0)
				return nil, nil
			})
			require.NoError(t, err)
		})

		const runnerId = "runner-1"
		runnerSet, err := createOrOpenRunnerSet(db, db, runnerId)
		require.NoError(t, err)
		require.NotNil(t, runnerSet)
		t.Run("can place an unseen task into:", func(t *testing.T) {
			t.Run("the suspended queue", func(t *testing.T) {
				tkey, err := makeTestingTask(t)
				require.NoError(t, err)
				require.NotNil(t, tkey)

				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					return nil, placer.PlaceTaskIn(tx, PlacementLocationSuspended, tkey)
				})
				require.NoError(t, err)

				items, _, err := suspendedSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.Contains(t, items.ToSlice(), tkey.Id())

				_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
					requireTaskPlacerUtilization(t, tx, placer, 0, 0, testingTaskCpuMillis, testingTaskMemoryBytes)
					return nil, nil
				})
				require.NoError(t, err)
			})
			t.Run("the pending queue", func(t *testing.T) {
				tkey, err := makeTestingTask(t)
				require.NoError(t, err)
				require.NotNil(t, tkey)

				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					return nil, placer.PlaceTaskIn(tx, PlacementLocationPending, tkey)
				})
				require.NoError(t, err)

				items, _, err := pendingSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.Contains(t, items.ToSlice(), tkey.Id())

				_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
					requireTaskPlacerUtilization(t, tx, placer, testingTaskCpuMillis, testingTaskMemoryBytes, 0, 0)
					return nil, nil
				})
				require.NoError(t, err)
			})
			t.Run("a running queue", func(t *testing.T) {
				runnerPlacer, runCompactor, err := CreateOrOpenTaskPlacer(db)
				require.NoError(t, err)
				require.NotNil(t, runnerPlacer)
				require.NotNil(t, runCompactor)

				tkey, err := makeTestingTask(t)
				require.NoError(t, err)
				require.NotNil(t, tkey)

				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					return nil, runnerPlacer.PlaceTaskOnRunner(tx, runnerId, runnerSet, tkey)
				})
				require.ErrorIs(t, err, ErrTaskNotInPendingState)

				items, _, err := runnerSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.NotContains(t, items.ToSlice(), tkey.Id())

				_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
					requireTaskPlacerUtilization(t, tx, placer, 0, 0, 0, 0)
					return nil, nil
				})
				require.NoError(t, err)
			})
		})

		t.Run("can place a preexisting task into:", func(t *testing.T) {
			makeAndPlaceTestingTask := func(t *testing.T, placer *TaskPlacer, location PlacementLocation) (task.TaskKey, error) {
				tkey, err := makeTestingTask(t)
				if err != nil {
					return task.TaskKey{}, err
				}
				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					return nil, placer.PlaceTaskIn(tx, location, tkey)
				})
				return tkey, err
			}
			t.Run("the suspended queue", func(t *testing.T) {
				tkey, err := makeAndPlaceTestingTask(t, placer, PlacementLocationPending)
				require.NoError(t, err)
				require.NotNil(t, tkey)

				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					return nil, placer.PlaceTaskIn(tx, PlacementLocationSuspended, tkey)
				})
				require.NoError(t, err)

				suspendedItems, _, err := suspendedSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.Contains(t, suspendedItems.ToSlice(), tkey.Id())

				pendingItems, _, err := pendingSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.NotContains(t, pendingItems.ToSlice(), tkey.Id())

				_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
					requireTaskPlacerUtilization(t, tx, placer, 0, 0, testingTaskCpuMillis, testingTaskMemoryBytes)
					return nil, nil
				})
				require.NoError(t, err)
			})
			t.Run("the pending queue", func(t *testing.T) {
				tkey, err := makeAndPlaceTestingTask(t, placer, PlacementLocationSuspended)
				require.NoError(t, err)
				require.NotNil(t, tkey)

				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					return nil, placer.PlaceTaskIn(tx, PlacementLocationPending, tkey)
				})
				require.NoError(t, err)

				pendingItems, _, err := pendingSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.Contains(t, pendingItems.ToSlice(), tkey.Id())

				suspendedItems, _, err := suspendedSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.NotContains(t, suspendedItems.ToSlice(), tkey.Id())

				_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
					requireTaskPlacerUtilization(t, tx, placer, testingTaskCpuMillis, testingTaskMemoryBytes, 0, 0)
					return nil, nil
				})
				require.NoError(t, err)
			})
			t.Run("a running queue", func(t *testing.T) {
				runnerPlacer, runCompactor, err := CreateOrOpenTaskPlacer(db)
				require.NoError(t, err)
				require.NotNil(t, runnerPlacer)
				require.NotNil(t, runCompactor)

				tkey, err := makeAndPlaceTestingTask(t, placer, PlacementLocationPending)
				require.NoError(t, err)
				require.NotNil(t, tkey)

				_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
					return nil, runnerPlacer.PlaceTaskOnRunner(tx, runnerId, runnerSet, tkey)
				})
				require.NoError(t, err)

				items, _, err := runnerSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.Contains(t, items.ToSlice(), tkey.Id())

				pendingItems, _, err := pendingSet.Items(t.Context(), db.Database)
				require.NoError(t, err)
				require.NotContains(t, pendingItems.ToSlice(), tkey.Id())

				_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
					requireTaskPlacerUtilization(t, tx, placer, testingTaskCpuMillis, testingTaskMemoryBytes, 0, 0)
					return nil, nil
				})
				require.NoError(t, err)
			})
		})
	})
}

func requireTaskPlacerUtilization(
	t testing.TB,
	tx fdb.ReadTransaction,
	placer *TaskPlacer,
	expectedActiveCpuMillis,
	expectedActiveMemoryBytes,
	expectedSuspendedCpuMillis,
	expectedSuspendedMemoryBytes int64,
) {
	t.Helper()

	activeCpuUtilization, err := placer.GetActiveDemandUtilization(tx, UtilizationDimensionCPU)
	require.NoError(t, err)
	require.Equal(t, expectedActiveCpuMillis, activeCpuUtilization)

	activeMemoryUtilization, err := placer.GetActiveDemandUtilization(tx, UtilizationDimensionMemory)
	require.NoError(t, err)
	require.Equal(t, expectedActiveMemoryBytes, activeMemoryUtilization)

	suspendedCpuUtilization, err := placer.GetSuspendedUtilization(tx, UtilizationDimensionCPU)
	require.NoError(t, err)
	require.Equal(t, expectedSuspendedCpuMillis, suspendedCpuUtilization)

	suspendedMemoryUtilization, err := placer.GetSuspendedUtilization(tx, UtilizationDimensionMemory)
	require.NoError(t, err)
	require.Equal(t, expectedSuspendedMemoryBytes, suspendedMemoryUtilization)
}
