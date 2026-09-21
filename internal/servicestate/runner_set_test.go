package servicestate

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func testResourceRequest() *taskv1.TaskResourceRequest {
	return taskv1.TaskResourceRequest_builder{
		CpuMillis:   proto.Uint32(500),
		MemoryBytes: proto.Uint64(1024),
	}.Build()
}

func requireRunnerSetUtilization(t testing.TB, db dbutil.DbRoot, runnerSet *RunnerSet, expectedCpuMillis, expectedMemoryBytes int64) {
	t.Helper()

	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		cpuUtilization, err := runnerSet.GetUtilization(tx, UtilizationDimensionCPU)
		require.NoError(t, err)
		require.Equal(t, expectedCpuMillis, cpuUtilization)

		memoryUtilization, err := runnerSet.GetUtilization(tx, UtilizationDimensionMemory)
		require.NoError(t, err)
		require.Equal(t, expectedMemoryBytes, memoryUtilization)
		return nil, nil
	})
	require.NoError(t, err)
}

func TestRunnerSet(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		runnerSet, err := createOrOpenRunnerSet(db, db, "test-runner")
		require.NoError(t, err)
		require.NotNil(t, runnerSet)

		taskDir, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)

		taskID := task.Id("test-task")
		resourceRequest := testResourceRequest()

		taskKey, err := taskDir.Create(db, taskID)
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			taskKey.ResourceRequest().Set(tx, resourceRequest)
			return nil, nil
		})
		require.NoError(t, err)

		t.Run("adding tasks should automatically update the utilization aggregate", func(t *testing.T) {
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				err := runnerSet.Add(tx, taskKey)
				require.NoError(t, err)

				cpuUtilization, err := runnerSet.utilizationAggregate.get(tx, UtilizationDimensionCPU)
				require.NoError(t, err)
				require.Equal(t, int64(resourceRequest.GetCpuMillis()), cpuUtilization)

				memoryUtilization, err := runnerSet.utilizationAggregate.get(tx, UtilizationDimensionMemory)
				require.NoError(t, err)
				require.Equal(t, int64(resourceRequest.GetMemoryBytes()), memoryUtilization)

				return nil, nil
			})
			require.NoError(t, err)
		})

		t.Run("adding a task twice does not double count the aggregate", func(t *testing.T) {
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				err := runnerSet.Add(tx, taskKey)
				require.NoError(t, err)
				return nil, nil
			})
			require.NoError(t, err)

			requireRunnerSetUtilization(t, db, runnerSet, int64(resourceRequest.GetCpuMillis()), int64(resourceRequest.GetMemoryBytes()))
		})

		t.Run("removing tasks should automatically update the utilization aggregate", func(t *testing.T) {
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				err := runnerSet.Remove(tx, taskKey)
				require.NoError(t, err)

				cpuUtilization, err := runnerSet.utilizationAggregate.get(tx, UtilizationDimensionCPU)
				require.NoError(t, err)
				require.Equal(t, int64(0), cpuUtilization)

				memoryUtilization, err := runnerSet.utilizationAggregate.get(tx, UtilizationDimensionMemory)
				require.NoError(t, err)
				require.Equal(t, int64(0), memoryUtilization)

				return nil, nil
			})
			require.NoError(t, err)
		})

		t.Run("removing a task twice does not make the aggregate negative", func(t *testing.T) {
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				err := runnerSet.Remove(tx, taskKey)
				require.NoError(t, err)
				return nil, nil
			})
			require.NoError(t, err)

			requireRunnerSetUtilization(t, db, runnerSet, 0, 0)
		})

		t.Run("clearing the runner set should clear the task set and utilization aggregate", func(t *testing.T) {
			// first add another task
			taskKey2, err := taskDir.Create(db, task.Id("test-task-2"))
			require.NoError(t, err)
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				taskKey2.ResourceRequest().Set(tx, testResourceRequest())
				return nil, nil
			})
			require.NoError(t, err)

			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				err := runnerSet.Add(tx, taskKey2)
				require.NoError(t, err)
				return nil, nil
			})
			require.NoError(t, err)

			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				err := runnerSet.Clear(tx)
				require.NoError(t, err)
				return nil, nil
			})
			require.NoError(t, err)

			requireRunnerSetUtilization(t, db, runnerSet, 0, 0)

			items, _, err := runnerSet.Items(t.Context(), db.Database)
			require.NoError(t, err)
			require.Zero(t, items.Cardinality())
		})
	})
}

func TestRunnerSetClearLeavesNoKeys(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		runnerSet, err := createOrOpenRunnerSet(db, db, "test-runner")
		require.NoError(t, err)
		taskDir, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)
		taskKey, err := taskDir.Create(db, task.Id("test-task"))
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			taskKey.ResourceRequest().Set(tx, testResourceRequest())
			return nil, runnerSet.Add(tx, taskKey)
		})
		require.NoError(t, err)
		// the aggregate's keys live under the set's directory
		begin, end := runnerSet.utilizationAggregate.directory.FDBRangeKeys()
		countAggregateKeys := func() int {
			n, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				kvs, err := tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{}).GetSliceWithError()
				return len(kvs), err
			})
			require.NoError(t, err)
			return n.(int)
		}
		require.Equal(t, 2, countAggregateKeys())

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) { return nil, runnerSet.Clear(tx) })
		require.NoError(t, err)

		// the directory is gone, and nothing was written back under its old prefix
		_, err = openRunnerSet(db, db, "test-runner")
		require.ErrorIs(t, err, directory.ErrDirNotExists)
		require.Zero(t, countAggregateKeys(), "clearing must not recreate the aggregate under a removed directory")
	})
}
