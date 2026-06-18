package pool

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func testResourceRequest() *taskv1.TaskResourceRequest {
	return &taskv1.TaskResourceRequest{
		CpuMillis:   500,
		MemoryBytes: 1024,
	}
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
				require.Equal(t, int64(resourceRequest.CpuMillis), cpuUtilization)

				memoryUtilization, err := runnerSet.utilizationAggregate.get(tx, UtilizationDimensionMemory)
				require.NoError(t, err)
				require.Equal(t, int64(resourceRequest.MemoryBytes), memoryUtilization)

				return nil, nil
			})
			require.NoError(t, err)
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
	})
}
