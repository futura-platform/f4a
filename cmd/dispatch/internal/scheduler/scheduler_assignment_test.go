package scheduler

import (
	"log/slog"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestAssignPendingRetriesWhenResourcesAppear(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		s, tasksDir, pendingSet := newSchedulerFixture(t, db, "worker-0")
		taskID := task.Id("pending-task-retry")
		seedPendingTask(t, db, tasksDir, pendingSet, taskID)

		retryAssignLater, err := s.assignPending(t.Context(), []string{string(taskID)}, map[string]float64{})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{string(taskID)}, retryAssignLater.ToSlice())

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Equal(t, "", runnerID)
		requireSetContainsTask(t, db, pendingSet, taskID)

		retryAssignLater, err = s.assignPending(t.Context(), retryAssignLater.ToSlice(), map[string]float64{"worker-0": 0.8})
		require.NoError(t, err)
		require.Zero(t, retryAssignLater.Cardinality())

		status, runnerID = readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, "worker-0", runnerID)
		requireSetNotContainsTask(t, db, pendingSet, taskID)
	})
}

func TestAssignPendingSkipsMissingTasks(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		s, tasksDir, pendingSet := newSchedulerFixture(t, db, "worker-1")
		taskID := task.Id("pending-task-live")
		seedPendingTask(t, db, tasksDir, pendingSet, taskID)

		retryAssignLater, err := s.assignPending(
			t.Context(),
			[]string{"missing-task-id", string(taskID)},
			map[string]float64{"worker-1": 0.3},
		)
		require.NoError(t, err)
		require.Zero(t, retryAssignLater.Cardinality())

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, "worker-1", runnerID)
		requireSetNotContainsTask(t, db, pendingSet, taskID)
	})
}

func newSchedulerFixture(t *testing.T, db dbutil.DbRoot, workerID string) (*Scheduler, task.TasksDirectory, *reliableset.Set) {
	t.Helper()
	tasksDir, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	pendingSet, pendingSetCancel, err := servicestate.CreateOrOpenReadySet(db)
	require.NoError(t, err)
	t.Cleanup(pendingSetCancel)

	workerSet, workerSetCancel, err := pool.CreateOrOpenTaskSetForRunner(db, workerID)
	require.NoError(t, err)
	t.Cleanup(workerSetCancel)

	s := &Scheduler{
		cfg: Config{
			BatchTxParallelism: 1,
			Logger:             slog.Default(),
		},
		db:               db,
		taskDir:          tasksDir,
		pendingSet:       pendingSet,
		pendingSetCancel: pendingSetCancel,
		taskSets: map[string]*reliableset.Set{
			workerID: workerSet,
		},
		logger: slog.Default(),
	}
	return s, tasksDir, pendingSet
}

func seedPendingTask(t *testing.T, db dbutil.DbRoot, tasksDir task.TasksDirectory, pendingSet *reliableset.Set, id task.Id) {
	t.Helper()

	taskKey, err := tasksDir.Create(db, id)
	require.NoError(t, err)

	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		taskKey.LifecycleStatus().Set(tx, task.LifecycleStatusPending)
		taskKey.RunnerId().Set(tx, nil)
		if err := pendingSet.Add(tx, []byte(id)); err != nil {
			return nil, err
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func readTaskState(t *testing.T, db dbutil.DbRoot, tasksDir task.TasksDirectory, id task.Id) (task.LifecycleStatus, string) {
	t.Helper()
	taskKey, err := tasksDir.Open(db, id)
	require.NoError(t, err)

	var status task.LifecycleStatus
	var runnerID string
	_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		status = taskKey.LifecycleStatus().Get(tx).MustGet()
		runnerID = *taskKey.RunnerId().Get(tx).MustGet()
		return nil, nil
	})
	require.NoError(t, err)
	return status, runnerID
}

func requireSetContainsTask(t *testing.T, db dbutil.DbRoot, set *reliableset.Set, id task.Id) {
	t.Helper()
	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		items, _, err := set.Items(tx)
		if err != nil {
			return nil, err
		}
		require.True(t, items.ContainsOne(string(id)), "expected task %q in set, items=%v", id, items.ToSlice())
		return nil, nil
	})
	require.NoError(t, err)
}

func requireSetNotContainsTask(t *testing.T, db dbutil.DbRoot, set *reliableset.Set, id task.Id) {
	t.Helper()
	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		items, _, err := set.Items(tx)
		if err != nil {
			return nil, err
		}
		require.False(t, items.ContainsOne(string(id)))
		return nil, nil
	})
	require.NoError(t, err)
}
