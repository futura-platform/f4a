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
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
)

func scoresMap(m map[string]float64) *xsync.Map[string, float64] {
	out := xsync.NewMap[string, float64](xsync.WithPresize(len(m)))
	for k, v := range m {
		out.Store(k, v)
	}
	return out
}

func TestAssignPendingRetriesWhenResourcesAppear(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		s, tasksDir, pendingSet, activeRunnerSets := newSchedulerFixture(t, db, "worker-0")
		taskID := task.Id("pending-task-retry")
		seedPendingTask(t, db, tasksDir, pendingSet, taskID)

		retryAssignLater, err := s.assignPending(t.Context(), []string{string(taskID)}, scoresMap(map[string]float64{}), newMockedRunnerSetCache(db, map[string]*reliableset.Set{}))
		require.NoError(t, err)
		require.ElementsMatch(t, []string{string(taskID)}, retryAssignLater.ToSlice())

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, runnerID)
		requireSetContainsTask(t, db, pendingSet, taskID)

		retryAssignLater, err = s.assignPending(t.Context(), retryAssignLater.ToSlice(), scoresMap(map[string]float64{"worker-0": 0.8}), activeRunnerSets)
		require.NoError(t, err)
		require.Zero(t, retryAssignLater.Cardinality())

		status, runnerID = readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, "worker-0", *runnerID)
		requireSetNotContainsTask(t, db, pendingSet, taskID)
	})
}

func TestAssignPendingSkipsInactiveRunnerAndRetries(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const inactiveRunner = "worker-inactive"

		s, tasksDir, pendingSet, activeRunnerSets := newSchedulerFixture(t, db, inactiveRunner)
		taskID := task.Id("pending-task-inactive-runner")
		seedPendingTask(t, db, tasksDir, pendingSet, taskID)

		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			s.activeRunners.SetActive(tx, inactiveRunner, false)
			return nil, nil
		})
		require.NoError(t, err)

		scores := scoresMap(map[string]float64{inactiveRunner: 0.5})
		retryAssignLater, err := s.assignPending(
			t.Context(),
			[]string{string(taskID)},
			scores,
			activeRunnerSets,
		)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{string(taskID)}, retryAssignLater.ToSlice())
		_, ok := scores.Load(inactiveRunner)
		require.False(t, ok, "inactive runner score should be evicted to avoid repeated retries")

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, runnerID)
		requireSetContainsTask(t, db, pendingSet, taskID)
	})
}

func TestAssignPendingSkipsRunnerWithMissingSetAndRetries(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const (
			healthyRunner    = "worker-healthy"
			missingSetRunner = "worker-missing-set"
		)

		s, tasksDir, pendingSet, activeRunnerSets := newSchedulerFixture(t, db, healthyRunner)
		taskID := task.Id("pending-task-missing-runner-set")
		seedPendingTask(t, db, tasksDir, pendingSet, taskID)

		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			s.activeRunners.SetActive(tx, missingSetRunner, true)
			return nil, nil
		})
		require.NoError(t, err)

		scores := scoresMap(map[string]float64{missingSetRunner: 0.8})
		retryAssignLater, err := s.assignPending(
			t.Context(),
			[]string{string(taskID)},
			scores,
			activeRunnerSets,
		)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{string(taskID)}, retryAssignLater.ToSlice())
		_, ok := scores.Load(missingSetRunner)
		require.False(t, ok, "runner score should be evicted when runner queue directory is gone")

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, runnerID)
		requireSetContainsTask(t, db, pendingSet, taskID)
	})
}

func TestAssignPendingSkipsMissingTasks(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		s, tasksDir, pendingSet, activeRunnerSets := newSchedulerFixture(t, db, "worker-1")
		taskID := task.Id("pending-task-live")
		seedPendingTask(t, db, tasksDir, pendingSet, taskID)

		retryAssignLater, err := s.assignPending(
			t.Context(),
			[]string{"missing-task-id", string(taskID)},
			scoresMap(map[string]float64{"worker-1": 0.3}),
			activeRunnerSets,
		)
		require.NoError(t, err)
		require.Zero(t, retryAssignLater.Cardinality())

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, "worker-1", *runnerID)
		requireSetNotContainsTask(t, db, pendingSet, taskID)
	})
}

func TestAssignPendingFailsInvariantViolation(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		s, tasksDir, pendingSet, activeRunnerSets := newSchedulerFixture(t, db, "worker-2")
		taskID := task.Id("pending-task-invalid-runner")
		seedPendingTask(t, db, tasksDir, pendingSet, taskID)

		taskKey, err := tasksDir.Open(db, taskID)
		require.NoError(t, err)
		staleRunner := "stale-runner"
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			taskKey.RunnerId().Set(tx, &staleRunner)
			taskKey.LifecycleStatus().Set(tx, task.LifecycleStatusPending)
			return nil, nil
		})
		require.NoError(t, err)

		_, err = s.assignPending(
			t.Context(),
			[]string{string(taskID)},
			scoresMap(map[string]float64{"worker-2": 0.7}),
			activeRunnerSets,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, task.ErrNonRunningTaskHasRunnerID)
	})
}

func newSchedulerFixture(t *testing.T, db dbutil.DbRoot, workerID string) (
	*Scheduler,
	task.TasksDirectory,
	*reliableset.Set,
	*runnerSetCache,
) {
	t.Helper()
	tasksDir, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	pendingSet, err := servicestate.CreateOrOpenReadySet(db, db)
	require.NoError(t, err)

	workerSet, err := pool.CreateOrOpenTaskSetForRunner(db, db, workerID)
	require.NoError(t, err)

	activeRunners, err := pool.CreateOrOpenActiveRunners(db)
	require.NoError(t, err)
	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		activeRunners.SetActive(tx, workerID, true)
		return nil, nil
	})
	require.NoError(t, err)

	s := &Scheduler{
		cfg: Config{
			BatchTxParallelism: 1,
			Logger:             slog.Default(),
		},
		db:            db,
		taskDir:       tasksDir,
		pendingSet:    pendingSet,
		activeRunners: activeRunners,
		logger:        slog.Default(),
	}
	return s, tasksDir, pendingSet, newMockedRunnerSetCache(db, map[string]*reliableset.Set{workerID: workerSet})
}

func newMockedRunnerSetCache(db dbutil.DbRoot, src map[string]*reliableset.Set) *runnerSetCache {
	cache := &runnerSetCache{
		db:         db,
		activeSets: xsync.NewMap[string, *reliableset.Set](xsync.WithPresize(len(src))),
	}
	for k, v := range src {
		cache.activeSets.Store(k, v)
	}
	return cache
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

func readTaskState(t *testing.T, db dbutil.DbRoot, tasksDir task.TasksDirectory, id task.Id) (task.LifecycleStatus, *string) {
	t.Helper()
	taskKey, err := tasksDir.Open(db, id)
	require.NoError(t, err)

	var status task.LifecycleStatus
	var runnerID *string
	_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		status = taskKey.LifecycleStatus().Get(tx).MustGet()
		runnerID = taskKey.RunnerId().Get(tx).MustGet()
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
