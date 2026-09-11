package scheduler

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestAssignmentUsesRecreatedRunnerQueue(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-recreated-0"
		s, taskDir, placer, oldSet := newSchedulerFixture(t, db, runnerID)
		require.NoError(t, pool.DrainTaskRunner(t.Context(), db, placer, runnerID, s.activeRunners, oldSet, taskDir))
		currentSet, err := servicestate.CreateOrOpenTaskSetForRunner(db, db, runnerID)
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			s.activeRunners.SetActive(tx, runnerID, true)
			return nil, nil
		})
		require.NoError(t, err)
		id := task.Id("recreated-runner-task")
		seedPendingTask(t, db, taskDir, placer, id)
		failures, err := s.assignPending(t.Context(), taskIDSet(id))
		require.NoError(t, err)
		requireAssignmentFailures(t, failures, nil, nil)
		status, assignedRunner := readTaskState(t, db, taskDir, id)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, runnerID, *assignedRunner)
		items, _, err := currentSet.Items(t.Context(), db.Database)
		require.NoError(t, err)
		require.True(t, items.Contains(id), "running task must exist in the recreated runner queue")
	})
}

func TestAssignmentConflictsWithConcurrentQueueReplacement(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-replaced-0"
		s, taskDir, placer, _ := newSchedulerFixture(t, db, runnerID)
		id := task.Id("concurrent-queue-replacement")
		seedPendingTask(t, db, taskDir, placer, id)
		tx, err := db.CreateTransaction()
		require.NoError(t, err)
		defer tx.Cancel()
		oldSet, err := servicestate.OpenTaskSetForRunner(tx, db, runnerID)
		require.NoError(t, err)
		require.NoError(t, s.assignTask(tx, id, runnerID, oldSet))
		_, err = db.Root.Remove(db, []string{"task_queue", runnerID})
		require.NoError(t, err)
		currentSet, err := servicestate.CreateOrOpenTaskSetForRunner(db, db, runnerID)
		require.NoError(t, err)
		var conflict fdb.Error
		require.ErrorAs(t, tx.Commit().Get(), &conflict)
		require.Equal(t, 1020, conflict.Code, "replacing a queue must invalidate its in-flight assignment")
		status, assignedRunner := readTaskState(t, db, taskDir, id)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, assignedRunner)
		failures, err := s.assignPending(t.Context(), taskIDSet(id))
		require.NoError(t, err)
		requireAssignmentFailures(t, failures, nil, nil)
		items, _, err := currentSet.Items(t.Context(), db.Database)
		require.NoError(t, err)
		require.True(t, items.Contains(id))
	})
}
