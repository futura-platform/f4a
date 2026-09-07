package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/require"

	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
)

func TestPassAssignsTaskRequeuedWithinOneChunk(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-0"
		s, tasksDir, taskPlacer, activeRunnerSets := newSchedulerFixture(t, db, runnerID)
		taskID := task.Id("requeued-in-chunk")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		pending, err := taskPlacer.StreamPendingTasks(ctx)
		require.NoError(t, err)
		require.True(t, pending.Snapshot().Equal(taskIDSet(taskID)))

		// assign then re-queue in one transaction, so the chunk holds a Remove
		// and an Add for the same id and its net effect on membership is nothing
		runnerSet, err := activeRunnerSets.open(runnerID)
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			taskKey, err := tasksDir.Open(tx, taskID)
			if err != nil {
				return nil, err
			}
			if err := taskPlacer.PlaceTaskOnRunner(tx, runnerID, runnerSet, taskKey); err != nil {
				return nil, err
			}
			return nil, taskPlacer.PlaceTaskIn(tx, servicestate.PlacementLocationPending, taskKey)
		})
		require.NoError(t, err)

		select {
		case batch := <-pending.Events():
			require.Equal(t, []reliableset.TLogEntry[task.Id]{
				{Op: reliableset.LogOperationRemove, Value: taskID},
				{Op: reliableset.LogOperationAdd, Value: taskID},
			}, batch)
		case err := <-pending.Err():
			t.Fatalf("stream error: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for the chunk")
		}
		status, runnerIDOnTask := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, runnerIDOnTask)

		require.True(t, pending.Snapshot().Equal(taskIDSet(taskID)))

		failures, err := s.assignPending(ctx, pending.Snapshot())
		require.NoError(t, err)
		requireAssignmentFailures(t, failures, nil, nil)
		status, runnerIDOnTask = readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, runnerID, *runnerIDOnTask)
		requirePendingNotContainsTask(t, taskPlacer, taskID)
	})
}
