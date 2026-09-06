package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/require"

	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
)

func TestPeriodicPassAssignsTaskWhoseRequeueTheStreamCollapsed(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-0"
		s, tasksDir, taskPlacer, activeRunnerSets := newSchedulerFixture(t, db, runnerID)
		taskID := task.Id("collapsed-requeue")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		initial, events, errCh, err := taskPlacer.StreamPendingTasks(ctx)
		require.NoError(t, err)
		require.True(t, initial.Equal(taskIDSet(taskID)))
		pending := newPendingMirror(initial)

		// The scheduler assigns the task and a draining runner re-queues it
		// before the stream reads the log again: the chunk holds a Remove and
		// an Add for the same id and nets to no event. One transaction makes
		// that ordering certain; in production the two commits are seconds
		// apart and the stream's read lands after both.
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
		case batch := <-events:
			t.Fatalf("expected the collapsed chunk to emit nothing, got %d entries", len(batch))
		case err := <-errCh:
			t.Fatalf("stream error: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
		status, runnerIDOnTask := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, runnerIDOnTask)

		// Nothing was ever reported as a failure, so a scheduler that only
		// retries its failures would have nothing to attempt here; the
		// mirror still holds the task.
		require.True(t, pending.snapshot().Equal(taskIDSet(taskID)))

		failures, err := s.assignPending(ctx, pending.snapshot())
		require.NoError(t, err)
		requireAssignmentFailures(t, failures, nil, nil)
		status, runnerIDOnTask = readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, runnerID, *runnerIDOnTask)
		requirePendingNotContainsTask(t, taskPlacer, taskID)
	})
}
