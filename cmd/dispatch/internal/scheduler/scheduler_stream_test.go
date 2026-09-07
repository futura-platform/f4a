package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/futura-platform/f4a/cmd/dispatch/internal/k8s"
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

func TestLoopTickerPassAssignsTaskRequeuedWithinOneChunk(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-0"
		s, tasksDir, taskPlacer, activeRunnerSets := newSchedulerFixture(t, db, runnerID)
		s.cfg.MetricsInterval = 500 * time.Millisecond
		s.clients = &k8s.Clients{Core: fake.NewClientset()}
		taskID := task.Id("requeued-in-chunk-loop")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		loopErr := make(chan error, 1)
		go func() { loopErr <- s.commandRunners(ctx) }()

		// the initial pass assigns the seeded task
		require.Eventually(t, func() bool {
			status, _ := readTaskState(t, db, tasksDir, taskID)
			return status == task.LifecycleStatusRunning
		}, 10*time.Second, 20*time.Millisecond)

		// re-queue it in one transaction: the stream's chunk is a net no-op for
		// membership, so only a pass over the snapshot can pick it up again
		runnerSet, err := activeRunnerSets.open(runnerID)
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			taskKey, err := tasksDir.Open(tx, taskID)
			if err != nil {
				return nil, err
			}
			if err := taskPlacer.PlaceTaskIn(tx, servicestate.PlacementLocationPending, taskKey); err != nil {
				return nil, err
			}
			return nil, taskPlacer.PlaceTaskOnRunner(tx, runnerID, runnerSet, taskKey)
		})
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			taskKey, err := tasksDir.Open(tx, taskID)
			if err != nil {
				return nil, err
			}
			return nil, taskPlacer.PlaceTaskIn(tx, servicestate.PlacementLocationPending, taskKey)
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			status, runnerIDOnTask := readTaskState(t, db, tasksDir, taskID)
			return status == task.LifecycleStatusRunning && runnerIDOnTask != nil && *runnerIDOnTask == runnerID
		}, 10*time.Second, 20*time.Millisecond)
		requirePendingNotContainsTask(t, taskPlacer, taskID)

		cancel()
		select {
		case err := <-loopErr:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for the loop to stop")
		}
	})
}
