package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
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
		s, tasksDir, taskPlacer, runnerSet := newSchedulerFixture(t, db, runnerID)
		taskID := task.Id("requeued-in-chunk")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		pending, err := taskPlacer.StreamPendingTasks(ctx)
		require.NoError(t, err)
		require.True(t, pending.Snapshot().Equal(taskIDSet(taskID)))

		// assign then re-queue in one transaction, so the chunk holds a Remove
		// and an Add for the same id and its net effect on membership is nothing
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

func TestLoopPassPlansFromTheSnapshot(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-0"
		s, tasksDir, taskPlacer, _ := newSchedulerFixture(t, db, runnerID)
		s.cfg.MetricsInterval = time.Hour
		s.clients = &k8s.Clients{Core: fake.NewClientset()}
		taskID := task.Id("requeued-running-task")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		loopErr := make(chan error, 1)
		go func() { loopErr <- s.commandRunners(ctx) }()

		require.Eventually(t, func() bool {
			status, _ := readTaskState(t, db, tasksDir, taskID)
			return status == task.LifecycleStatusRunning
		}, 10*time.Second, 20*time.Millisecond)

		// re-queue the running task: the event wakes the loop, and the task was
		// never a recorded failure, so only a pass planned from the snapshot
		// assigns it again
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			taskKey, err := tasksDir.Open(tx, taskID)
			if err != nil {
				return nil, err
			}
			return nil, taskPlacer.PlaceTaskIn(tx, servicestate.PlacementLocationPending, taskKey)
		})
		require.NoError(t, err)

		requireEventuallyRunningOn(t, db, tasksDir, taskID, runnerID)
		requirePendingNotContainsTask(t, taskPlacer, taskID)

		cancel()
		requireLoopStops(t, loopErr)
	})
}

func TestLoopTickerPassRetriesTaskNoReadyRunnerCouldTake(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-0"
		s, tasksDir, taskPlacer, _ := newSchedulerFixture(t, db, runnerID)
		s.cfg.MetricsInterval = 500 * time.Millisecond
		s.clients = &k8s.Clients{Core: fake.NewClientset()}
		lister := &readinessPodLister{pod: podListerForRunners(runnerID).pods[0]}
		s.runnerPodLister = lister
		taskID := task.Id("unplaceable-until-ready")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		loopErr := make(chan error, 1)
		go func() { loopErr <- s.commandRunners(ctx) }()

		// the initial pass finds no ready runner and records the task as a failure
		require.Eventually(t, func() bool { return lister.lists.Load() >= 1 }, 10*time.Second, 20*time.Millisecond)
		status, _ := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)

		// readiness is not a pending-set change, so nothing wakes the loop but
		// the ticker
		lister.ready.Store(true)
		requireEventuallyRunningOn(t, db, tasksDir, taskID, runnerID)
		requirePendingNotContainsTask(t, taskPlacer, taskID)

		cancel()
		requireLoopStops(t, loopErr)
	})
}

// readinessPodLister serves one runner pod whose readiness the test controls.
type readinessPodLister struct {
	pod   *corev1.Pod
	ready atomic.Bool
	lists atomic.Int32
}

func (l *readinessPodLister) current() *corev1.Pod {
	pod := l.pod.DeepCopy()
	status := corev1.ConditionFalse
	if l.ready.Load() {
		status = corev1.ConditionTrue
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
	return pod
}

func (l *readinessPodLister) List(labels.Selector) ([]*corev1.Pod, error) {
	l.lists.Add(1)
	return []*corev1.Pod{l.current()}, nil
}

func (l *readinessPodLister) Get(name string) (*corev1.Pod, error) {
	if name != l.pod.Name {
		return nil, fmt.Errorf("pod %q not found", name)
	}
	return l.current(), nil
}

func requireEventuallyRunningOn(t *testing.T, db dbutil.DbRoot, tasksDir task.TasksDirectory, id task.Id, runnerID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		status, runnerIDOnTask := readTaskState(t, db, tasksDir, id)
		return status == task.LifecycleStatusRunning && runnerIDOnTask != nil && *runnerIDOnTask == runnerID
	}, 10*time.Second, 20*time.Millisecond)
}

func requireLoopStops(t *testing.T, loopErr <-chan error) {
	t.Helper()
	select {
	case err := <-loopErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the loop to stop")
	}
}
