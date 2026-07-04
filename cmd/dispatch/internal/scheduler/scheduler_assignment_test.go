package scheduler

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func TestAssignPendingRetriesWhenResourcesAppear(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		s, tasksDir, taskPlacer, activeRunnerSets := newSchedulerFixture(t, db, "worker-0")
		taskID := task.Id("pending-task-retry")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		s.runnerPodLister = podListerForRunners()
		s.activeRunnerSets = newMockedRunnerSetCache(db, map[string]*servicestate.RunnerSet{})
		assignmentFailures, err := s.assignPending(t.Context(), taskIDSet(taskID))
		require.NoError(t, err)
		requireAssignmentFailures(t, assignmentFailures, []task.Id{taskID}, nil)

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, runnerID)
		requirePendingContainsTask(t, taskPlacer, taskID)

		s.runnerPodLister = podListerForRunners("worker-0")
		s.activeRunnerSets = activeRunnerSets
		assignmentFailures, err = s.assignPending(t.Context(), assignmentFailures.All())
		require.NoError(t, err)
		requireAssignmentFailures(t, assignmentFailures, nil, nil)

		status, runnerID = readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, "worker-0", *runnerID)
		requirePendingNotContainsTask(t, taskPlacer, taskID)
	})
}

func TestAssignPendingSkipsInactiveRunnerAndRetries(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const inactiveRunner = "worker-inactive"

		s, tasksDir, taskPlacer, _ := newSchedulerFixture(t, db, inactiveRunner)
		taskID := task.Id("pending-task-inactive-runner")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			s.activeRunners.SetActive(tx, inactiveRunner, false)
			return nil, nil
		})
		require.NoError(t, err)

		assignmentFailures, err := s.assignPending(
			t.Context(),
			taskIDSet(taskID),
		)
		require.NoError(t, err)
		requireAssignmentFailures(t, assignmentFailures, []task.Id{taskID}, nil)

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, runnerID)
		requirePendingContainsTask(t, taskPlacer, taskID)
	})
}

func TestAssignPendingSkipsRunnerWithMissingSetAndRetries(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const (
			healthyRunner    = "worker-healthy"
			missingSetRunner = "worker-missing-set"
		)

		s, tasksDir, taskPlacer, _ := newSchedulerFixture(t, db, healthyRunner)
		taskID := task.Id("pending-task-missing-runner-set")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			s.activeRunners.SetActive(tx, missingSetRunner, true)
			return nil, nil
		})
		require.NoError(t, err)

		s.runnerPodLister = podListerForRunners(missingSetRunner)
		assignmentFailures, err := s.assignPending(
			t.Context(),
			taskIDSet(taskID),
		)
		require.NoError(t, err)
		requireAssignmentFailures(t, assignmentFailures, []task.Id{taskID}, nil)

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusPending, status)
		require.Nil(t, runnerID)
		requirePendingContainsTask(t, taskPlacer, taskID)
	})
}

func TestAssignPendingSkipsMissingTasks(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		s, tasksDir, taskPlacer, _ := newSchedulerFixture(t, db, "worker-1")
		taskID := task.Id("pending-task-live")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

		assignmentFailures, err := s.assignPending(
			t.Context(),
			taskIDSet(task.Id("missing-task-id"), taskID),
		)
		require.NoError(t, err)
		requireAssignmentFailures(t, assignmentFailures, nil, nil)

		status, runnerID := readTaskState(t, db, tasksDir, taskID)
		require.Equal(t, task.LifecycleStatusRunning, status)
		require.Equal(t, "worker-1", *runnerID)
		requirePendingNotContainsTask(t, taskPlacer, taskID)
	})
}

func TestAssignPendingFailsInvariantViolation(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		s, tasksDir, taskPlacer, _ := newSchedulerFixture(t, db, "worker-2")
		taskID := task.Id("pending-task-invalid-runner")
		seedPendingTask(t, db, tasksDir, taskPlacer, taskID)

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
			taskIDSet(taskID),
		)
		require.Error(t, err)
		require.ErrorIs(t, err, task.ErrNonRunningTaskHasRunnerID)
	})
}

func taskIDSet(ids ...task.Id) mapset.Set[task.Id] {
	return mapset.NewSet(ids...)
}

func requireAssignmentFailures(
	t testing.TB,
	failures assignmentFailures,
	expectedNoResources []task.Id,
	expectedRunnerInactive []task.Id,
) {
	t.Helper()

	require.ElementsMatch(t, expectedNoResources, failures.noResources.ToSlice())
	require.ElementsMatch(t, expectedRunnerInactive, failures.runnerInactive.ToSlice())

	expectedAll := append([]task.Id{}, expectedNoResources...)
	expectedAll = append(expectedAll, expectedRunnerInactive...)
	require.ElementsMatch(t, expectedAll, failures.All().ToSlice())
}

func newSchedulerFixture(t *testing.T, db dbutil.DbRoot, workerID string) (
	*Scheduler,
	task.TasksDirectory,
	*servicestate.TaskPlacer,
	*runnerSetCache,
) {
	t.Helper()
	tasksDir, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	taskPlacer, _, err := servicestate.CreateOrOpenTaskPlacer(db)
	require.NoError(t, err)

	workerSet, err := servicestate.CreateOrOpenTaskSetForRunner(db, db, workerID)
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
		taskPlacer:    taskPlacer,
		activeRunners: activeRunners,
		logger:        slog.Default(),
	}
	activeRunnerSets := newMockedRunnerSetCache(db, map[string]*servicestate.RunnerSet{workerID: workerSet})
	s.activeRunnerSets = activeRunnerSets
	s.runnerPodLister = podListerForRunners(workerID)
	return s, tasksDir, taskPlacer, activeRunnerSets
}

type staticPodNamespaceLister struct {
	pods []*corev1.Pod
}

func podListerForRunners(runnerIDs ...string) staticPodNamespaceLister {
	pods := make([]*corev1.Pod, 0, len(runnerIDs))
	for _, runnerID := range runnerIDs {
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: runnerID},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "runner",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1000m"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				}},
			},
		})
	}
	return staticPodNamespaceLister{pods: pods}
}

func (l staticPodNamespaceLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return l.pods, nil
}

func (l staticPodNamespaceLister) Get(name string) (*corev1.Pod, error) {
	for _, pod := range l.pods {
		if pod.Name == name {
			return pod, nil
		}
	}
	return nil, fmt.Errorf("pod %q not found", name)
}

func newMockedRunnerSetCache(db dbutil.DbRoot, src map[string]*servicestate.RunnerSet) *runnerSetCache {
	cache := &runnerSetCache{
		db:         db,
		activeSets: xsync.NewMap[string, *servicestate.RunnerSet](xsync.WithPresize(len(src))),
	}
	for k, v := range src {
		cache.activeSets.Store(k, v)
	}
	return cache
}

func seedPendingTask(t *testing.T, db dbutil.DbRoot, tasksDir task.TasksDirectory, taskPlacer *servicestate.TaskPlacer, id task.Id) {
	t.Helper()

	taskKey, err := tasksDir.Create(db, id)
	require.NoError(t, err)

	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		taskKey.ResourceRequest().Set(tx, taskv1.TaskResourceRequest_builder{
			CpuMillis:   proto.Uint32(100),
			MemoryBytes: proto.Uint64(128 * 1024 * 1024),
		}.Build())
		if err := taskPlacer.PlaceTaskIn(tx, servicestate.PlacementLocationPending, taskKey); err != nil {
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

func requirePendingContainsTask(t *testing.T, taskPlacer *servicestate.TaskPlacer, id task.Id) {
	t.Helper()
	items, _, err := taskPlacer.PendingTasks(t.Context())
	require.NoError(t, err)
	require.True(t, items.ContainsOne(id), "expected task %q in set, items=%v", id, items.ToSlice())
}

func requirePendingNotContainsTask(t *testing.T, taskPlacer *servicestate.TaskPlacer, id task.Id) {
	t.Helper()
	items, _, err := taskPlacer.PendingTasks(t.Context())
	require.NoError(t, err)
	require.False(t, items.ContainsOne(id))
}
