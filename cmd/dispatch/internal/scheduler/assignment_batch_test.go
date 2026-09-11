package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestAssignPendingConcurrentBatchesPreserveQueuesAndUtilization(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const perRunner = 512
		runnerIDs := []string{"worker-0", "worker-1", "worker-2"}
		s, tasksDir, placer, _ := newSchedulerFixture(t, db, runnerIDs[0])
		s.cfg.BatchTxParallelism = DefaultBatchParallelism
		pods := podListerForRunners(runnerIDs...)
		for _, pod := range pods.pods {
			pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("512m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			}
			_, err := servicestate.CreateOrOpenTaskSetForRunner(db, db, pod.Name)
			require.NoError(t, err)
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				s.activeRunners.SetActive(tx, pod.Name, true)
				return nil, nil
			})
			require.NoError(t, err)
		}
		s.runnerPodLister = pods
		ids := mapset.NewSet[task.Id]()
		for offset := 0; offset < perRunner*len(runnerIDs); offset += 64 {
			created, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				batch := make([]task.Id, 0, 64)
				for i := offset; i < offset+64; i++ {
					id := task.Id(fmt.Sprintf("batch-task-%04d", i))
					key, err := tasksDir.Create(tx, id)
					if err != nil {
						return nil, err
					}
					key.ResourceRequest().Set(tx, taskv1.TaskResourceRequest_builder{
						CpuMillis: proto.Uint32(1), MemoryBytes: proto.Uint64(1024 * 1024),
					}.Build())
					if err := placer.PlaceTaskIn(tx, servicestate.PlacementLocationPending, key); err != nil {
						return nil, err
					}
					batch = append(batch, id)
				}
				return batch, nil
			})
			require.NoError(t, err)
			ids.Append(created.([]task.Id)...)
		}

		started := time.Now()
		failures, err := s.assignPending(t.Context(), ids)
		require.NoError(t, err)
		requireAssignmentFailures(t, failures, nil, nil)
		t.Logf("assigned %d tasks in %s", ids.Cardinality(), time.Since(started))
		pending, _, err := placer.PendingTasks(t.Context())
		require.NoError(t, err)
		require.Zero(t, pending.Cardinality())
		seen := mapset.NewSet[task.Id]()
		plan := make(map[string]mapset.Set[taskWithResourceRequest])
		for _, runnerID := range runnerIDs {
			set, err := servicestate.OpenTaskSetForRunner(db, db, runnerID)
			require.NoError(t, err)
			items, _, err := set.Items(t.Context(), db.Database)
			require.NoError(t, err)
			require.Equal(t, perRunner, items.Cardinality())
			require.Zero(t, items.Intersect(seen).Cardinality())
			seen = seen.Union(items)
			plan[runnerID] = mapset.NewSet[taskWithResourceRequest]()
			for id := range items.Iter() {
				status, owner := readTaskState(t, db, tasksDir, id)
				require.Equal(t, task.LifecycleStatusRunning, status)
				require.Equal(t, runnerID, *owner)
				plan[runnerID].Add(taskWithResourceRequest{taskId: id})
			}
		}
		require.True(t, seen.Equal(ids))

		failures, err = s.executeAssignmentPlan(t.Context(), plan)
		require.NoError(t, err)
		requireAssignmentFailures(t, failures, nil, nil)
		for _, runnerID := range runnerIDs {
			set, err := servicestate.OpenTaskSetForRunner(db, db, runnerID)
			require.NoError(t, err)
			_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				cpu, err := set.GetUtilization(tx, servicestate.UtilizationDimensionCPU)
				if err != nil {
					return nil, err
				}
				memory, err := set.GetUtilization(tx, servicestate.UtilizationDimensionMemory)
				require.EqualValues(t, perRunner, cpu)
				require.EqualValues(t, perRunner*1024*1024, memory)
				return nil, err
			})
			require.NoError(t, err)
		}
	})
}

func TestConcurrentAssignmentPlanSkipsSuspendedAndDeletedTasks(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-0"
		s, tasksDir, placer, runner := newSchedulerFixture(t, db, runnerID)
		tasks := mapset.NewSet[taskWithResourceRequest]()
		for i := range 64 {
			id := task.Id(fmt.Sprintf("stale-plan-%02d", i))
			seedPendingTaskWithResources(t, db, tasksDir, placer, id, 1, 1024*1024)
			tasks.Add(taskWithResourceRequest{taskId: id})
			if i%4 > 1 {
				continue
			}
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				key, err := tasksDir.Open(tx, id)
				if err != nil {
					return nil, err
				}
				if i%4 == 0 {
					if err := placer.PlaceTaskIn(tx, servicestate.PlacementLocationNowhere, key); err != nil {
						return nil, err
					}
					return nil, key.Clear(tx)
				}
				return nil, placer.PlaceTaskIn(tx, servicestate.PlacementLocationSuspended, key)
			})
			require.NoError(t, err)
		}
		failures, err := s.executeAssignmentPlan(t.Context(), map[string]mapset.Set[taskWithResourceRequest]{runnerID: tasks})
		require.NoError(t, err)
		requireAssignmentFailures(t, failures, nil, nil)
		for i := range 64 {
			id := task.Id(fmt.Sprintf("stale-plan-%02d", i))
			if i%4 == 0 {
				_, err := tasksDir.Open(db, id)
				require.ErrorIs(t, err, directory.ErrDirNotExists)
				continue
			}
			status, owner := readTaskState(t, db, tasksDir, id)
			if i%4 == 1 {
				require.Equal(t, task.LifecycleStatusSuspended, status)
				require.Nil(t, owner)
			} else {
				require.Equal(t, task.LifecycleStatusRunning, status)
				require.Equal(t, runnerID, *owner)
			}
		}
		items, _, err := runner.Items(t.Context(), db.Database)
		require.NoError(t, err)
		require.Equal(t, 32, items.Cardinality())
		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			cpu, err := runner.GetUtilization(tx, servicestate.UtilizationDimensionCPU)
			require.EqualValues(t, 32, cpu)
			return nil, err
		})
		require.NoError(t, err)
	})
}

func TestConcurrentAssignmentFailureRollsBackWholeBatch(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "worker-0"
		s, tasksDir, placer, runner := newSchedulerFixture(t, db, runnerID)
		tasks := mapset.NewSet[taskWithResourceRequest]()
		for i := range 64 {
			id := task.Id(fmt.Sprintf("rollback-%02d", i))
			seedPendingTaskWithResources(t, db, tasksDir, placer, id, 1, 1024*1024)
			tasks.Add(taskWithResourceRequest{taskId: id})
		}
		key, err := tasksDir.Open(db, "rollback-31")
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			key.RunnerId().Set(tx, new("invalid-owner"))
			return nil, nil
		})
		require.NoError(t, err)
		plan := map[string]mapset.Set[taskWithResourceRequest]{runnerID: tasks}
		_, err = s.executeAssignmentPlan(t.Context(), plan)
		require.ErrorIs(t, err, task.ErrNonRunningTaskHasRunnerID)
		pending, _, err := placer.PendingTasks(t.Context())
		require.NoError(t, err)
		require.Equal(t, 64, pending.Cardinality())
		items, _, err := runner.Items(t.Context(), db.Database)
		require.NoError(t, err)
		require.Zero(t, items.Cardinality())
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			key.RunnerId().Set(tx, nil)
			return nil, nil
		})
		require.NoError(t, err)
		failures, err := s.executeAssignmentPlan(t.Context(), plan)
		require.NoError(t, err)
		requireAssignmentFailures(t, failures, nil, nil)
		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			cpu, err := runner.GetUtilization(tx, servicestate.UtilizationDimensionCPU)
			require.EqualValues(t, 64, cpu)
			return nil, err
		})
		require.NoError(t, err)
	})
}
