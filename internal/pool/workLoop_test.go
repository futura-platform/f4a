package pool

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	waitTimeout = 2 * time.Second
	waitShort   = 200 * time.Millisecond
)

func testResourceRequest() *taskv1.TaskResourceRequest {
	return &taskv1.TaskResourceRequest{
		CpuMillis:   500,
		MemoryBytes: 1024,
	}
}

func seedTask(
	t *testing.T,
	db dbutil.DbRoot,
	id task.Id,
	executorId execute.ExecutorId,
	callbackUrl string,
	runnerId string,
) error {
	t.Helper()

	tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return err
	}

	taskDirectory, err := tasksDirectory.Create(db, id)
	if err != nil {
		return err
	}

	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		taskDirectory.ExecutorId().Set(tx, executorId)
		taskDirectory.CallbackUrl().Set(tx, &callbackUrl)
		taskDirectory.Input().Set(tx, []byte(id))
		taskDirectory.RunnerId().Set(tx, &runnerId)
		taskDirectory.LifecycleStatus().Set(tx, task.LifecycleStatusRunning)
		taskDirectory.ResourceRequest().Set(tx, testResourceRequest())
		return nil, nil
	})
	return err
}

func openTaskSet(t testing.TB, db dbutil.DbRoot, runnerId string) *servicestate.RunnerSet {
	t.Helper()

	set, err := servicestate.CreateOrOpenTaskSetForRunner(db, db, runnerId)
	require.NoError(t, err)
	return set
}

func addTasks(t testing.TB, db dbutil.DbRoot, set *servicestate.RunnerSet, ids []task.Id) {
	t.Helper()

	tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, id := range ids {
			taskKey, err := tasksDirectory.Open(tx, id)
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					taskKey, err = tasksDirectory.Create(tx, id)
					if err != nil {
						return nil, err
					}
					taskKey.ResourceRequest().Set(tx, testResourceRequest())
					if err := set.Add(tx, taskKey); err != nil {
						return nil, err
					}
					if err := taskKey.Clear(tx); err != nil {
						return nil, err
					}
					continue
				}
				return nil, err
			}
			if err := set.Add(tx, taskKey); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func removeTasks(t testing.TB, db dbutil.DbRoot, set *servicestate.RunnerSet, ids []task.Id) {
	t.Helper()

	tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, id := range ids {
			taskKey, err := tasksDirectory.Open(tx, id)
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					continue
				}
				return nil, err
			}
			if err := set.Remove(tx, taskKey); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
}

// waitForTaskDeletion polls until the task directory for id no longer exists.
// It is currently unused: post-settlement task deletion is not wired into the
// work loop yet. The pending subtests below will need it once deletion lands.
func waitForTaskDeletion(t testing.TB, db dbutil.DbRoot, id task.Id) {
	t.Helper()

	tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		exists := false
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			_, err := tasksDirectory.Open(tx, id)
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					return nil, nil
				}
				return nil, err
			}
			exists = true
			return nil, nil
		})
		require.NoError(t, err)
		if !exists {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for task deletion: %s", id)
}

func waitForTaskEvents(t *testing.T, ch <-chan task.Id, runErr <-chan error, ids []task.Id) {
	t.Helper()

	remaining := make(map[task.Id]struct{}, len(ids))
	for _, id := range ids {
		remaining[id] = struct{}{}
	}

	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	for len(remaining) > 0 {
		select {
		case id := <-ch:
			if _, ok := remaining[id]; !ok {
				t.Fatalf("unexpected task event for %s", id)
			}
			delete(remaining, id)
		case err := <-runErr:
			t.Fatalf("work loop exited early: %v", err)
		case <-timer.C:
			t.Fatalf("timeout waiting for task events: %v", remaining)
		}
	}
}

func assertNoTaskEvents(t *testing.T, ch <-chan task.Id, runErr <-chan error, disallowed map[task.Id]struct{}, wait time.Duration) {
	t.Helper()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	for {
		select {
		case id := <-ch:
			if _, ok := disallowed[id]; ok {
				t.Fatalf("unexpected task event for %s", id)
			}
		case err := <-runErr:
			t.Fatalf("work loop exited early: %v", err)
		case <-timer.C:
			return
		}
	}
}

func TestWorkLoop(t *testing.T) {
	t.Run("when a task is assigned, it is executed", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			assert.NoError(t, db.Options().SetTransactionRetryLimit(3)) // since we are accessing concurrently we can get conflicts

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			runnerId := "test-runner"
			executorId := execute.ExecutorId("test-executor")
			taskCount := 4
			taskIds := make([]task.Id, 0, taskCount)
			for range taskCount {
				id := task.NewId()
				taskIds = append(taskIds, id)
				require.NoError(t, seedTask(t, db, id, executorId, "http://example.com/callback", runnerId))
			}

			startedCh := make(chan task.Id, taskCount*2)
			canceledCh := make(chan task.Id, taskCount*2)

			executor := &testutil.MockExecutor{
				Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
					id := task.Id(marshalledInput)
					startedCh <- id

					<-ctx.Done()
					canceledCh <- id
					return context.Cause(ctx)
				},
			}
			router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

			runErrCh := make(chan error, 1)
			taskSet := openTaskSet(t, db, runnerId)
			go func() {
				err := RunWorkLoop(ctx, runnerId, db, taskSet, router)
				assert.ErrorIs(t, err, context.Canceled)
				runErrCh <- err
			}()

			// start 4 or more tasks, then check that they are running
			addTasks(t, db, taskSet, taskIds)
			waitForTaskEvents(t, startedCh, runErrCh, taskIds)

			removeCount := 2
			removedTaskIds := taskIds[:removeCount]
			remainingTaskIds := taskIds[removeCount:]

			t.Run("when a task is removed, it is stopped", func(t *testing.T) {
				// remove 2 or more tasks, then check that they are stopped
				removeTasks(t, db, taskSet, removedTaskIds)
				waitForTaskEvents(t, canceledCh, runErrCh, removedTaskIds)
			})

			t.Run("when a task is unchanged, it continues execution uninterrupted", func(t *testing.T) {
				// continuing from the removal test, this should assert that the unaffected tasks continue execution uninterrupted
				remaining := make(map[task.Id]struct{}, len(remainingTaskIds))
				for _, id := range remainingTaskIds {
					remaining[id] = struct{}{}
				}
				assertNoTaskEvents(t, canceledCh, runErrCh, remaining, waitShort)
			})

			cancel()
			select {
			case err := <-runErrCh:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(waitTimeout):
				t.Fatal("timeout waiting for RunWorkLoop to stop")
			}
		})
	})
	t.Run("if a non-existent task is present in the initial assignment snapshot, it is ignored", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			assert.NoError(t, db.Options().SetTransactionRetryLimit(3))

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			runWorkLoopErr := make(chan error, 1)
			runnerId := "test-runner"
			taskSet := openTaskSet(t, db, runnerId)
			startedCh := make(chan task.Id, 1)
			executorId := execute.ExecutorId("test-executor")
			executor := &testutil.MockExecutor{
				Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
					startedCh <- task.Id(marshalledInput)
					<-ctx.Done()
					return context.Cause(ctx)
				},
			}
			router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

			validID := task.NewId()
			require.NoError(t, seedTask(t, db, validID, executorId, "http://example.com/callback", runnerId))
			addTasks(t, db, taskSet, []task.Id{task.NewId(), validID})

			go func() {
				runWorkLoopErr <- RunWorkLoop(ctx, runnerId, db, taskSet, router)
			}()

			select {
			case id := <-startedCh:
				assert.Equal(t, validID, id)
			case err := <-runWorkLoopErr:
				t.Fatalf("work loop exited early: %v", err)
			case <-time.After(waitTimeout):
				t.Fatal("timeout waiting for valid task to start")
			}

			cancel()
			select {
			case err := <-runWorkLoopErr:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(waitTimeout):
				t.Fatal("timeout waiting for RunWorkLoop to stop")
			}
		})
	})
	t.Run("does not return non-run cancel cause", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			assert.NoError(t, db.Options().SetTransactionRetryLimit(3)) // since we are accessing concurrently we can get conflicts

			ctx, cancel := context.WithCancelCause(context.Background())
			cancelCause := errors.New("run loop canceled")
			runWorkLoopErr := make(chan error, 1)
			runnerId := "test-runner"
			taskSet := openTaskSet(t, db, runnerId)
			go func() {
				runWorkLoopErr <- RunWorkLoop(ctx, runnerId, db, taskSet, nil)
			}()
			cancel(cancelCause)
			select {
			case err := <-runWorkLoopErr:
				assert.ErrorIs(t, err, context.Canceled)
				assert.NotErrorIs(t, err, cancelCause)
				assert.NotErrorIs(t, err, ErrRunFailed)
			case <-time.After(waitTimeout):
				t.Fatal("timeout waiting for RunWorkLoop to return an error")
			}
		})
	})
	t.Run("returns run failed error when a run fails", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			assert.NoError(t, db.Options().SetTransactionRetryLimit(10))

			expectedErr := fmt.Errorf("%w: expected error", run.ErrRunFatal)
			runWorkLoopErr := make(chan error, 1)
			runnerId := "test-runner"
			taskSet := openTaskSet(t, db, runnerId)

			executor := &testutil.MockExecutor{
				Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
					return expectedErr
				},
			}
			executorId := execute.ExecutorId("test-executor")
			router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

			go func() {
				runWorkLoopErr <- RunWorkLoop(t.Context(), runnerId, db, taskSet, router)
			}()

			id := task.NewId()
			require.NoError(t, seedTask(t, db, id, executorId, "http://example.com/callback", runnerId))
			addTasks(t, db, taskSet, []task.Id{id})

			select {
			case err := <-runWorkLoopErr:
				assert.ErrorIs(t, err, ErrRunFailed)
				assert.ErrorIs(t, err, expectedErr)
			case <-time.After(waitTimeout):
				t.Fatal("timeout waiting for RunWorkLoop to return an error")
			}
		})
	})
	t.Run("the result is reliably delivered at least once to the callback url", func(t *testing.T) {
		t.Run("when there are no errors", func(t *testing.T) {
			// Result delivery now lives inside Executable.Settle (pkg/execute);
			// at this layer we assert the work loop drives an assigned task's
			// settlement to completion without surfacing an error.
			testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
				assert.NoError(t, db.Options().SetTransactionRetryLimit(10))

				settledCh := make(chan task.Id, 1)
				executor := &testutil.MockExecutor{
					Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, callbackUrl *url.URL, _ ...ftype.FlowLoopOption) error {
						assert.NotNil(t, callbackUrl)
						settledCh <- task.Id(marshalledInput)
						return nil
					},
				}
				executorId := execute.ExecutorId("test-executor")
				router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				runWorkLoopErr := make(chan error, 1)
				runnerId := "test-runner"
				taskSet := openTaskSet(t, db, runnerId)
				go func() {
					runWorkLoopErr <- RunWorkLoop(ctx, runnerId, db, taskSet, router)
				}()

				id := task.NewId()
				require.NoError(t, seedTask(t, db, id, executorId, "http://example.com/callback", runnerId))
				addTasks(t, db, taskSet, []task.Id{id})

				select {
				case err := <-runWorkLoopErr:
					t.Fatalf("RunWorkLoop returned an error: %v", err)
				case <-time.After(waitTimeout):
					t.Fatal("timeout waiting for the task to settle")
				case settledId := <-settledCh:
					assert.Equal(t, id, settledId)
				}

				// a run that settled successfully must not surface an error to the loop
				select {
				case err := <-runWorkLoopErr:
					t.Fatalf("RunWorkLoop returned an error after settlement: %v", err)
				case <-time.After(waitShort):
				}

				cancel()
				select {
				case err := <-runWorkLoopErr:
					assert.ErrorIs(t, err, context.Canceled)
				case <-time.After(waitTimeout):
					t.Fatal("timeout waiting for worker shutdown")
				}
			})
		})
		t.Run("when delete fails transiently, callback is not reposted and delete is retried", func(t *testing.T) {
			// Pending: once post-settlement deletion is wired into the work
			// loop, assert that a transient deleteTask failure is retried until
			// the task record is gone (waitForTaskDeletion) while the callback
			// is delivered exactly once (no repost by the retried delete).
			t.Skip("terminal task deletion / dead letter queue not wired yet — pending next draft")
		})
		t.Run("external delete race does not cause callback retry loop", func(t *testing.T) {
			// Pending: once post-settlement deletion is wired into the work
			// loop, assert that a concurrent external delete of the task record
			// (removing it from the task set and clearing the directory) does
			// not make the loop re-run settlement or re-deliver the callback.
			t.Skip("terminal task deletion / dead letter queue not wired yet — pending next draft")
		})
		t.Run("when the loop fails during the callback, the next worker loop will retry the callback", func(t *testing.T) {
			// Pending: once post-settlement deletion is wired into the work
			// loop, assert that a worker crash mid-delivery leaves the task
			// owed (not deleted), and a fresh RunWorkLoop picks it up and
			// drives settlement (callback delivery) to completion.
			t.Skip("terminal task deletion / dead letter queue not wired yet — pending next draft")
		})
	})
}
