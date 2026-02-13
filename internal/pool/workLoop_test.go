package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	waitTimeout = 2 * time.Second
	waitShort   = 200 * time.Millisecond
)

func seedTask(
	t *testing.T,
	db dbutil.DbRoot,
	id task.Id,
	executorId execute.ExecutorId,
	callbackUrl string,
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
		taskDirectory.CallbackUrl().Set(tx, callbackUrl)
		taskDirectory.Input().Set(tx, []byte(id.String()))
		return nil, nil
	})
	return err
}

func openTaskSet(t testing.TB, db dbutil.DbRoot, runnerId string) *reliableset.Set {
	t.Helper()

	path := append([]string{}, db.Root.GetPath()...)
	path = append(path, "task_queue", runnerId)
	set, err := reliableset.CreateOrOpen(db, path)
	require.NoError(t, err)
	return set
}

func addTasks(t testing.TB, db dbutil.DbRoot, set *reliableset.Set, ids []task.Id) {
	t.Helper()

	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, id := range ids {
			if err := set.Add(tx, id.Bytes()); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func removeTasks(t testing.TB, db dbutil.DbRoot, set *reliableset.Set, ids []task.Id) {
	t.Helper()

	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, id := range ids {
			if err := set.Remove(tx, id.Bytes()); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
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
			idByInput := make(map[string]task.Id, taskCount)
			for range taskCount {
				id := task.NewId()
				taskIds = append(taskIds, id)
				idByInput[id.String()] = id
				require.NoError(t, seedTask(t, db, id, executorId, "http://example.com/callback"))
			}

			startedCh := make(chan task.Id, taskCount*2)
			canceledCh := make(chan task.Id, taskCount*2)

			executor := &testutil.MockExecutor{
				Execute: func(_ executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, _ ...ftype.FlowLoopOption) ([]byte, error) {
					id, ok := idByInput[string(marshalledInput)]
					if !ok {
						return nil, fmt.Errorf("unexpected input: %q", marshalledInput)
					}
					startedCh <- id

					<-ctx.Done()
					canceledCh <- id
					return nil, nil
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
	t.Run("if a non-existent task is assigned, it causes RunWorkLoop to return an error", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			assert.NoError(t, db.Options().SetTransactionRetryLimit(3)) // since we are accessing concurrently we can get conflicts

			runWorkLoopErr := make(chan error)
			runnerId := "test-runner"
			taskSet := openTaskSet(t, db, runnerId)
			go func() {
				runWorkLoopErr <- RunWorkLoop(t.Context(), runnerId, db, taskSet, nil)
			}()
			addTasks(t, db, taskSet, []task.Id{task.NewId()})
			select {
			case err := <-runWorkLoopErr:
				assert.ErrorContains(t, err, "failed to load runnables")
			case <-time.After(waitTimeout):
				t.Fatal("timeout waiting for RunWorkLoop to return an error")
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
			expectedErr := fmt.Errorf("%w: expected error", run.ErrRunFatal)
			runWorkLoopErr := make(chan error, 1)
			runnerId := "test-runner"
			taskSet := openTaskSet(t, db, runnerId)

			executor := &testutil.MockExecutor{
				Execute: func(_ executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, _ ...ftype.FlowLoopOption) ([]byte, error) {
					return nil, expectedErr
				},
			}
			executorId := execute.ExecutorId("test-executor")
			router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

			go func() {
				runWorkLoopErr <- RunWorkLoop(t.Context(), runnerId, db, taskSet, router)
			}()

			id := task.NewId()
			require.NoError(t, seedTask(t, db, id, executorId, "http://example.com/callback"))
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
			testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
				expectedOutput := fmt.Appendf([]byte{}, "expected output: %f", rand.Float64())
				gotCallbackCh := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, "POST", r.Method)
					assert.Equal(t, "/callback", r.URL.Path)
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					assert.Equal(t, expectedOutput, body)
					w.WriteHeader(http.StatusAccepted)
					gotCallbackCh <- struct{}{}
				}))
				defer server.Close()

				runWorkLoopErr := make(chan error, 1)
				runnerId := "test-runner"
				taskSet := openTaskSet(t, db, runnerId)

				executor := &testutil.MockExecutor{
					Execute: func(_ executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, _ ...ftype.FlowLoopOption) ([]byte, error) {
						return expectedOutput, nil
					},
				}
				executorId := execute.ExecutorId("test-executor")
				router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

				go func() {
					runWorkLoopErr <- RunWorkLoop(t.Context(), runnerId, db, taskSet, router)
				}()

				id := task.NewId()
				require.NoError(t, seedTask(t, db, id, executorId, fmt.Sprintf("%s/callback", server.URL)))
				addTasks(t, db, taskSet, []task.Id{id})
				select {
				case err := <-runWorkLoopErr:
					t.Fatalf("RunWorkLoop returned an error: %v", err)
				case <-time.After(waitTimeout):
					t.Fatal("timeout waiting for callback")
				case <-gotCallbackCh:
				}
			})
		})
		t.Run("when the loop fails during the callback, the next worker loop will retry the callback", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
				expectedOutput := fmt.Appendf([]byte{}, "expected output: %f", rand.Float64())
				callbackSuccessful := make(chan struct{})
				var callCount atomic.Int32
				firstWorkerLoopCtx, firstWorkerLoopCancel := context.WithCancel(t.Context())
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, "POST", r.Method)
					assert.Equal(t, "/callback", r.URL.Path)
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					assert.Equal(t, expectedOutput, body)
					defer w.WriteHeader(http.StatusAccepted)

					switch callCount.Add(1) {
					case 1:
						firstWorkerLoopCancel()
						time.Sleep(waitShort)
						return
					case 2:
						callbackSuccessful <- struct{}{}
					default:
						t.Fatalf("unexpected callback call: %d", callCount.Load())
					}
				}))
				defer server.Close()

				runWorkLoopErr := make(chan error, 1)
				runnerId := "test-runner"
				taskSet := openTaskSet(t, db, runnerId)

				executor := &testutil.MockExecutor{
					Execute: func(_ executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, _ ...ftype.FlowLoopOption) ([]byte, error) {
						return expectedOutput, nil
					},
				}
				executorId := execute.ExecutorId("test-executor")
				router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

				// first we spawn a worker that will fail when the callback is called
				go func() {
					runWorkLoopErr <- RunWorkLoop(firstWorkerLoopCtx, runnerId, db, taskSet, router)
				}()

				id := task.NewId()
				require.NoError(t, seedTask(t, db, id, executorId, fmt.Sprintf("%s/callback", server.URL)))

				addTasks(t, db, taskSet, []task.Id{id})
				select {
				case err := <-runWorkLoopErr:
					assert.ErrorIs(t, err, context.Canceled)
				case <-time.After(waitTimeout):
					t.Fatal("timeout waiting for first callback")
				case <-callbackSuccessful:
					t.Fatal("callback successful before first worker loop context was canceled")
				}

				// now simulate the worker coming back online
				go func() {
					runWorkLoopErr <- RunWorkLoop(t.Context(), runnerId, db, taskSet, router)
				}()
				select {
				case err := <-runWorkLoopErr:
					t.Fatalf("RunWorkLoop returned an error: %v", err)
				case <-time.After(waitTimeout):
					t.Fatal("timeout waiting for second worker loop")
				case <-callbackSuccessful:
				}
			})
		})
	})
}
