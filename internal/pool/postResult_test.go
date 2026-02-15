package pool

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"schneider.vip/problem"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type capturedRequest struct {
	method      string
	path        string
	contentType string
	body        []byte
	readErr     error
}

func loadRunnableTask(t *testing.T, db dbutil.DbRoot, callbackURL string) run.RunnableTask {
	t.Helper()

	executorId := execute.ExecutorId("test-executor")
	tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	id := task.NewId()
	taskDirectory, err := tasksDirectory.Create(db, id)
	if err != nil {
		panic(err)
	}
	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		taskDirectory.ExecutorId().Set(tx, executorId)
		taskDirectory.CallbackUrl().Set(tx, callbackURL)
		taskDirectory.Input().Set(tx, []byte(id))
		return nil, nil
	})
	require.NoError(t, err)

	executor := &testutil.MockExecutor{
		Execute: func(_ executiontype.TransactionalContainer, _ context.Context, _ []byte, _ ...ftype.FlowLoopOption) ([]byte, error) {
			return nil, nil
		},
	}
	router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

	tasks, err := run.LoadTasks(t.Context(), db, router, []task.Id{id})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	return tasks[0]
}

func TestTaskManagerPostResult(t *testing.T) {
	t.Run("posts raw output when no error", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			output := []byte("expected output")
			capturedCh := make(chan capturedRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				capturedCh <- capturedRequest{
					method:      r.Method,
					path:        r.URL.Path,
					contentType: r.Header.Get("Content-Type"),
					body:        body,
					readErr:     err,
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()

			runnable := loadRunnableTask(t, db, server.URL+"/callback")
			manager := &taskManager{runMap: newRunMap(t.Name(), nil), c: server.Client()}

			err := manager.postResult(t.Context(), runnable, output, nil)
			assert.NoError(t, err)

			captured := <-capturedCh
			assert.NoError(t, captured.readErr)
			assert.Equal(t, "POST", captured.method)
			assert.Equal(t, "/callback", captured.path)
			assert.Equal(t, "application/octet-stream", captured.contentType)
			assert.Equal(t, output, captured.body)
		})
	})

	t.Run("posts problem JSON when error", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			taskErr := errors.New("boom")
			capturedCh := make(chan capturedRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				capturedCh <- capturedRequest{
					method:      r.Method,
					path:        r.URL.Path,
					contentType: r.Header.Get("Content-Type"),
					body:        body,
					readErr:     err,
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			runnable := loadRunnableTask(t, db, server.URL+"/callback")
			manager := &taskManager{runMap: newRunMap(t.Name(), nil), c: server.Client()}

			err := manager.postResult(t.Context(), runnable, []byte("ignored"), taskErr)
			assert.NoError(t, err)

			captured := <-capturedCh
			assert.NoError(t, captured.readErr)
			assert.Equal(t, "POST", captured.method)
			assert.Equal(t, "/callback", captured.path)
			assert.Equal(t, problem.ContentTypeJSON, captured.contentType)

			var payload map[string]any
			assert.NoError(t, json.Unmarshal(captured.body, &payload))
			assert.Equal(t, "Task failed", payload["title"])
			assert.Equal(t, taskErr.Error(), payload["detail"])
			assert.Equal(t, float64(http.StatusInternalServerError), payload["status"])
		})
	})

	t.Run("returns error on non-accepted status", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusTeapot)
			}))
			defer server.Close()

			runnable := loadRunnableTask(t, db, server.URL+"/callback")
			manager := &taskManager{runMap: newRunMap(t.Name(), nil), c: server.Client()}

			err := manager.postResult(t.Context(), runnable, []byte("output"), nil)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "bad status")
		})
	})

	t.Run("returns error when client fails", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			runnable := loadRunnableTask(t, db, "http://example.com/callback")
			manager := &taskManager{
				runMap: newRunMap(t.Name(), nil),
				c: &http.Client{
					Transport: roundTripperFunc(func(_ *http.Request) (*http.Response, error) {
						return nil, errors.New("transport failed")
					}),
				},
			}

			err := manager.postResult(t.Context(), runnable, []byte("output"), nil)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "failed to send result")
		})
	})
}
