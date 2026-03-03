package pool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/futura/flog"
	"schneider.vip/problem"
)

var deleteTaskAfterCallback = func(ctx context.Context, manager *taskManager, runnable run.RunnableTask) error {
	return manager.deleteTaskAfterCallback(ctx, runnable)
}

// run shadows the runMap.run method. This is to abstract away the callback function.
func (m *taskManager) run(ctx context.Context, r run.RunnableTask) error {
	callbackDelivered := false
	return m.runMap.run(ctx, r.Runnable, func(runCtx context.Context, output []byte, err error) error {
		if !callbackDelivered {
			if callbackErr := m.postResult(runCtx, r, output, err); callbackErr != nil {
				return callbackErr
			}
			callbackDelivered = true
		}
		return deleteTaskAfterCallback(runCtx, m, r)
	})
}

func (m *taskManager) postResult(ctx context.Context, runnable run.RunnableTask, output []byte, taskErr error) error {
	callbackUrl := runnable.CallbackUrl()
	l := flog.FromContext(ctx)
	if callbackUrl == nil {
		l.LogAttrs(ctx, slog.LevelDebug, "no callback url, skipping result delivery",
			slog.String("task_id", string(runnable.Id())),
			slog.String("task_error", fmt.Sprint(taskErr)),
		)
		return nil
	}
	l.LogAttrs(ctx, slog.LevelDebug, "sending result to callback",
		slog.String("task_id", string(runnable.Id())),
		slog.String("callback_url", callbackUrl.String()),
		slog.String("task_error", fmt.Sprint(taskErr)),
	)
	var body io.Reader
	var bodyCloser io.Closer
	var contentType string
	if taskErr != nil {
		p := problem.New(
			problem.Title("Task failed"),
			problem.Detail(taskErr.Error()),
			problem.Status(http.StatusInternalServerError),
		)
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			err := json.NewEncoder(pw).Encode(p)
			if err != nil {
				_ = pw.CloseWithError(err)
			}
		}()

		body = pr
		bodyCloser = pr
		contentType = problem.ContentTypeJSON
	} else {
		body = bytes.NewReader(output)
		contentType = "application/octet-stream"
	}

	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		callbackUrl.String(),
		body,
	)
	if err != nil {
		if bodyCloser != nil {
			_ = bodyCloser.Close()
		}
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", contentType)
	resp, err := m.c.Do(req)
	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return fmt.Errorf("failed to send result: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		return nil
	default:
		return fmt.Errorf("failed to send result: bad status: %s", resp.Status)
	}
}

func (m *taskManager) deleteTaskAfterCallback(ctx context.Context, runnable run.RunnableTask) error {
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "deleting task after callback",
		slog.String("task_id", string(runnable.Id())))
	_, err := m.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		taskKey, err := m.taskDirectory.Open(tx, runnable.Id())
		if err != nil {
			if errors.Is(err, directory.ErrDirNotExists) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to open task: %w", err)
		}
		assignmentState, err := task.ReadAssignmentState(tx, taskKey)
		if err != nil {
			return nil, err
		}
		if err := assignmentState.ValidateRunnerLifecycleInvariant(); err != nil {
			return nil, fmt.Errorf("task assignment invariant violation: %w", err)
		}
		isRunningOnThisRunner, err := assignmentState.IsRunningOn(m.runnerId)
		if err != nil {
			return nil, err
		} else if !isRunningOnThisRunner {
			return nil, nil
		}

		_, err = m.revisionStore.ApplyNext(tx, runnable.Id(), task.RevisionOperationDelete, func() error {
			if err := m.taskSet.Remove(tx, []byte(runnable.Id())); err != nil {
				return fmt.Errorf("failed to remove task from task queue: %w", err)
			}
			if err := taskKey.Clear(tx); err != nil {
				return fmt.Errorf("failed to clear task: %w", err)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to apply revisioned delete: %w", err)
		}
		return nil, nil
	})
	return err
}
