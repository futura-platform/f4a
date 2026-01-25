package pool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/futura/flog"
	"schneider.vip/problem"
)

// run shadows the runMap.run method. This is to abstract away the callback function.
func (m *taskManager) run(r run.RunnableTask) error {
	return m.runMap.run(r.Runnable, func(ctx context.Context, output []byte, err error) error {
		return m.postResult(ctx, r, output, err)
	})
}

func (m *taskManager) postResult(ctx context.Context, runnable run.RunnableTask, output []byte, taskErr error) error {
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "sending result to callback",
		slog.String("task_id", runnable.Id().String()),
		slog.Bool("error", taskErr != nil))
	var body io.Reader
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
		contentType = problem.ContentTypeJSON
	} else {
		body = bytes.NewReader(output)
		contentType = "application/octet-stream"
	}

	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		runnable.CallbackUrl().String(),
		body,
	)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", contentType)
	resp, err := m.c.Do(req)
	if err != nil {
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
