package pool

import (
	"context"
	"errors"
	"sync"

	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
)

// runMap is a type to keep track of all the running tasks in a pool.
type runMap struct {
	mu sync.Mutex
	wg sync.WaitGroup

	runCancels map[task.Id]context.CancelCauseFunc
	runnerId   string
	onRunError func(task.Id, error)
}

func newRunMap(runnerId string, onRunError func(task.Id, error)) *runMap {
	if onRunError == nil {
		onRunError = func(task.Id, error) {}
	}
	return &runMap{
		runCancels: make(map[task.Id]context.CancelCauseFunc),
		runnerId:   runnerId,
		onRunError: onRunError,
	}
}

var (
	ErrRunNotFound  = errors.New("run not found")
	ErrDuplicateRun = errors.New("run already exists for task")
)

func (m *runMap) run(ctx context.Context, r run.Runnable, callback func(context.Context, []byte, error) error) error {
	ctx = task.WithTaskKey(ctx, r.TaskKey())

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.runCancels[r.Id()]; ok {
		return ErrDuplicateRun
	}

	ctx, cancel := context.WithCancelCause(ctx)
	m.wg.Go(func() {
		cleanup := func() {
			m.mu.Lock()
			defer m.mu.Unlock()

			cancel(nil)
			delete(m.runCancels, r.Id())
		}
		err := r.Run(ctx, m.runnerId, callback)
		if err != nil && ctx.Err() == nil {
			m.onRunError(r.Id(), err)
		}
		cleanup()
	})
	m.runCancels[r.Id()] = cancel
	return nil
}

func (m *runMap) cancel(id task.Id) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cancel, ok := m.runCancels[id]
	if !ok {
		return ErrRunNotFound
	}
	cancel(nil)
	delete(m.runCancels, id)
	return nil
}

// wait waits for all runs to exit.
func (m *runMap) wait() {
	m.wg.Wait()
}
