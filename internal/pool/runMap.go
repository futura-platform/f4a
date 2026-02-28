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

	runStates  map[task.Id]*runState
	runnerId   string
	onRunError func(task.Id, error)
}

func newRunMap(runnerId string, onRunError func(task.Id, error)) *runMap {
	if onRunError == nil {
		onRunError = func(task.Id, error) {}
	}
	return &runMap{
		runStates:  make(map[task.Id]*runState),
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

	newState := newRunState(ctx, func(runCtx context.Context) {
		err := r.Run(runCtx, m.runnerId, callback)
		if err != nil && runCtx.Err() == nil {
			m.onRunError(r.Id(), err)
		}
	})

	s, ok := m.runStates[r.Id()]
	if !ok {
		m.runStates[r.Id()] = newState
		m.wg.Go(func() {
			m.runChain(r.Id(), newState)
		})
		return nil
	}
	if runStateChainActive(s) {
		newState.cancel(ErrDuplicateRun)
		return ErrDuplicateRun
	}
	lastRunState(s).next = newState
	return nil
}

func (m *runMap) cancel(id task.Id) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.runStates[id]
	if !ok {
		return ErrRunNotFound
	}
	for current := s; current != nil; current = current.next {
		current.cancel(nil)
	}
	return nil
}

// wait waits for all runs to exit.
func (m *runMap) wait() {
	m.wg.Wait()
}

// runChain runs the chain of run states serially.
// It maintains the runStates map to only contain the relevant portion of the chain,
// meaning the current run state, or pending future run states that have not started yet.
// Pending states that were canceled before their turn are skipped.
func (m *runMap) runChain(id task.Id, current *runState) {
	for current != nil {
		// Pending states canceled before they start are skipped entirely.
		if current.active() {
			current.runFn(current.ctx)
		}

		m.mu.Lock()
		next := current.next
		if next != nil {
			m.runStates[id] = next
		} else {
			delete(m.runStates, id)
		}
		m.mu.Unlock()

		current = next
	}
}

func runStateChainActive(head *runState) bool {
	for current := head; current != nil; current = current.next {
		if current.active() {
			return true
		}
	}
	return false
}

func lastRunState(head *runState) *runState {
	current := head
	for current.next != nil {
		current = current.next
	}
	return current
}
