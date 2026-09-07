package pool

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/futura/flog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// runMap is a type to keep track of all the running tasks in a pool.
type runMap struct {
	mu sync.Mutex
	wg sync.WaitGroup

	runStates map[task.Id]*runState
	runnerId  string
	// onRunError receives the outcome of a run that failed without being
	// cancelled; onRunSettled receives every run that settled (returned nil)
	// so the owner can retire the task.
	onRunError   func(task.Id, error)
	onRunSettled func(context.Context, task.Id)
}

func newRunMap(runnerId string, onRunError func(task.Id, error), onRunSettled func(context.Context, task.Id)) *runMap {
	if onRunError == nil {
		onRunError = func(task.Id, error) {}
	}
	if onRunSettled == nil {
		onRunSettled = func(context.Context, task.Id) {}
	}
	return &runMap{
		runStates:    make(map[task.Id]*runState),
		runnerId:     runnerId,
		onRunError:   onRunError,
		onRunSettled: onRunSettled,
	}
}

var (
	ErrRunNotFound  = errors.New("run not found")
	ErrDuplicateRun = errors.New("run already exists for task")
)

func (m *runMap) run(ctx context.Context, r run.Runnable, callbackUrl *url.URL) error {
	ctx = task.WithTaskKey(ctx, r.TaskKey())
	m.mu.Lock()
	defer m.mu.Unlock()

	newState := newRunState(ctx, func(runCtx context.Context) {
		// Start each run as its own root trace so individual task runs are
		// viewable in isolation, instead of accumulating under the scheduler's
		// long-lived work-loop trace. A span link preserves the connection back
		// to the span that launched this run.
		runCtx, span := tracer.Start(runCtx, "run",
			trace.WithNewRoot(),
			trace.WithLinks(trace.LinkFromContext(runCtx,
				attribute.String("f4a.link", "launcher"),
			)),
		)
		defer span.End()
		span.SetAttributes(attribute.String("task_id", string(r.Id())))
		span.SetAttributes(attribute.String("executor_id", string(r.ExecutorId())))

		// Swap this run's span context into the task record and link back to
		// the previous attempt, chaining the task's runs across machines.
		// Best-effort: telemetry must not fail the run.
		if prev, err := swapLastRunSpan(runCtx, r.Db(), r.TaskKey(), span.SpanContext()); err != nil {
			flog.FromContext(runCtx).LogAttrs(runCtx, slog.LevelWarn,
				"failed to chain run span to previous attempt",
				slog.String("task_id", string(r.Id())),
				slog.String("error", err.Error()),
			)
		} else if prev.IsValid() {
			span.AddLink(trace.Link{
				SpanContext: prev,
				Attributes: []attribute.KeyValue{
					attribute.String("f4a.link", "previous_run"),
				},
			})
		}

		err := r.Run(runCtx, m.runnerId, callbackUrl)
		if err != nil && runCtx.Err() == nil {
			span.RecordError(err)
			if errors.Is(err, run.ErrLeaseLost) {
				flog.FromContext(runCtx).LogAttrs(runCtx, slog.LevelWarn, "run ended with its lease",
					slog.String("task_id", string(r.Id())), slog.String("error", err.Error()))
				span.SetAttributes(attribute.Bool("canceled", true))
				return
			}
			m.onRunError(r.Id(), err)
		}
		if runCtx.Err() != nil {
			// Distinguish runs that were cancelled (suspend/reschedule/delete)
			// from runs that completed on their own.
			span.SetAttributes(attribute.Bool("canceled", true))
			if cause := context.Cause(runCtx); cause != nil {
				span.SetAttributes(attribute.String("cancel_cause", cause.Error()))
			}
		} else if err == nil {
			// settled: the task owes nothing — hand it back to the owner to
			// be retired
			m.onRunSettled(runCtx, r.Id())
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

// swapLastRunSpan atomically replaces the task's stored last-run span context
// with next, returning the previous value. It returns a zero SpanContext when
// no prior run was recorded.
func swapLastRunSpan(ctx context.Context, db fdb.Database, taskKey task.TaskKey, next trace.SpanContext) (trace.SpanContext, error) {
	prev, err := dbutil.TransactContext(ctx, db.Transact, func(tx fdb.Transaction) (any, error) {
		key := taskKey.LastRunSpan()
		prev, err := key.Get(tx).Get()
		if err != nil {
			return nil, err
		}
		key.Set(tx, next)
		return prev, nil
	})
	if err != nil {
		return trace.SpanContext{}, err
	}
	return prev.(trace.SpanContext), nil
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
