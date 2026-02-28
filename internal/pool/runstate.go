package pool

import (
	"context"
)

type runState struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	next  *runState
	runFn func(context.Context)
}

// newRunState creates a new run state.
// A run state represents a single-flight execution of a runnable,
// with the ability to chain another run state to run after the current one completes.
func newRunState(
	ctx context.Context,
	runFn func(context.Context),
) *runState {
	ctx, cancelFn := context.WithCancelCause(ctx)
	return &runState{
		ctx:    ctx,
		cancel: cancelFn,
		runFn:  runFn,
	}
}

func (s *runState) active() bool {
	return s.ctx.Err() == nil
}
