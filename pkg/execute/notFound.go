package execute

import (
	"context"
	"errors"
	"fmt"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
)

var ErrExecutorNotFound = errors.New("executor not found")

type notFoundExecutor struct {
	requestedExecutorId ExecutorId
}

func (r notFoundExecutor) ExecuteFrom(executiontype.TransactionalContainer) Executable {
	return notFoundExecutable{requestedExecutorId: r.requestedExecutorId}
}

type notFoundExecutable struct {
	requestedExecutorId ExecutorId
}

func (r notFoundExecutable) Execute(ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
	return nil, fmt.Errorf("%w: %s", ErrExecutorNotFound, r.requestedExecutorId)
}
