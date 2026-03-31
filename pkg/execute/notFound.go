package execute

import (
	"context"
	"errors"
	"fmt"
	"time"

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

func (r notFoundExecutable) Execute(ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, time.Time, error) {
	return nil, time.Now(), fmt.Errorf("%w: %s", ErrExecutorNotFound, r.requestedExecutorId)
}
