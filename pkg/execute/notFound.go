package execute

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/samber/mo"
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

func (r notFoundExecutable) Execute(ctx context.Context, marshalledInput []byte, callbackUrl *url.URL, opts ...ftype.FlowLoopOption) (mo.Option[string], error) {
	return mo.None[string](), fmt.Errorf("%w: %s", ErrExecutorNotFound, r.requestedExecutorId)
}
