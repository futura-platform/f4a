package execute

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/futura-platform/futura/ftype"
)

var ErrExecutorNotFound = errors.New("executor not found")

type notFoundExecutor struct {
	requestedExecutorId ExecutorId
}

func (r notFoundExecutor) ExecuteFrom(SettlementContainers) Executable {
	return notFoundExecutable{requestedExecutorId: r.requestedExecutorId}
}

type notFoundExecutable struct {
	requestedExecutorId ExecutorId
}

// Settle always fails without settling: an unknown executor is a run error
// (this worker may simply lack the executor), never a terminal task result.
func (r notFoundExecutable) Settle(ctx context.Context, marshalledInput []byte, callbackUrl *url.URL, opts ...ftype.FlowLoopOption) error {
	return fmt.Errorf("%w: %s", ErrExecutorNotFound, r.requestedExecutorId)
}
