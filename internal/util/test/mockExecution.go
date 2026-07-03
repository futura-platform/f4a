package testutil

import (
	"context"
	"net/url"

	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/samber/mo"
)

type MockExecutor struct {
	Settle func(
		inContainer executiontype.TransactionalContainer,
		ctx context.Context,
		marshalledInput []byte,
		callbackUrl *url.URL,
		opts ...ftype.FlowLoopOption,
	) (mo.Option[string], error)
}

var _ execute.Executor = &MockExecutor{}

type mockExecutable struct {
	container executiontype.TransactionalContainer
	settle    func(
		inContainer executiontype.TransactionalContainer,
		ctx context.Context,
		marshalledInput []byte,
		callbackUrl *url.URL,
		opts ...ftype.FlowLoopOption,
	) (mo.Option[string], error)
}

func (e *MockExecutor) ExecuteFrom(c executiontype.TransactionalContainer) execute.Executable {
	return &mockExecutable{container: c, settle: e.Settle}
}

func (m *mockExecutable) Settle(ctx context.Context, marshalledInput []byte, callbackUrl *url.URL, opts ...ftype.FlowLoopOption) (mo.Option[string], error) {
	return m.settle(m.container, ctx, marshalledInput, callbackUrl, opts...)
}
