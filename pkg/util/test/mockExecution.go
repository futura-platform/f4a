package testutil

import (
	"context"

	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
)

type MockExecutor struct {
	Execute func(
		inContainer executiontype.TransactionalContainer,
		ctx context.Context,
		marshalledInput []byte,
		opts ...ftype.FlowLoopOption,
	) ([]byte, error)
}

var _ execute.Executor = &MockExecutor{}

func (e *MockExecutor) ExecuteFrom(c executiontype.TransactionalContainer) execute.Executable {
	return &mockExecutable{container: c, execute: e.Execute}
}

type mockExecutable struct {
	container executiontype.TransactionalContainer
	execute   func(
		inContainer executiontype.TransactionalContainer,
		ctx context.Context,
		marshalledInput []byte,
		opts ...ftype.FlowLoopOption,
	) ([]byte, error)
}

func (m *mockExecutable) Execute(ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
	return m.execute(m.container, ctx, marshalledInput, opts...)
}
