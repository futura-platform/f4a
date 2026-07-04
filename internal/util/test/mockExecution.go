package testutil

import (
	"context"
	"net/url"

	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
)

type MockExecutor struct {
	Settle func(
		containers execute.SettlementContainers,
		ctx context.Context,
		marshalledInput []byte,
		callbackUrl *url.URL,
		opts ...ftype.FlowLoopOption,
	) error
}

var _ execute.Executor = &MockExecutor{}

type mockExecutable struct {
	containers execute.SettlementContainers
	settle     func(
		containers execute.SettlementContainers,
		ctx context.Context,
		marshalledInput []byte,
		callbackUrl *url.URL,
		opts ...ftype.FlowLoopOption,
	) error
}

func (e *MockExecutor) ExecuteFrom(c execute.SettlementContainers) execute.Executable {
	return &mockExecutable{containers: c, settle: e.Settle}
}

func (m *mockExecutable) Settle(ctx context.Context, marshalledInput []byte, callbackUrl *url.URL, opts ...ftype.FlowLoopOption) error {
	return m.settle(m.containers, ctx, marshalledInput, callbackUrl, opts...)
}
