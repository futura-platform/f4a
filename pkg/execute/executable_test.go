package execute_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
)

type trackingMarshaller[A, R any] struct {
	unmarshal      func([]byte) (A, error)
	marshal        func(R) ([]byte, error)
	unmarshalCalls int
	marshalCalls   int
}

func (m *trackingMarshaller[A, R]) UnmarshalInput(data []byte) (A, error) {
	m.unmarshalCalls++
	return m.unmarshal(data)
}

func (m *trackingMarshaller[A, R]) MarshalOutput(data R) ([]byte, error) {
	m.marshalCalls++
	return m.marshal(data)
}

func TestExecutableExecuteSuccess(t *testing.T) {
	container := executiontype.NewInMemoryContainer()
	var received string

	marshaller := &trackingMarshaller[string, string]{
		unmarshal: func(data []byte) (string, error) {
			return string(data), nil
		},
		marshal: func(data string) ([]byte, error) {
			return []byte(data), nil
		},
	}

	executor := execute.NewExecutor(func(b futura.FlowBuilder, input string) (string, error) {
		received = input
		return input + "-out", nil
	}, marshaller)

	executable := executor.ExecuteFrom(container)
	output, err := executable.Execute(context.Background(), []byte("input"))

	assert.NoError(t, err)
	assert.Equal(t, "input-out", string(output))
	assert.Equal(t, "input", received)
	assert.Equal(t, 1, marshaller.unmarshalCalls)
	assert.Equal(t, 1, marshaller.marshalCalls)
}

func TestExecutableExecuteUnmarshalError(t *testing.T) {
	container := executiontype.NewInMemoryContainer()
	sentinel := errors.New("unmarshal failure")
	called := false

	marshaller := &trackingMarshaller[string, string]{
		unmarshal: func(data []byte) (string, error) {
			return "", sentinel
		},
		marshal: func(data string) ([]byte, error) {
			return []byte("should-not-be-called"), nil
		},
	}

	executor := execute.NewExecutor(func(b futura.FlowBuilder, input string) (string, error) {
		called = true
		return "", nil
	}, marshaller)

	executable := executor.ExecuteFrom(container)
	_, err := executable.Execute(context.Background(), []byte("input"))

	assert.ErrorIs(t, err, sentinel)
	assert.False(t, called)
	assert.Equal(t, 1, marshaller.unmarshalCalls)
	assert.Equal(t, 0, marshaller.marshalCalls)
}

func TestExecutableExecuteFlowError(t *testing.T) {
	container := executiontype.NewInMemoryContainer()
	sentinel := errors.New("flow failure")

	marshaller := &trackingMarshaller[string, string]{
		unmarshal: func(data []byte) (string, error) {
			return string(data), nil
		},
		marshal: func(data string) ([]byte, error) {
			return []byte(data), nil
		},
	}

	executor := execute.NewExecutor(func(b futura.FlowBuilder, input string) (string, error) {
		return "", fmt.Errorf("%w: %w", ftype.ErrCancelFlow, sentinel)
	}, marshaller)

	executable := executor.ExecuteFrom(container)
	_, err := executable.Execute(t.Context(), []byte("input"))

	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, marshaller.unmarshalCalls)
	assert.Equal(t, 0, marshaller.marshalCalls)
}
