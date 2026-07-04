package execute_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestExecutableSettleSuccess(t *testing.T) {
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

	capturedCh := make(chan []byte, 1)
	server := testutil.NewEphemeralHTTPServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		capturedCh <- body
		w.WriteHeader(http.StatusAccepted)
	})

	callbackURL, err := url.Parse(server.URL + "/callback")
	require.NoError(t, err)

	executable := executor.ExecuteFrom(execute.SettlementContainers{
		User:      executiontype.NewInMemoryContainer(),
		Discharge: executiontype.NewInMemoryContainer(),
	})
	err = executable.Settle(context.Background(), []byte("input"), callbackURL)

	assert.NoError(t, err)
	assert.Equal(t, "input", received)
	assert.Equal(t, 1, marshaller.unmarshalCalls)
	assert.Equal(t, 1, marshaller.marshalCalls)
	assert.Equal(t, []byte("input-out"), <-capturedCh)
}

func TestExecutableSettleUnmarshalError(t *testing.T) {
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

	executable := executor.ExecuteFrom(execute.SettlementContainers{
		User:      executiontype.NewInMemoryContainer(),
		Discharge: executiontype.NewInMemoryContainer(),
	})
	err := executable.Settle(context.Background(), []byte("input"), nil)

	assert.ErrorIs(t, err, sentinel)
	assert.False(t, called)
	assert.Equal(t, 1, marshaller.unmarshalCalls)
	assert.Equal(t, 0, marshaller.marshalCalls)
}

func TestExecutableSettleFlowError(t *testing.T) {
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

	executable := executor.ExecuteFrom(execute.SettlementContainers{
		User:      executiontype.NewInMemoryContainer(),
		Discharge: executiontype.NewInMemoryContainer(),
	})
	err := executable.Settle(t.Context(), []byte("input"), nil)

	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, marshaller.unmarshalCalls)
	assert.Equal(t, 0, marshaller.marshalCalls)
}
