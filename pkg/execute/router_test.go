package execute_test

import (
	"context"
	"testing"

	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura"
	"github.com/stretchr/testify/assert"
)

func TestRouter(t *testing.T) {
	t.Run("route to executor", func(t *testing.T) {
		executor := execute.NewExecutor(func(b futura.FlowBuilder, args struct{}) (struct{}, error) {
			return struct{}{}, nil
		}, nil)
		router := execute.NewRouter(
			execute.Route{
				Id:       "executor1",
				Executor: executor,
			},
		)

		routed := router.Route("executor1")
		assert.Equal(t, executor, routed)
	})

	t.Run("route to unknown executor", func(t *testing.T) {
		router := execute.NewRouter()
		routed := router.Route("executor1")
		_, err := routed.ExecuteFrom(nil).Execute(context.Background(), nil)
		assert.ErrorIs(t, err, execute.ErrExecutorNotFound)
	})
}
