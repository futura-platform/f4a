package execute_test

import (
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

		executor, err := router.Route("executor1")
		assert.NoError(t, err)
		assert.Equal(t, executor, executor)
	})

	t.Run("route to unknown executor", func(t *testing.T) {
		router := execute.NewRouter()
		_, err := router.Route("executor1")
		assert.ErrorIs(t, err, execute.ErrExecutorNotFound)
	})
}
