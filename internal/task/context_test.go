package task

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithTaskKey(t *testing.T) {
	t.Run("can access task id from context", func(t *testing.T) {
		ctx := WithTaskKey(context.Background(), TaskKey{
			id: Id("123"),
		})

		tk, ok := FromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, Id("123"), tk.Id())
	})

	t.Run("cannot access task id from context if not set", func(t *testing.T) {
		ctx := context.Background()
		tk, ok := FromContext(ctx)
		assert.False(t, ok)
		assert.Equal(t, Id(""), tk.Id())
	})
}
