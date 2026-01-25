package task

import (
	"testing"

	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorIdSerializerRoundTrip(t *testing.T) {
	serializer := executorIdSerializer{}
	original := execute.ExecutorId("executor-1")

	marshalled := serializer.Marshal(original)
	decoded, err := serializer.Unmarshal(marshalled)

	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}
