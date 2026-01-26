package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunnerIdSerializerRoundTrip(t *testing.T) {
	serializer := runnerIdSerializer{}
	original := "runner-id"

	marshalled := serializer.Marshal(original)
	decoded, err := serializer.Unmarshal(marshalled)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}
