package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInputSerializerRoundTrip(t *testing.T) {
	serializer := inputSerializer{}
	original := []byte("input payload")

	marshalled := serializer.Marshal(original)
	decoded, err := serializer.Unmarshal(marshalled)

	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}
