package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallbackUrlSerializerRoundTrip(t *testing.T) {
	serializer := callbackUrlSerializer{}
	original := "https://example.com/callback?mode=fast"

	marshalled := serializer.Marshal(original)
	decoded, err := serializer.Unmarshal(marshalled)

	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}
