package task

import (
	"testing"

	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestResourceRequestSerializerRoundTrip(t *testing.T) {
	serializer := resourceRequestSerializer{}
	original := &taskv1.TaskResourceRequest{
		CpuMillis:   500,
		MemoryBytes: 1024,
	}

	marshalled := serializer.Marshal(original)
	decoded, err := serializer.Unmarshal(marshalled)

	require.NoError(t, err)
	assert.True(t, proto.Equal(original, decoded))
}

func TestResourceRequestSerializerNil(t *testing.T) {
	serializer := resourceRequestSerializer{}

	assert.Nil(t, serializer.Marshal(nil))

	decoded, err := serializer.Unmarshal(nil)
	require.NoError(t, err)
	assert.Nil(t, decoded)
}
