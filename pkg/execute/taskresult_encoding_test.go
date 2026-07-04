package execute

import (
	"bytes"
	"net/url"
	"testing"
	"time"

	"github.com/futura-platform/futura/ftype/seal"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/require"
)

func TestTaskResultSealRoundTrip(t *testing.T) {
	for name, build := range map[string]func() protoTaskResult{
		"result": func() protoTaskResult {
			return newTaskResultSuccess([]byte("payload"))
		},
		"failure": func() protoTaskResult {
			return newTaskResultFailure("boom")
		},
	} {
		t.Run(name, func(t *testing.T) {
			orig := seal.Seal(build())
			got := orig.V()
			require.Equal(t, orig.V().GetResult(), got.GetResult())
			require.Equal(t, orig.V().GetFailure(), got.GetFailure())
			require.Equal(t, orig.V().HasResult(), got.HasResult())
			require.Equal(t, orig.V().HasFailure(), got.HasFailure())
		})
	}
}

func TestDeliveryRequestPrivateEncodingRoundTrip(t *testing.T) {
	privateencoding.Register[seal.Sealed[protoTaskResult]]()
	u, err := url.Parse("http://example.com/callback")
	require.NoError(t, err)

	orig := deliveryRequest{
		completedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		callbackUrl: *u,
		result:      seal.Seal(newTaskResultSuccess([]byte("out"))),
	}

	buf := bytes.NewBuffer(nil)
	enc := privateencoding.NewEncoder[deliveryRequest](buf)
	require.NoError(t, enc.Encode(orig))
	dec := privateencoding.NewDecoder[deliveryRequest](buf)
	got, err := dec.Decode()
	require.NoError(t, err)
	require.True(t, orig.completedAt.Equal(got.completedAt))
	require.Equal(t, orig.callbackUrl.String(), got.callbackUrl.String())
	require.Equal(t, orig.result.V().GetResult(), got.result.V().GetResult())
}
