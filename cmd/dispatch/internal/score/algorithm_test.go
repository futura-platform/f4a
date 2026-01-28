package score

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerUtilizationScore(t *testing.T) {
	score := WorkerUtilizationScore(0.4, 0.9)
	require.InEpsilon(t, 0.91, score, 0.0001)
}
