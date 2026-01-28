package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScoreCacheUpdate(t *testing.T) {
	cache := newScoreCache(0.2)

	scores := cache.Update(map[string]float64{
		"worker-a": 0.5,
	})
	require.InEpsilon(t, 0.5, scores["worker-a"], 0.0001)

	scores = cache.Update(map[string]float64{
		"worker-a": 1.0,
	})
	require.InEpsilon(t, 0.6, scores["worker-a"], 0.0001)

	scores = cache.Update(map[string]float64{})
	_, ok := scores["worker-a"]
	require.False(t, ok)
}

func TestSelectWeightedWorker(t *testing.T) {
	scores := map[string]float64{
		"worker-low":  0.1,
		"worker-high": 0.9,
	}

	counts := map[string]int{}
	for range 1000 {
		worker, ok := selectWeightedWorker(scores)
		require.True(t, ok)
		counts[worker]++
	}

	require.Greater(t, counts["worker-low"], counts["worker-high"])
}
