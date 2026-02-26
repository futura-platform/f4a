package scheduler

import (
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
)

func TestScoreCacheUpdate(t *testing.T) {
	cache := newScoreCache(0.2)

	snapshot := xsync.NewMap[string, float64]()
	snapshot.Store("worker-a", 0.5)
	scores := cache.Update(snapshot)
	v, ok := scores.Load("worker-a")
	require.True(t, ok)
	require.InEpsilon(t, 0.5, v, 0.0001)

	snapshot = xsync.NewMap[string, float64]()
	snapshot.Store("worker-a", 1.0)
	scores = cache.Update(snapshot)
	v, ok = scores.Load("worker-a")
	require.True(t, ok)
	require.InEpsilon(t, 0.6, v, 0.0001)

	scores = cache.Update(xsync.NewMap[string, float64]())
	_, ok = scores.Load("worker-a")
	require.False(t, ok)
}

func TestSelectWeightedWorker(t *testing.T) {
	scores := xsync.NewMap[string, float64]()
	scores.Store("worker-low", 0.1)
	scores.Store("worker-high", 0.9)

	counts := map[string]int{}
	for range 1000 {
		worker, ok := selectWeightedRunner(scores)
		require.True(t, ok)
		counts[worker]++
	}

	require.Greater(t, counts["worker-low"], counts["worker-high"])
}
