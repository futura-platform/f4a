package scheduler

import "github.com/puzpuzpuz/xsync/v4"

type scoreCache struct {
	// alpha controls EMA smoothing; higher values react faster to new scores.
	alpha  float64
	scores *xsync.Map[string, float64]
}

func newScoreCache(alpha float64) *scoreCache {
	return &scoreCache{
		alpha:  alpha,
		scores: xsync.NewMap[string, float64](),
	}
}

func (c *scoreCache) Update(snapshot *xsync.Map[string, float64]) *xsync.Map[string, float64] {
	c.scores.Range(func(name string, value float64) bool {
		if _, ok := snapshot.Load(name); !ok {
			c.scores.Delete(name)
		}
		return true
	})

	snapshot.Range(func(name string, value float64) bool {
		if previous, ok := c.scores.Load(name); ok {
			c.scores.Store(name, (1-c.alpha)*previous+c.alpha*value)
		} else {
			c.scores.Store(name, value)
		}
		return true
	})

	return c.scores
}
