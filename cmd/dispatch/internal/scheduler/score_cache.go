package scheduler

type scoreCache struct {
	// alpha controls EMA smoothing; higher values react faster to new scores.
	alpha  float64
	scores map[string]float64
}

func newScoreCache(alpha float64) *scoreCache {
	return &scoreCache{
		alpha:  alpha,
		scores: make(map[string]float64),
	}
}

func (c *scoreCache) Update(snapshot map[string]float64) map[string]float64 {
	if c.scores == nil {
		c.scores = make(map[string]float64)
	}

	for name := range c.scores {
		if _, ok := snapshot[name]; !ok {
			delete(c.scores, name)
		}
	}

	for name, value := range snapshot {
		if previous, ok := c.scores[name]; ok {
			c.scores[name] = (1-c.alpha)*previous + c.alpha*value
			continue
		}
		c.scores[name] = value
	}

	return c.scores
}
