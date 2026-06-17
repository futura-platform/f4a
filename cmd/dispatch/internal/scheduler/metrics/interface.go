package schedulermetrics

import (
	"context"
	"time"
)

type AggregateType int

const (
	AggregateTypeAvg AggregateType = iota
	AggregateTypeMax
	AggregateTypeMin
	AggregateTypeP95
)

type QueryParameters struct {
	AggregateType AggregateType
	RunnerId      string

	// queries the range [From - LookbackDuration, From)
	From             time.Time
	LookbackDuration time.Duration
}

// return values are in percentage, from 0-1
type RunnerMetrics interface {
	Cpu(context.Context, QueryParameters) (float64, error)
	Memory(context.Context, QueryParameters) (float64, error)
}
