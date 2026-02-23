package main

import (
	"testing"

	"github.com/futura-platform/f4a/cmd/dispatch/internal/scheduler"
	"github.com/futura-platform/f4a/pkg/constants"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigUsesDefaultBatchParallelism(t *testing.T) {
	setRequiredDispatchEnv(t)
	t.Setenv(constants.AssignmentBatchParallelism, "")

	cfg, leaderElectionName, err := loadConfig()
	require.NoError(t, err)
	require.Equal(t, "test-leader-election", leaderElectionName)
	require.Equal(t, scheduler.DefaultBatchParallelism, cfg.BatchTxParallelism)
}

func TestLoadConfigUsesEnvBatchParallelism(t *testing.T) {
	setRequiredDispatchEnv(t)
	t.Setenv(constants.AssignmentBatchParallelism, "7")

	cfg, _, err := loadConfig()
	require.NoError(t, err)
	require.Equal(t, 7, cfg.BatchTxParallelism)
}

func TestLoadConfigRejectsInvalidBatchParallelism(t *testing.T) {
	setRequiredDispatchEnv(t)
	t.Setenv(constants.AssignmentBatchParallelism, "0")

	_, _, err := loadConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, constants.AssignmentBatchParallelism)
}

func setRequiredDispatchEnv(t *testing.T) {
	t.Helper()
	t.Setenv(constants.Namespace, "test-namespace")
	t.Setenv(constants.PodNamespace, "")
	t.Setenv(constants.StatefulSetName, "test-statefulset")
	t.Setenv(constants.LeaderElectionName, "test-leader-election")
	t.Setenv(constants.MetricsInterval, "5s")
	t.Setenv(constants.ScoreEmaAlpha, "0.5")
}
