package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/futura-platform/f4a/cmd/dispatch/internal/k8s"
	"github.com/futura-platform/f4a/cmd/dispatch/internal/scheduler"
	"github.com/futura-platform/f4a/internal/util"
	serverutil "github.com/futura-platform/f4a/internal/util/server"
	"github.com/futura-platform/f4a/pkg/constants"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// This program is designed to run as a stateless k8s operator that facilitates
// the dispatching of tasks from the pending set, to individual worker sets.

const (
	leaderLeaseDuration = 30 * time.Second
	leaderRenewDeadline = 20 * time.Second
	leaderRetryPeriod   = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, leaderElectionName, err := loadConfig()
	if err != nil {
		return err
	}

	dbRoot, err := util.CreateOrOpenDefaultDbRoot()
	if err != nil {
		return fmt.Errorf("failed to create or open default db root: %w", err)
	}

	restCfg, err := k8s.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load kubernetes config: %w", err)
	}
	clients, err := k8s.NewClients(restCfg)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clients: %w", err)
	}

	s, _ := serverutil.NewBaseK8sService(dbRoot, func() int {
		return http.StatusOK
	}, func() int {
		return http.StatusOK
	})
	port, err := util.RequiredPort("DISPATCH_PORT")
	if err != nil {
		return err
	}
	s.Addr = fmt.Sprintf(":%d", port)

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.RegisterOnShutdown(cancel)

	group, ctx := errgroup.WithContext(baseCtx)
	group.Go(func() error {
		err := runWithLeaderElection(ctx, cfg, leaderElectionName, dbRoot, clients)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), constants.SHUTDOWN_TIMEOUT)
		defer cancel()
		if shutdownErr := s.Shutdown(shutdownCtx); shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
			return errors.Join(err, shutdownErr)
		}
		return err
	})
	group.Go(func() error {
		err := serverutil.ListenAndServe(s, constants.SHUTDOWN_TIMEOUT)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	return group.Wait()
}

func runWithLeaderElection(
	ctx context.Context,
	cfg scheduler.Config,
	leaderElectionName string,
	dbRoot util.DbRoot,
	clients *k8s.Clients,
) error {
	if clients == nil || clients.Core == nil {
		return fmt.Errorf("kubernetes client is required for leader election")
	}
	identity, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname for leader election: %w", err)
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaderElectionName,
			Namespace: cfg.Namespace,
		},
		Client: clients.Core.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	errCh := make(chan error, 1)
	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		leaderelection.RunOrDie(leaderCtx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			LeaseDuration:   leaderLeaseDuration,
			RenewDeadline:   leaderRenewDeadline,
			RetryPeriod:     leaderRetryPeriod,
			ReleaseOnCancel: true,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(runCtx context.Context) {
					slog.Info("dispatch leadership acquired", "identity", identity, "lock", leaderElectionName)
					errCh <- scheduler.Run(runCtx, cfg, dbRoot, clients)
				},
				OnStoppedLeading: func() {
					slog.Warn("dispatch leadership lost", "identity", identity, "lock", leaderElectionName)
				},
			},
		})
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func loadConfig() (scheduler.Config, string, error) {
	namespace := resolveNamespace()
	if namespace == "" {
		return scheduler.Config{}, "", fmt.Errorf("NAMESPACE or POD_NAMESPACE is required")
	}
	statefulSetName := strings.TrimSpace(os.Getenv("STATEFULSET_NAME"))
	if statefulSetName == "" {
		return scheduler.Config{}, "", fmt.Errorf("STATEFULSET_NAME is required")
	}
	leaderElectionName := strings.TrimSpace(os.Getenv("LEADER_ELECTION_NAME"))
	if leaderElectionName == "" {
		return scheduler.Config{}, "", fmt.Errorf("LEADER_ELECTION_NAME is required")
	}

	metricsIntervalValue := strings.TrimSpace(os.Getenv("METRICS_INTERVAL"))
	if metricsIntervalValue == "" {
		return scheduler.Config{}, "", fmt.Errorf("METRICS_INTERVAL is required")
	}
	metricsInterval, err := time.ParseDuration(metricsIntervalValue)
	if err != nil {
		return scheduler.Config{}, "", fmt.Errorf("invalid METRICS_INTERVAL: %w", err)
	}

	scoreAlphaValue := strings.TrimSpace(os.Getenv("SCORE_EMA_ALPHA"))
	if scoreAlphaValue == "" {
		return scheduler.Config{}, "", fmt.Errorf("SCORE_EMA_ALPHA is required")
	}
	scoreAlpha, err := strconv.ParseFloat(scoreAlphaValue, 64)
	if err != nil {
		return scheduler.Config{}, "", fmt.Errorf("invalid SCORE_EMA_ALPHA: %w", err)
	}

	cfg := scheduler.Config{
		Namespace:       namespace,
		StatefulSetName: statefulSetName,
		MetricsInterval: metricsInterval,
		ScoreAlpha:      scoreAlpha,
		Logger:          slog.Default(),
	}

	return cfg, leaderElectionName, nil
}

func resolveNamespace() string {
	if value := strings.TrimSpace(os.Getenv("NAMESPACE")); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); value != "" {
		return value
	}
	return ""
}
