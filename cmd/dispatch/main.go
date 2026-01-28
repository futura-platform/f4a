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
)

// This program is designed to run as a stateless k8s operator that facilitates
// the dispatching of tasks from the pending set, to individual worker sets.

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
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
	s.Addr = fmt.Sprintf(":%d", constants.DISPATCH_PORT)

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.RegisterOnShutdown(cancel)

	group, ctx := errgroup.WithContext(baseCtx)
	group.Go(func() error {
		err := scheduler.Run(ctx, cfg, dbRoot, clients)

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

func loadConfig() (scheduler.Config, error) {
	namespace := resolveNamespace()
	if namespace == "" {
		return scheduler.Config{}, fmt.Errorf("NAMESPACE or POD_NAMESPACE is required")
	}
	statefulSetName := strings.TrimSpace(os.Getenv("STATEFULSET_NAME"))
	if statefulSetName == "" {
		return scheduler.Config{}, fmt.Errorf("STATEFULSET_NAME is required")
	}

	metricsIntervalValue := strings.TrimSpace(os.Getenv("METRICS_INTERVAL"))
	if metricsIntervalValue == "" {
		return scheduler.Config{}, fmt.Errorf("METRICS_INTERVAL is required")
	}
	metricsInterval, err := time.ParseDuration(metricsIntervalValue)
	if err != nil {
		return scheduler.Config{}, fmt.Errorf("invalid METRICS_INTERVAL: %w", err)
	}

	scoreAlphaValue := strings.TrimSpace(os.Getenv("SCORE_EMA_ALPHA"))
	if scoreAlphaValue == "" {
		return scheduler.Config{}, fmt.Errorf("SCORE_EMA_ALPHA is required")
	}
	scoreAlpha, err := strconv.ParseFloat(scoreAlphaValue, 64)
	if err != nil {
		return scheduler.Config{}, fmt.Errorf("invalid SCORE_EMA_ALPHA: %w", err)
	}

	cfg := scheduler.Config{
		Namespace:       namespace,
		StatefulSetName: statefulSetName,
		MetricsInterval: metricsInterval,
		ScoreAlpha:      scoreAlpha,
		Logger:          slog.Default(),
	}

	return cfg, nil
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
