package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/futura-platform/f4a/cmd/gateway/internal/api"
	"github.com/futura-platform/f4a/internal/gen/task/v1/taskv1connect"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/internal/util"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	serverutil "github.com/futura-platform/f4a/internal/util/server"
	"github.com/futura-platform/f4a/pkg/constants"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// This program is designed to run as a stateless http server that receives
// task control requests from clients and atomically handles them in a db transaction.

func main() {
	slog.Info("starting gateway")
	shutdownOTEL, err := serverutil.BootstrapOTEL(context.Background())
	if err != nil {
		slog.Error("otel bootstrap failed", "error", err)
		os.Exit(1)
	}
	defer shutdownOTEL(context.Background())

	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	dbRoot, err := dbutil.CreateOrOpenDefaultDbRoot()
	if err != nil {
		return fmt.Errorf("failed to create or open default db root: %w", err)
	}

	controller, releaseController, err := api.NewController(dbRoot)
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}
	defer releaseController()

	queryService, err := api.NewQueryService(dbRoot)
	if err != nil {
		return fmt.Errorf("failed to create query service: %w", err)
	}

	s, mux := serverutil.NewBaseK8sService(dbRoot, func() int {
		return http.StatusOK
	}, func() int {
		return http.StatusOK
	})
	s.RegisterOnShutdown(releaseController)

	gcCtx, cancelGC := context.WithCancel(context.Background())
	defer cancelGC()
	s.RegisterOnShutdown(cancelGC)
	go task.RunRevisionGCLoop(gcCtx, dbRoot)

	port, err := util.RequiredPort(constants.GatewayPort)
	if err != nil {
		return err
	}
	slog.Info("gateway listening", "port", port)
	s.Addr = fmt.Sprintf(":%d", port)
	controlPath, controlHandler := taskv1connect.NewControlServiceHandler(
		controller,
		connect.WithInterceptors(validate.NewInterceptor()),
	)
	mux.Handle(controlPath, otelhttp.NewHandler(controlHandler, taskv1connect.ControlServiceName))

	queryPath, queryHandler := taskv1connect.NewQueryServiceHandler(
		queryService,
		connect.WithInterceptors(validate.NewInterceptor()),
	)
	mux.Handle(queryPath, otelhttp.NewHandler(queryHandler, taskv1connect.QueryServiceName))

	err = serverutil.K8sAwareListenAndServe(s, constants.SHUTDOWN_TIMEOUT, nil)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("failed to listen and serve: %w", err)
	}
	return nil
}
