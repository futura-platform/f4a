package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/futura-platform/f4a/cmd/gateway/internal/api"
	"github.com/futura-platform/f4a/internal/gen/task/v1/taskv1connect"
	"github.com/futura-platform/f4a/internal/util"
	serverutil "github.com/futura-platform/f4a/internal/util/server"
	"github.com/futura-platform/f4a/pkg/constants"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	dbRoot, err := util.CreateOrOpenDefaultDbRoot()
	if err != nil {
		return fmt.Errorf("failed to create or open default db root: %w", err)
	}

	controller, err := api.NewController(dbRoot)
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	s, mux := serverutil.NewBaseK8sService(dbRoot, func() int {
		return http.StatusOK
	}, func() int {
		return http.StatusOK
	})
	s.Addr = fmt.Sprintf(":%d", constants.GATEWAY_PORT)
	mux.Handle(taskv1connect.NewControlServiceHandler(controller))

	err = serverutil.ListenAndServe(s, constants.SHUTDOWN_TIMEOUT)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("failed to listen and serve: %w", err)
	}
	return nil
}
