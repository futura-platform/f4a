package serverutil_test

import (
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/util"
	serverutil "github.com/futura-platform/f4a/internal/util/server"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func ephemeralListener() (net.Listener, error) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	return ln, nil
}

func withEphemeralBaseK8sService(t *testing.T,
	healthCheck, additionalReadyCheck func() (status int), fn func(
		t *testing.T,
		db fdb.DatabaseOptions,
		addr net.Addr,
		srv *http.Server,
		startServing func(),
	)) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		ln, err := ephemeralListener()
		require.NoError(t, err)
		srv, _ := serverutil.NewBaseK8sService(db, healthCheck, additionalReadyCheck)
		startServing := make(chan struct{})
		go func() {
			err := srv.Serve(ln)
			require.ErrorIs(t, err, http.ErrServerClosed)
		}()
		defer srv.Shutdown(t.Context())
		db.Options()
		fn(t, db.Options(), ln.Addr(), srv, func() { close(startServing) })
	})
}

func TestBaseK8sService(t *testing.T) {
	t.Run("uses the provided health check", func(t *testing.T) {
		withEphemeralBaseK8sService(t, func() (status int) {
			return http.StatusTeapot
		}, func() (status int) {
			return http.StatusOK
		}, func(t *testing.T, db fdb.DatabaseOptions, addr net.Addr, _ *http.Server, startServing func()) {
			startServing()
			resp, err := http.Get(fmt.Sprintf("http://%s/healthz", addr))
			require.NoError(t, err)
			require.Equal(t, http.StatusTeapot, resp.StatusCode)
		})
	})
	t.Run("uses the provided additional ready check", func(t *testing.T) {
		withEphemeralBaseK8sService(t, func() (status int) {
			return http.StatusOK
		}, func() (status int) {
			return http.StatusTeapot
		}, func(t *testing.T, db fdb.DatabaseOptions, addr net.Addr, _ *http.Server, startServing func()) {
			startServing()
			resp, err := http.Get(fmt.Sprintf("http://%s/readyz", addr))
			require.NoError(t, err)
			require.Equal(t, http.StatusTeapot, resp.StatusCode)
		})
	})
	t.Run("returns ServiceUnavailable if FDB fails", func(t *testing.T) {
		withEphemeralBaseK8sService(t, func() (status int) {
			return http.StatusOK
		}, func() (status int) {
			return http.StatusTeapot
		}, func(t *testing.T, opts fdb.DatabaseOptions, addr net.Addr, _ *http.Server, startServing func()) {
			opts.SetTransactionTimeout(1)
			startServing()
			resp, err := http.Get(fmt.Sprintf("http://%s/readyz", addr))
			require.NoError(t, err)
			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
			opts.SetTransactionTimeout(10000)
		})
	})
}
