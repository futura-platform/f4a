package f4a

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
)

const (
	fdbClusterFileName = "f4a-fdb-test.cluster"
	serverWaitTimeout  = 10 * time.Second
)

func TestStart_HealthzReadyz(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(_ dbutil.DbRoot) {
		t.Setenv("FDB_CLUSTER_FILE", requireClusterFile(t))

		address := freeAddress(t)
		cancel, errCh := startServer(t, address)
		t.Cleanup(cancel)

		client := httpClient()
		baseURL := "http://" + address

		waitForHTTPStatus(t, client, http.MethodGet, baseURL+"/healthz", http.StatusOK)
		waitForHTTPStatus(t, client, http.MethodPost, baseURL+"/readyz", http.StatusOK)

		cancel()
		if err := waitForStartReturn(t, errCh); err == nil {
			t.Fatalf("expected shutdown error")
		}
	})
}

func TestStart_ShutdownOnContextCancel(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(_ dbutil.DbRoot) {
		t.Setenv("FDB_CLUSTER_FILE", requireClusterFile(t))

		address := freeAddress(t)
		cancel, errCh := startServer(t, address)
		t.Cleanup(cancel)

		client := httpClient()
		baseURL := "http://" + address

		waitForHTTPStatus(t, client, http.MethodGet, baseURL+"/healthz", http.StatusOK)

		cancel()
		err := waitForStartReturn(t, errCh)
		if err == nil {
			t.Fatalf("expected shutdown error")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation error, got %v", err)
		}
		waitForServerDown(t, client, baseURL+"/healthz")
	})
}

func requireClusterFile(t *testing.T) string {
	t.Helper()
	clusterFile := filepath.Join(os.TempDir(), fdbClusterFileName)
	if _, err := os.Stat(clusterFile); err != nil {
		t.Fatalf("cluster file not found at %s: %v", clusterFile, err)
	}
	return clusterFile
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("failed to release port: %v", err)
	}
	return address
}

func httpClient() *http.Client {
	return &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
}

func startServer(t *testing.T, address string) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- startOnAddress(ctx, address, nil)
	}()
	return cancel, errCh
}

func waitForStartReturn(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(serverWaitTimeout):
		t.Fatal("timed out waiting for server shutdown")
		return nil
	}
}

func waitForHTTPStatus(t *testing.T, client *http.Client, method, url string, status int) {
	t.Helper()
	deadline := time.Now().Add(serverWaitTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == status {
				return
			}
			lastErr = fmt.Errorf("unexpected status: %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s %s to return %d: %v", method, url, status, lastErr)
}

func waitForServerDown(t *testing.T, client *http.Client, url string) {
	t.Helper()
	deadline := time.Now().Add(serverWaitTimeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("failed to build request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server still responding on %s", url)
}

func TestResolveServerLoopExit(t *testing.T) {
	t.Run("passes through non server-closed errors", func(t *testing.T) {
		errSentinel := errors.New("boom")
		err := resolveServerLoopExit(errSentinel, false)
		if !errors.Is(err, errSentinel) {
			t.Fatalf("expected sentinel error, got %v", err)
		}
	})

	t.Run("returns nil when work loop initiated shutdown", func(t *testing.T) {
		err := resolveServerLoopExit(http.ErrServerClosed, true)
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("returns context canceled when server closed externally", func(t *testing.T) {
		err := resolveServerLoopExit(http.ErrServerClosed, false)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	})
}

func TestWithDrain(t *testing.T) {
	options := new(StartOptions)
	drainErr := errors.New("drain failed")
	type contextKey string
	const key contextKey = "test-key"

	WithDrain(func(ctx context.Context) error {
		if got := ctx.Value(key); got != "test-value" {
			t.Fatalf("expected context value to be preserved, got %v", got)
		}
		return drainErr
	})(options)

	if options.Drain == nil {
		t.Fatal("expected drain option to be set")
	}

	err := options.Drain(context.WithValue(context.Background(), key, "test-value"))
	if !errors.Is(err, drainErr) {
		t.Fatalf("expected drain error %v, got %v", drainErr, err)
	}
}
