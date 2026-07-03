package testutil

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The fixture shares one FoundationDB container across the whole test run,
// keyed by a fixed name and a fixed host port (4500), because FDB clients
// connect to the concrete address baked into the cluster file — dynamic
// per-container ports aren't workable. `go test ./...` compiles one binary
// per package and runs several concurrently, and each binary is its own
// testcontainers "session" with its own Ryuk reaper. Ryuk reaps by the
// session that *created* a container, so when the first package's binary
// exits, its reaper tears the shared container down while sibling packages
// are still using it — surfacing as "container is marked for removal" and
// FDB 1031 timeouts in unrelated tests.
//
// A single container that must outlive individual sessions is exactly the
// case Ryuk should not manage, so we opt this process out of reaping. The
// container is the only one this repo starts (verified: ephemeraldb.go is the
// sole testcontainers user), it is reused by name across runs, and
// testcontainers restarts it if it was stopped — so "leaking" it is the
// intended persistence of a reused fixture, not a resource leak. An explicit
// value is respected so a caller can force Ryuk back on if they accept
// per-binary containers.
//
// Set in init() so it lands before testcontainers' cached config.Read().
func init() {
	if _, ok := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !ok {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}

type contextProvider interface {
	Context() context.Context
}

func testContext(t testing.TB) context.Context {
	if ctxProvider, ok := t.(contextProvider); ok {
		return ctxProvider.Context()
	}
	return context.Background()
}

func WithEphemeralDBRoot(t testing.TB, fn func(db dbutil.DbRoot)) {
	fdb.MustAPIVersion(constants.FDB_API_VERSION)
	ctx := testContext(t)

	req := testcontainers.ContainerRequest{
		Name:         "f4a-fdb-test",
		Image:        "foundationdb/foundationdb:7.3.69",
		ExposedPorts: []string{"4500:4500/tcp"},
		Env: map[string]string{
			"FDB_NETWORKING_MODE": "host",
		},
		WaitingFor: wait.ForLog("FDBD joined cluster").WithStartupTimeout(30 * time.Second),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            true,
	})
	require.NoError(t, err)
	// First time setup: configure the database
	// This will fail if already configured (e.g., cluster file was deleted but container still running)
	exitCode, output, err := c.Exec(ctx, []string{"fdbcli", "--exec", "configure new single memory"})
	require.NoError(t, err)

	// Exit code 0 = success, non-zero might mean already configured which is fine
	if exitCode != 0 {
		// Read output to check if it's just "already configured"
		buf := make([]byte, 1024)
		n, _ := output.Read(buf)
		outputStr := string(buf[:n])
		if !strings.Contains(outputStr, "already") && !strings.Contains(outputStr, "Configuration") {
			t.Fatal(fmt.Errorf("fdbcli configure failed with exit code %d: %s", exitCode, outputStr))
			return
		}
	}

	// "FDBD joined cluster" only means the process started (fdbserver logs go
	// to trace files, so no stdout line ever indicates availability), and a
	// fresh database is unavailable until the configure above completes its
	// recovery. Gate on the status probe or the first transactions race the
	// recovery and die with 1031 timeouts.
	require.NoError(t, waitForDatabaseAvailable(ctx, c, 30*time.Second))

	clusterFile, err := setupClusterFile(ctx, c)
	require.NoError(t, err)

	path := []string{"f4a", "test", t.Name(), fmt.Sprintf("%d", rand.Int())}
	db, err := dbutil.CreateOrOpenDbRoot(path, func() (fdb.Database, error) {
		return fdb.OpenDatabase(clusterFile)
	})
	require.NoError(t, err)

	// clear this path from any previous tests
	_, err = directory.Root().Remove(db, path)
	require.NoError(t, err)
	err = db.Options().SetTransactionRetryLimit(2)
	require.NoError(t, err)
	err = db.Options().SetTransactionTimeout(10000)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err := directory.Root().Remove(db, path)
		assert.NoError(t, err)
	})
	fn(db)
}

// waitForDatabaseAvailable polls fdbcli until the cluster reports the
// database as available ("status minimal" prints "The database is available."
// once recovery completes). Availability is a status property, not a log
// line — it cannot be expressed as a container WaitingFor strategy on first
// boot because the database only becomes configurable after startup.
func waitForDatabaseAvailable(ctx context.Context, c testcontainers.Container, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastDiag string
	for time.Now().Before(deadline) {
		exitCode, reader, err := c.Exec(ctx, []string{"fdbcli", "--exec", "status minimal"})
		// capture output on every path (exec error, non-zero exit, or success)
		// so a persistent failure — e.g. a container being torn down — is
		// diagnosable rather than an empty string.
		var out string
		if reader != nil {
			if b, readErr := io.ReadAll(reader); readErr == nil {
				out = string(b)
			}
		}
		switch {
		case err != nil:
			lastDiag = fmt.Sprintf("exec error: %v", err)
		case exitCode != 0:
			lastDiag = fmt.Sprintf("exit %d: %s", exitCode, strings.TrimSpace(out))
		default:
			lastDiag = strings.TrimSpace(out)
			if strings.Contains(out, "The database is available") {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("database not available after %s; last status: %s", timeout, lastDiag)
}

const (
	fdbClusterFileName = "f4a-fdb-test.cluster"
)

func setupClusterFile(ctx context.Context, c testcontainers.Container) (string, error) {
	// Use a stable path so reused containers can reuse the cluster file
	clusterFilePath := filepath.Join(os.TempDir(), fdbClusterFileName)

	// Check if cluster file already exists and is valid
	if _, err := os.Stat(clusterFilePath); err == nil {
		return clusterFilePath, nil
	}

	clusterPaths := []string{"/etc/foundationdb/fdb.cluster", "/var/fdb/fdb.cluster"}
	var exitCode int
	var reader io.Reader
	var err error
	for _, path := range clusterPaths {
		exitCode, reader, err = c.Exec(ctx, []string{"cat", path})
		if err == nil && exitCode == 0 {
			break
		}
	}
	if err != nil {
		return "", fmt.Errorf("reading cluster file: %w", err)
	}
	if exitCode != 0 {
		return "", fmt.Errorf("cluster file read failed with exit code %d", exitCode)
	}

	buf := make([]byte, 1024)
	n, err := reader.Read(buf)
	if err != nil {
		return "", fmt.Errorf("reading cluster file content: %w", err)
	}

	clusterContent := string(buf[:n])
	for i := 0; i < len(clusterContent); i++ {
		ch := clusterContent[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			clusterContent = clusterContent[i:]
			break
		}
	}
	clusterContent = strings.TrimSpace(clusterContent)

	if err := os.WriteFile(clusterFilePath, []byte(clusterContent+"\n"), 0644); err != nil {
		return "", fmt.Errorf("writing cluster file: %w", err)
	}

	return clusterFilePath, nil
}
