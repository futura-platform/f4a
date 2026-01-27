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
	"github.com/futura-platform/f4a/internal/util"
	"github.com/futura-platform/f4a/pkg/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type contextProvider interface {
	Context() context.Context
}

func testContext(t testing.TB) context.Context {
	if ctxProvider, ok := t.(contextProvider); ok {
		return ctxProvider.Context()
	}
	return context.Background()
}

func WithEphemeralDBRoot(t testing.TB, fn func(db util.DbRoot)) {
	fdb.MustAPIVersion(constants.FDB_API_VERSION)
	ctx := testContext(t)

	// Extend Ryuk timeout to allow for debugging sessions
	// Default is 1m which is too short when paused at breakpoints
	os.Setenv("TESTCONTAINERS_RYUK_CONNECTION_TIMEOUT", "10m")

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

	clusterFile, err := setupClusterFile(ctx, c)
	require.NoError(t, err)

	path := []string{"f4a", "test", t.Name(), fmt.Sprintf("%d", rand.Int())}
	db, err := util.CreateOrOpenDbRoot(path, func() (fdb.Database, error) {
		return fdb.OpenDatabase(clusterFile)
	})
	require.NoError(t, err)

	// clear this path from any previous tests
	_, err = directory.Root().Remove(db, path)
	require.NoError(t, err)
	err = db.Options().SetTransactionRetryLimit(0)
	require.NoError(t, err)
	err = db.Options().SetTransactionTimeout(10000)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err := directory.Root().Remove(db, path)
		assert.NoError(t, err)
	})
	fn(db)
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
