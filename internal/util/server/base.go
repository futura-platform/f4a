package serverutil

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// NewBaseK8sService creates a dbroot and a new http.Server with the base Kubernetes service endpoints already added to the mux. (health check, ready check)
// The ready check first checks that the database is writable, then calls the additionalReadyCheck function.
// Also, it adds the necessary protocols to the server to function in the f4a k8s cluster.
func NewBaseK8sService(
	dbr dbutil.DbRoot,
	healthCheck, additionalReadyCheck func() (status int),
) (*http.Server, *http.ServeMux) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(healthCheck())
	})
	readyKey := fdb.Key(fmt.Sprintf("%d/ready", time.Now().UnixNano()))
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		expected := []byte("ok")
		var actual fdb.FutureByteSlice
		_, err := dbr.TransactContext(r.Context(), func(tr fdb.Transaction) (any, error) {
			tr.Set(readyKey, expected)
			actual = tr.Get(readyKey)
			tr.Clear(readyKey)
			return nil, nil
		})
		if err != nil {
			fmt.Println("readyz transaction failed", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		} else if !bytes.Equal(actual.MustGet(), expected) {
			fmt.Println("readyz transaction integrity check failed", string(actual.MustGet()))
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(additionalReadyCheck())
	})

	p := new(http.Protocols)
	p.SetHTTP1(true)
	// Use h2c so we can serve HTTP/2 without TLS.
	p.SetUnencryptedHTTP2(true)

	return &http.Server{
		Handler:   mux,
		Protocols: p,
	}, mux
}

type ShutdownWrappedServer struct {
	*http.Server
	shuttingDown *atomic.Bool
}

func (s *ShutdownWrappedServer) Shutdown(ctx context.Context) error {
	s.shuttingDown.Store(true)
	return s.Server.Shutdown(ctx)
}
