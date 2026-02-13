package dbutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestTransactContext(t *testing.T) {
	t.Run("cancels transaction if context is cancelled", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			_, err := db.TransactContext(ctx, func(tr fdb.Transaction) (any, error) {
				for {
					tr.Get(fdb.Key("infinite_read_%d")).MustGet()
				}
			})
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	})
}
