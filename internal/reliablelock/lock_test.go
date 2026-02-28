package reliablelock

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type acquireResult struct {
	lease *Lease
	err   error
}

func startAcquire(ctx context.Context, lock *Lock, db fdb.Database) <-chan acquireResult {
	ch := make(chan acquireResult, 1)
	go func() {
		lease, err := lock.Acquire(ctx, db)
		ch <- acquireResult{lease: lease, err: err}
	}()
	return ch
}

func receiveAcquireResult(ch <-chan acquireResult, result *acquireResult, received *bool) func() bool {
	return func() bool {
		if *received {
			return true
		}
		select {
		case *result = <-ch:
			*received = true
			return true
		default:
			return false
		}
	}
}

func TestTryAcquireLock(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		lockDir, err := db.Root.CreateOrOpen(db, []string{"test_lock"}, nil)
		require.NoError(t, err)
		lock := NewLock(lockDir)

		var lease *Lease
		t.Run("acquisition without pre-existing holder", func(t *testing.T) {
			var holderExpiration time.Time
			lease, holderExpiration, err = lock.TryAcquire(t.Context(), db.Database, db, DefaultLeaseOptions())
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.Zero(t, holderExpiration)
		})
		t.Run("acquisition with pre-existing holder", func(t *testing.T) {
			failedLease, holderExpiration, err := lock.TryAcquire(t.Context(), db.Database, db, DefaultLeaseOptions())
			require.NoError(t, err)
			require.Nil(t, failedLease)
			exp, err := lease.Expiration(db)
			require.NoError(t, err)
			require.Equal(t, exp, holderExpiration)
		})
		t.Run("acquisition after pre-existing holder has been released", func(t *testing.T) {
			require.NoError(t, lease.Release())
			newLease, holderExpiration, err := lock.TryAcquire(t.Context(), db.Database, db, DefaultLeaseOptions())
			require.NoError(t, err)
			require.NotNil(t, newLease)
			require.Zero(t, holderExpiration)
		})
	})
}

func TestAcquireLock(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		lockDir, err := db.Root.CreateOrOpen(db, []string{"test_lock"}, nil)
		require.NoError(t, err)
		lock := NewLock(lockDir)

		t.Run("acquisition without pre-existing holder", func(t *testing.T) {
			lease, err := lock.Acquire(t.Context(), db.Database)
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.NoError(t, lease.Release())
		})
		t.Run("if there is a pre-existing holder, acquisition retries when possible", func(t *testing.T) {
			t.Run("waits for pre-existing holder to release", func(t *testing.T) {
				preexistingLease, err := lock.Acquire(t.Context(), db.Database)
				require.NoError(t, err)
				require.NotNil(t, preexistingLease)

				acquireResultCh := startAcquire(t.Context(), lock, db.Database)
				var acquireResult acquireResult
				var receivedAcquire bool
				receivedAcquireResult := receiveAcquireResult(acquireResultCh, &acquireResult, &receivedAcquire)

				if !assert.Never(t, receivedAcquireResult, 100*time.Millisecond, 10*time.Millisecond) {
					if acquireResult.lease != nil {
						require.NoError(t, acquireResult.lease.Release())
					}
					t.FailNow()
				}
				require.NoError(t, preexistingLease.Release())
				require.Eventually(t, receivedAcquireResult, time.Second, 10*time.Millisecond)
				require.NoError(t, acquireResult.err)
				require.NotNil(t, acquireResult.lease)
				// release the new lease for the next test
				require.NoError(t, acquireResult.lease.Release())
			})
			t.Run("waits for pre-existing holder to expire", func(t *testing.T) {
				expirationTime := time.Now().Add(200 * time.Millisecond)
				_, err = db.TransactContext(t.Context(), func(t fdb.Transaction) (any, error) {
					lock.writeExpirationKey(t, expirationTime)
					return nil, nil
				})
				require.NoError(t, err)

				acquireResultCh := startAcquire(t.Context(), lock, db.Database)
				var acquireResult acquireResult
				var receivedAcquire bool
				receivedAcquireResult := receiveAcquireResult(acquireResultCh, &acquireResult, &receivedAcquire)

				preExpirationWindow := time.Until(expirationTime.Add(-50 * time.Millisecond))
				if preExpirationWindow > 0 {
					if !assert.Never(t, receivedAcquireResult, preExpirationWindow, 10*time.Millisecond) {
						if acquireResult.lease != nil {
							require.NoError(t, acquireResult.lease.Release())
						}
						t.FailNow()
					}
				}

				require.Eventually(t, receivedAcquireResult, time.Second, 10*time.Millisecond)
				require.NoError(t, acquireResult.err)
				require.NotNil(t, acquireResult.lease)
				require.NoError(t, acquireResult.lease.Release())
			})
		})
	})
}
