package reliablelock

import (
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

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

				acquiredLease := make(chan *Lease, 1)
				go func() {
					newLease, err := lock.Acquire(t.Context(), db.Database)
					if err != nil {
						t.Errorf("failed to acquire lock: %v", err)
						return
					}
					acquiredLease <- newLease
				}()

				time.Sleep(100 * time.Millisecond)
				require.NoError(t, preexistingLease.Release())
				select {
				case newLease := <-acquiredLease:
					require.NotNil(t, newLease)
					// release the new lease for the next test
					require.NoError(t, newLease.Release())
				case <-time.After(100 * time.Millisecond):
					t.Errorf("timeout waiting for new acquisition to happen")
				}
			})
			t.Run("waits for pre-existing holder to expire", func(t *testing.T) {
				expirationTime := time.Now().Add(200 * time.Millisecond)
				_, err = db.TransactContext(t.Context(), func(t fdb.Transaction) (any, error) {
					lock.writeExpirationKey(t, expirationTime)
					return nil, nil
				})
				require.NoError(t, err)

				acquiredLease := make(chan *Lease, 1)
				go func() {
					newLease, err := lock.Acquire(t.Context(), db.Database)
					if err != nil {
						t.Errorf("failed to acquire lock: %v", err)
						return
					}
					acquiredLease <- newLease
				}()

				select {
				case newLease := <-acquiredLease:
					require.WithinDuration(t, expirationTime, time.Now(), 10*time.Millisecond)
					require.NotNil(t, newLease)
					require.NoError(t, newLease.Release())
				case <-time.After(time.Second):
					t.Errorf("timeout waiting for new acquisition to happen")
				}
			})
		})
	})
}
