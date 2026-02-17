package task

import (
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestRevisionStoreApplyRules(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		store, err := CreateOrOpenRevisionStore(db)
		require.NoError(t, err)

		id := Id("revision-rules-task")

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			_, applyErr := store.Apply(tx, id, 2, RevisionOperationCreate, func() error { return nil })
			return nil, applyErr
		})
		require.ErrorIs(t, err, ErrCreateRevisionMustBeOne)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			decision, applyErr := store.Apply(tx, id, 1, RevisionOperationCreate, func() error { return nil })
			require.Equal(t, RevisionDecisionApplied, decision)
			return nil, applyErr
		})
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			decision, applyErr := store.Apply(tx, id, 1, RevisionOperationCreate, func() error { return nil })
			require.Equal(t, RevisionDecisionDuplicate, decision)
			return nil, applyErr
		})
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			_, applyErr := store.Apply(tx, id, 3, RevisionOperationMutate, func() error { return nil })
			return nil, applyErr
		})
		require.ErrorIs(t, err, ErrRevisionGap)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			decision, applyErr := store.Apply(tx, id, 2, RevisionOperationMutate, func() error { return nil })
			require.Equal(t, RevisionDecisionApplied, decision)
			return nil, applyErr
		})
		require.NoError(t, err)
	})
}

func TestRevisionStoreSweepExpiredTombstones(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		store, err := CreateOrOpenRevisionStore(db)
		require.NoError(t, err)

		fixedNow := time.Unix(1700000000, 0)
		store.now = func() time.Time { return fixedNow }
		store.tombstoneTTL = 0

		id := Id("gc-task")
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			_, applyErr := store.Apply(tx, id, 1, RevisionOperationCreate, func() error { return nil })
			if applyErr != nil {
				return nil, applyErr
			}
			_, applyErr = store.Apply(tx, id, 2, RevisionOperationDelete, func() error { return nil })
			return nil, applyErr
		})
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			count, sweepErr := store.SweepExpiredTombstones(tx, fixedNow.Add(time.Second), 10)
			require.Equal(t, 1, count)
			return nil, sweepErr
		})
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			revision, readErr := store.lastAppliedRevision(tx, id)
			if readErr != nil {
				return nil, readErr
			}
			deleted, readErr := store.deleted(tx, id)
			if readErr != nil {
				return nil, readErr
			}
			require.Equal(t, uint64(0), revision)
			require.False(t, deleted)

			begin, end := store.tombstones.FDBRangeKeys()
			kvs, readErr := tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{}).GetSliceWithError()
			if readErr != nil {
				return nil, readErr
			}
			require.Len(t, kvs, 0)
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestRevisionStoreSweepKeepsNewerRevisionMetadata(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		store, err := CreateOrOpenRevisionStore(db)
		require.NoError(t, err)

		fixedNow := time.Unix(1700000000, 0)
		store.now = func() time.Time { return fixedNow }
		store.tombstoneTTL = 0

		id := Id("gc-keep-newer")
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			_, applyErr := store.Apply(tx, id, 1, RevisionOperationCreate, func() error { return nil })
			if applyErr != nil {
				return nil, applyErr
			}
			_, applyErr = store.Apply(tx, id, 2, RevisionOperationDelete, func() error { return nil })
			if applyErr != nil {
				return nil, applyErr
			}
			_, applyErr = store.Apply(tx, id, 3, RevisionOperationMutate, func() error { return nil })
			return nil, applyErr
		})
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			count, sweepErr := store.SweepExpiredTombstones(tx, fixedNow.Add(time.Second), 10)
			require.Equal(t, 1, count)
			return nil, sweepErr
		})
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			revision, readErr := store.lastAppliedRevision(tx, id)
			if readErr != nil {
				return nil, readErr
			}
			deleted, readErr := store.deleted(tx, id)
			if readErr != nil {
				return nil, readErr
			}
			require.Equal(t, uint64(3), revision)
			require.True(t, deleted)
			return nil, nil
		})
		require.NoError(t, err)
	})
}
