package reliableset

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestLogCursorMaintainsIndependentState(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "log_cursor_independent")
		first := newLogCursor(set)
		second := newLogCursor(set)
		require.NotEqual(t, first.id, second.id)

		begin, _ := set.logSubspace.FDBRangeKeys()
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			first.register(tx, begin)
			second.register(tx, begin)
			return nil, nil
		})
		require.NoError(t, err)

		addItem(t, db, set, []byte("next"))
		lastKey := readLastLogKey(t, db, set)

		require.NoError(t, first.advance(context.Background(), lastKey))

		firstTail, firstLease := readCursor(t, db, set, first.id)
		secondTail, secondLease := readCursor(t, db, set, second.id)
		require.Equal(t, lastKey, firstTail)
		require.Equal(t, begin.FDBKey(), secondTail)
		require.True(t, firstLease.After(time.Now()))
		require.True(t, secondLease.After(time.Now()))
	})
}

func TestLogCursorClearRemovesOnlyOwnKeys(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "log_cursor_clear")
		first := newLogCursor(set)
		second := newLogCursor(set)

		begin, _ := set.logSubspace.FDBRangeKeys()
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			first.register(tx, begin)
			second.register(tx, begin)
			return nil, nil
		})
		require.NoError(t, err)

		require.NoError(t, first.clear(context.Background()))

		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			require.Nil(t, tx.Get(first.key(cursorKeyTail)).MustGet())
			require.Nil(t, tx.Get(first.key(cursorKeyLease)).MustGet())
			require.Nil(t, tx.Get(first.key(cursorKeyHint)).MustGet())

			require.NotNil(t, tx.Get(second.key(cursorKeyTail)).MustGet())
			require.NotNil(t, tx.Get(second.key(cursorKeyLease)).MustGet())
			require.NotNil(t, tx.Get(second.key(cursorKeyHint)).MustGet())
			return nil, nil
		})
		require.NoError(t, err)
	})
}
