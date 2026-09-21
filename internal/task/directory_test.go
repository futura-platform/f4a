package task

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestTasksDirectory(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		tasks, err := CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)
		id := NewId()
		// an id that extends this one: its keys sort right after, and must survive the clear
		neighbour := Id(string(id) + "x")

		_, err = tasks.Open(db, id)
		require.ErrorIs(t, err, ErrNotFound)

		tkey, err := tasks.Create(db, id)
		require.NoError(t, err)
		_, err = tasks.Create(db, id)
		require.ErrorIs(t, err, ErrAlreadyExists)
		opened, err := tasks.Open(db, id)
		require.NoError(t, err)
		require.Equal(t, id, opened.Id())
		_, err = tasks.Create(db, neighbour)
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			tkey.Input().Set(tx, []byte("input"))
			tx.Set(tkey.MemoTable("user").Pack(tuple.Tuple{"moment"}), []byte("m"))
			tx.Set(tkey.DurableObjectSpace("delivery").Pack(tuple.Tuple{"k"}), []byte("v"))
			tx.Set(tkey.keyspace().Sub("runnable_lock").Pack(tuple.Tuple{"holder", "identity"}), []byte("h"))
			return nil, nil
		})
		require.NoError(t, err)

		// everything the task owns goes with it
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) { return nil, tkey.Clear(tx) })
		require.NoError(t, err)
		_, err = tasks.Open(db, id)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			kvs, err := tx.GetRange(tkey.keyspace(), fdb.RangeOptions{}).GetSliceWithError()
			require.NoError(t, err)
			require.Empty(t, kvs)
			return nil, nil
		})
		require.NoError(t, err)
		_, err = tasks.Open(db, neighbour)
		require.NoError(t, err)
	})
}

func TestTaskKeyPrecondition(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		tasks, err := CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)
		tkey, err := tasks.Create(db, NewId())
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) { return nil, tkey.MustExist(tx) })
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) { return nil, tkey.Clear(tx) })
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			if err := tkey.MustExist(tx); err != nil {
				return nil, err
			}
			tkey.Input().Set(tx, []byte("late"))
			return nil, nil
		})
		require.ErrorIs(t, err, ErrNotFound)
		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			kvs, err := tx.GetRange(tkey.keyspace(), fdb.RangeOptions{}).GetSliceWithError()
			require.NoError(t, err)
			require.Empty(t, kvs, "a guarded write must not land under a deleted task")
			return nil, nil
		})
		require.NoError(t, err)
	})
}
