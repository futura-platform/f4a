package reliableset

import (
	"encoding/binary"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestReadCardinalityResolvesCurrentMetadata(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		path := []string{"counted"}
		read := func() (int64, error) {
			value, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				return ReadCardinality(tx, db, path)
			})
			if err != nil {
				return 0, err
			}
			return value.(int64), nil
		}
		_, err := read()
		require.ErrorIs(t, err, directory.ErrDirNotExists)

		metadata, err := db.Root.Create(db, append(path, metadataDirectory), nil)
		require.NoError(t, err)
		value, err := read()
		require.NoError(t, err)
		require.Zero(t, value)

		key := metadata.Pack(tuple.Tuple{cardinalityField})
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			tx.Set(key, binary.LittleEndian.AppendUint64(nil, 7))
			return nil, nil
		})
		require.NoError(t, err)
		value, err = read()
		require.NoError(t, err)
		require.Equal(t, int64(7), value)
		_, err = Open(db, db, path)
		require.Error(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			tx.Set(key, []byte{1})
			return nil, nil
		})
		require.NoError(t, err)
		_, err = read()
		require.ErrorContains(t, err, "invalid cardinality value length")

		_, err = db.Root.Remove(db, path)
		require.NoError(t, err)
		_, err = read()
		require.ErrorIs(t, err, directory.ErrDirNotExists)
		replacement, err := db.Root.Create(db, append(path, metadataDirectory), nil)
		require.NoError(t, err)
		require.NotEqual(t, metadata.Bytes(), replacement.Bytes())
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			tx.Set(replacement.Pack(tuple.Tuple{cardinalityField}), binary.LittleEndian.AppendUint64(nil, 11))
			return nil, nil
		})
		require.NoError(t, err)
		value, err = read()
		require.NoError(t, err)
		require.Equal(t, int64(11), value)
	})
}
