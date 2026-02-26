package dbutil_test

import (
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

type testStringSerializer struct {
	unmarshalErr error
}

func (s testStringSerializer) Marshal(v string) []byte {
	if v == "" {
		return nil
	}
	return []byte(v)
}

func (s testStringSerializer) Unmarshal(bytes []byte) (string, error) {
	if s.unmarshalErr != nil {
		return "", s.unmarshalErr
	}
	return string(bytes), nil
}

func TestTypedKey(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		key := fdb.Key("typed-key")

		t.Run("set and get round trip", func(t *testing.T) {
			typedKey := dbutil.NewTypedKey[string](key, testStringSerializer{})

			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				typedKey.Set(tx, "value")
				return nil, nil
			})
			require.NoError(t, err)

			var got string
			_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				var getErr error
				got, getErr = typedKey.Get(tx).Get()
				return nil, getErr
			})
			require.NoError(t, err)
			require.Equal(t, "value", got)
		})

		t.Run("clears when marshaled bytes are nil", func(t *testing.T) {
			typedKey := dbutil.NewTypedKey[string](key, testStringSerializer{})

			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				typedKey.Set(tx, "to-be-cleared")
				return nil, nil
			})
			require.NoError(t, err)

			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				typedKey.Set(tx, "")
				return nil, nil
			})
			require.NoError(t, err)

			var raw []byte
			_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				var getErr error
				raw, getErr = tx.Get(key).Get()
				return nil, getErr
			})
			require.NoError(t, err)
			require.Nil(t, raw)
		})

		t.Run("returns unmarshal errors", func(t *testing.T) {
			expectedErr := errors.New("unmarshal failed")
			typedKey := dbutil.NewTypedKey[string](key, testStringSerializer{
				unmarshalErr: expectedErr,
			})

			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(key, []byte("value"))
				return nil, nil
			})
			require.NoError(t, err)

			_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				_, getErr := typedKey.Get(tx).Get()
				return nil, getErr
			})
			require.ErrorIs(t, err, expectedErr)
		})

		t.Run("returns the configured key", func(t *testing.T) {
			typedKey := dbutil.NewTypedKey[string](key, testStringSerializer{})
			require.Equal(t, key, typedKey.Key().FDBKey())
		})
	})
}
