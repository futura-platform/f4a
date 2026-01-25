package dbutil

import "github.com/apple/foundationdb/bindings/go/src/fdb"

type TypedKey[T any] struct {
	serializable[T]

	key fdb.KeyConvertible
}

func NewTypedKey[T any](key fdb.KeyConvertible, serializable serializable[T]) TypedKey[T] {
	return TypedKey[T]{
		serializable: serializable,
		key:          key,
	}
}

func (k TypedKey[T]) Get(tx fdb.ReadTransaction) *Future[T] {
	return NewFuture(tx.Get(k.key), k.serializable.Unmarshal)
}

func (k TypedKey[T]) Set(tx fdb.Transaction, v T) {
	tx.Set(k.key, k.serializable.Marshal(v))
}

func (k TypedKey[T]) Key() fdb.KeyConvertible {
	return k.key
}
