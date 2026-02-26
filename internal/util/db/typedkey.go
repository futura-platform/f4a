package dbutil

import "github.com/apple/foundationdb/bindings/go/src/fdb"

type TypedKey[T any] struct {
	Serializer[T]

	key fdb.KeyConvertible
}

func NewTypedKey[T any](key fdb.KeyConvertible, serializable Serializer[T]) TypedKey[T] {
	return TypedKey[T]{
		Serializer: serializable,
		key:        key,
	}
}

func (k TypedKey[T]) Get(tx fdb.ReadTransaction) *Future[T] {
	return NewFuture(tx.Get(k.key), k.Serializer.Unmarshal)
}

func (k TypedKey[T]) Set(tx fdb.Transaction, v T) {
	bytes := k.Serializer.Marshal(v)
	if bytes == nil {
		tx.Clear(k.key)
	} else {
		tx.Set(k.key, bytes)
	}
}

func (k TypedKey[T]) Key() fdb.KeyConvertible {
	return k.key
}
