package dbutil

import "github.com/apple/foundationdb/bindings/go/src/fdb"

type Future[T any] struct {
	future    fdb.FutureByteSlice
	unmarshal func([]byte) (T, error)
}

func NewFuture[T any](future fdb.FutureByteSlice, unmarshal func([]byte) (T, error)) *Future[T] {
	return &Future[T]{
		future:    future,
		unmarshal: unmarshal,
	}
}

func (f *Future[T]) Get() (v T, err error) {
	bytes, err := f.future.Get()
	if err != nil {
		return
	}
	return f.unmarshal(bytes)
}

func (f *Future[T]) MustGet() T {
	v, err := f.Get()
	if err != nil {
		panic(err)
	}
	return v
}
