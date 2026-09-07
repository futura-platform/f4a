package reliableset

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func MakeTSet[T comparable](set *set, parser dbutil.Serializer[T]) TSet[T] {
	return TSet[T]{
		parser: parser,
		set:    set,
	}
}

type TSet[T comparable] struct {
	parser dbutil.Serializer[T]
	set    *set
}
type TLogEntry[T comparable] struct {
	Op    LogOperation
	Value T
}

func (s TSet[T]) Add(tx fdb.Transaction, value T) error {
	return s.set.Add(tx, s.parser.Marshal(value))
}
func (s TSet[T]) Clear(tx fdb.Transaction) error {
	return s.set.Clear(tx)
}
func (s TSet[T]) Items(ctx context.Context, db fdb.Database) (items mapset.Set[T], tail fdb.KeyConvertible, err error) {
	rawItems, tail, err := s.set.Items(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	items, err = convertSet(rawItems, s.parser)
	return items, tail, err
}
func (s TSet[T]) Remove(tx fdb.Transaction, value T) error {
	return s.set.Remove(tx, s.parser.Marshal(value))
}
func (s TSet[T]) Cardinality(t fdb.ReadTransaction) (int64, error) {
	return s.set.Cardinality(t)
}
func (s TSet[T]) RunCompactor() (cancel func()) {
	return s.set.RunCompactor()
}
func (s TSet[T]) Stream(ctx context.Context) (*Stream[T], error) {
	return streamWith(ctx, s.set, s.parser)
}
