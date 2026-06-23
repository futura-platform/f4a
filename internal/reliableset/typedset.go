package reliableset

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func MakeTSet[T comparable](set *Set, parser dbutil.Serializer[T]) TSet[T] {
	return TSet[T]{
		parser: parser,
		set:    set,
	}
}

type TSet[T comparable] struct {
	parser dbutil.Serializer[T]
	set    *Set
}
type TLogEntry[T comparable] struct {
	Op    LogOperation
	Value T
}

func (s TSet[T]) Add(tx fdb.Transaction, value T) error {
	return s.set.Add(tx, s.parser.Marshal(value))
}
func (s TSet[T]) Clear() error {
	return s.set.Clear()
}
func (s TSet[T]) Items(ctx context.Context, db fdb.Database) (items mapset.Set[T], tail fdb.KeyConvertible, err error) {
	rawItems, tail, err := s.set.Items(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	items, err = s.convertToTypedSet(rawItems)
	return items, tail, err
}
func (s TSet[T]) Remove(tx fdb.Transaction, value T) error {
	return s.set.Remove(tx, s.parser.Marshal(value))
}
func (s TSet[T]) RunCompactor() (cancel func()) {
	return s.set.RunCompactor()
}
func (s TSet[T]) Stream(ctx context.Context) (initialValues mapset.Set[T], events <-chan []TLogEntry[T], errCh <-chan error, err error) {
	ivs, rawEvents, errCh, err := s.set.Stream(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	eventsCh := make(chan []TLogEntry[T])
	wrappedErrCh := make(chan error, 1)
	go func() {
		defer close(eventsCh)
		for rawEventBatch := range rawEvents {
			eventBatch := make([]TLogEntry[T], len(rawEventBatch))
			for i, rawEvent := range rawEventBatch {
				value, err := s.parser.Unmarshal(rawEvent.Value)
				if err != nil {
					wrappedErrCh <- err
					return
				}
				eventBatch[i] = TLogEntry[T]{Op: rawEvent.Op, Value: value}
			}
			eventsCh <- eventBatch
		}
	}()
	go func() {
		defer close(wrappedErrCh)
		for err := range errCh {
			wrappedErrCh <- err
		}
	}()
	items, err := s.convertToTypedSet(ivs)
	if err != nil {
		return nil, nil, nil, err
	}
	return items, eventsCh, wrappedErrCh, nil
}

func (s TSet[T]) convertToTypedSet(ivs mapset.Set[string]) (mapset.Set[T], error) {
	items := mapset.NewSetWithSize[T](ivs.Cardinality())
	for item := range ivs.Iter() {
		value, err := s.parser.Unmarshal([]byte(item))
		if err != nil {
			return nil, err
		}
		items.Add(value)
	}
	return items, nil
}
