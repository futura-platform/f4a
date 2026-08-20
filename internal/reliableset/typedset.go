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

	items, err = s.convertToTypedSet(rawItems)
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
func (s TSet[T]) Stream(ctx context.Context) (initialValues mapset.Set[T], events <-chan []TLogEntry[T], errCh <-chan error, err error) {
	streamCtx, streamCancel := context.WithCancel(ctx)
	ivs, rawEvents, rawErrCh, err := s.set.Stream(streamCtx)
	if err != nil {
		streamCancel()
		return nil, nil, nil, err
	}

	items, err := s.convertToTypedSet(ivs)
	if err != nil {
		streamCancel()
		return nil, nil, nil, err
	}

	eventsCh := make(chan []TLogEntry[T])
	wrappedErrCh := make(chan error, 1)
	go func() {
		defer streamCancel()
		defer close(eventsCh)
		defer close(wrappedErrCh)

		for rawEvents != nil || rawErrCh != nil {
			select {
			case rawEventBatch, ok := <-rawEvents:
				if !ok {
					rawEvents = nil
					continue
				}
				eventBatch := make([]TLogEntry[T], len(rawEventBatch))
				for i, rawEvent := range rawEventBatch {
					value, err := s.parser.Unmarshal(rawEvent.Value)
					if err != nil {
						sendStreamErr(wrappedErrCh, err)
						return
					}
					eventBatch[i] = TLogEntry[T]{Op: rawEvent.Op, Value: value}
				}
				select {
				case eventsCh <- eventBatch:
				case <-streamCtx.Done():
					sendStreamErr(wrappedErrCh, context.Cause(streamCtx))
					return
				}
			case err, ok := <-rawErrCh:
				if !ok {
					rawErrCh = nil
					continue
				}
				sendStreamErr(wrappedErrCh, err)
				return
			}
		}
	}()
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
