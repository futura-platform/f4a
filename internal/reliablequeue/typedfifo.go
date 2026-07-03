package reliablequeue

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func CreateOrOpenTFIFO[T any](t fdb.Transactor, path []string, parser dbutil.Serializer[T]) (TFIFO[T], error) {
	q, err := createOrOpenFIFO(t, path)
	if err != nil {
		return TFIFO[T]{}, err
	}
	return makeTFIFO(q, parser), nil
}

func makeTFIFO[T any](q *fifo, parser dbutil.Serializer[T]) TFIFO[T] {
	return TFIFO[T]{
		parser: parser,
		fifo:   q,
	}
}

type TFIFO[T any] struct {
	parser dbutil.Serializer[T]
	fifo   *fifo
}

type TStreamEventBatch[T any] struct {
	Type  StreamEventType
	Items []T
}

func (q TFIFO[T]) Enqueue(tx fdb.Transaction, item T) error {
	return q.fifo.Enqueue(tx, q.parser.Marshal(item))
}

func (q TFIFO[T]) Dequeue(tx fdb.Transaction) (T, error) {
	raw, err := q.fifo.Dequeue(tx)
	if err != nil {
		var zero T
		return zero, err
	}
	return q.parser.Unmarshal(raw)
}

func (q TFIFO[T]) Stream(ctx context.Context, initialReadBatchSize int) (
	initialValues []T,
	events <-chan TStreamEventBatch[T],
	errCh <-chan error,
	err error,
) {
	streamCtx, streamCancel := context.WithCancel(ctx)
	rawInitial, rawEvents, rawErrCh, err := q.fifo.Stream(streamCtx, initialReadBatchSize)
	if err != nil {
		streamCancel()
		return nil, nil, nil, err
	}

	initialValues, err = q.convertItems(rawInitial)
	if err != nil {
		streamCancel()
		return nil, nil, nil, err
	}

	eventsCh := make(chan TStreamEventBatch[T])
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
				items, err := q.convertItems(rawEventBatch.Items)
				if err != nil {
					sendStreamErr(wrappedErrCh, err)
					return
				}
				select {
				case eventsCh <- TStreamEventBatch[T]{Type: rawEventBatch.Type, Items: items}:
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
	return initialValues, eventsCh, wrappedErrCh, nil
}

func (q TFIFO[T]) convertItems(rawItems [][]byte) ([]T, error) {
	items := make([]T, len(rawItems))
	for i, raw := range rawItems {
		item, err := q.parser.Unmarshal(raw)
		if err != nil {
			return nil, err
		}
		items[i] = item
	}
	return items, nil
}
