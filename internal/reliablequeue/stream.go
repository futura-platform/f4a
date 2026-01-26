package reliablequeue

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliablewatch"
	dbutil "github.com/futura-platform/f4a/pkg/util/db"
)

type StreamEventType int

const (
	StreamEventTypeEnqueued StreamEventType = iota
	StreamEventTypeDequeued
)

type StreamEventBatch struct {
	Type  StreamEventType
	Items [][]byte
}

// epochChunk contains queue changes and walk state for WatchCh.
type epochChunk struct {
	readVersion int64
}

// Stream establishes the necessary things for the consumer to construct the list of queued items, and have it update in realtime.
// It is gauranteed to eventually send every change that happens to the queue, in order (unless there is an error).
// The events channel is a channel of batches of events, each batch is a slice of StreamEvent.
func (q *FIFO) Stream(ctx context.Context) (
	initialValues [][]byte,
	events <-chan StreamEventBatch,
	errCh <-chan error,
	err error,
) {
	var initialEpochWatch fdb.FutureNil
	var initialReadVersion int64
	begin, end := q.subspace.FDBRangeKeys()
	var currentKvs []fdb.KeyValue
	var initialHeadKey fdb.Key
	var initialTailKey fdb.Key
	_, err = q.t.Transact(func(tx fdb.Transaction) (any, error) {
		initialEpochWatch = tx.Watch(q.epochKey)
		initialReadVersion = tx.GetReadVersion().MustGet()
		return nil, nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to start stream watch: %w", err)
	}

	// walk the entire subspace to get the initial values at a pinned read version
	currentKvs, err = q.readQueueRangeAtVersion(ctx, fdb.KeyRange{Begin: begin, End: end}, initialReadVersion)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to construct initial state: %w", err)
	}
	if len(currentKvs) > 0 {
		// if there are initial values, set the head and tail of the queue
		initialHeadKey = currentKvs[0].Key
		initialTailKey = currentKvs[len(currentKvs)-1].Key
	}

	initialValues = make([][]byte, len(currentKvs))
	for i, kv := range currentKvs {
		initialValues[i] = kv.Value
	}

	onEpochCh, onEpochErrCh := reliablewatch.WatchCh(
		ctx,
		q.t,
		q.epochKey,
		epochChunk{readVersion: initialReadVersion},
		initialEpochWatch,
		func(rt fdb.ReadTransaction, _ fdb.KeyConvertible, _ epochChunk) (epochChunk, error) {
			return epochChunk{readVersion: rt.GetReadVersion().MustGet()}, nil
		},
	)

	eventsCh := make(chan StreamEventBatch)
	_errCh := make(chan error)

	// start goroutine to listen for changes to the queue
	go func() {
		defer close(eventsCh)
		defer close(_errCh)
		headKey := initialHeadKey
		tailKey := initialTailKey
		// dont need a loop condition, since the channel is closed when the stream loop exits, which is linked to the context being cancelled.
		for {
			// waiting for the client to consume the events without buffering
			// is an intentional design decision to maximize batching opportunities.
			// Since our algorithm gaurantees eventual consistency, waiting more between events raises the probability of batching more items.
			select {
			case c, ok := <-onEpochCh:
				if !ok {
					return
				}
				newKvs, newTail, err := q.readQueueTailAtVersion(ctx, tailKey, c.readVersion)
				if err != nil {
					_errCh <- err
					return
				}
				tailKey = newTail

				// enqueue all new items
				if len(newKvs) > 0 {
					currentKvs = append(currentKvs, newKvs...)
					eventBatch := StreamEventBatch{Type: StreamEventTypeEnqueued, Items: make([][]byte, len(newKvs))}
					for i, kv := range newKvs {
						eventBatch.Items[i] = kv.Value
					}
					eventsCh <- eventBatch
				}

				newHead, err := q.readQueueHeadAtVersion(ctx, headKey, c.readVersion)
				if err != nil {
					_errCh <- err
					return
				}
				headKey = newHead

				// dequeue all removed items
				currentKvs, err = applyHeadAdvance(eventsCh, currentKvs, newHead)
				if err != nil {
					_errCh <- err
					return
				}

				// error handling, errors are fatal and will cancel the stream
			case err, ok := <-onEpochErrCh:
				if !ok {
					return
				}
				_errCh <- err
				return
			}
		}
	}()

	return initialValues, eventsCh, _errCh, nil
}

func (q *FIFO) readQueueRangeAtVersion(ctx context.Context, r fdb.Range, readVersion int64) ([]fdb.KeyValue, error) {
	opts := dbutil.IterateUnboundedOptions{
		BatchSize:   q.rangeBatchSize(),
		ReadVersion: &readVersion,
		Snapshot:    true,
	}
	values := make([]fdb.KeyValue, 0)
	var iterErr error
	dbutil.IterateUnbounded(ctx, q.t, r, opts)(func(chunk []fdb.KeyValue, err error) bool {
		if err != nil {
			iterErr = err
			return false
		}
		values = append(values, chunk...)
		return true
	})
	if iterErr != nil {
		return nil, fmt.Errorf("failed to iterate queue range: %w", iterErr)
	}
	return values, nil
}

func (q *FIFO) readQueueTailAtVersion(ctx context.Context, tailKey fdb.Key, readVersion int64) ([]fdb.KeyValue, fdb.Key, error) {
	begin, end := q.subspace.FDBRangeKeys()
	start := fdb.KeyConvertible(begin)
	if len(tailKey) > 0 {
		start = dbutil.KeyAfter(tailKey)
	}
	newKvs, err := q.readQueueRangeAtVersion(ctx, fdb.KeyRange{Begin: start, End: end}, readVersion)
	if err != nil {
		return nil, nil, err
	}
	newTail := tailKey
	if len(newKvs) > 0 {
		newTail = newKvs[len(newKvs)-1].Key
	}
	return newKvs, newTail, nil
}

func (q *FIFO) readQueueHeadAtVersion(ctx context.Context, headKey fdb.Key, readVersion int64) (fdb.Key, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	begin, end := q.subspace.FDBRangeKeys()
	start := fdb.KeyConvertible(begin)
	if len(headKey) > 0 {
		start = headKey
	}
	var newHead fdb.Key
	_, err := q.t.Transact(func(tx fdb.Transaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tx.SetReadVersion(readVersion)
		readTx := tx.Snapshot()
		headKvs, err := readTx.GetRange(
			fdb.KeyRange{Begin: start, End: end},
			fdb.RangeOptions{Limit: 1},
		).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(headKvs) > 0 {
			newHead = headKvs[0].Key
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return newHead, nil
}

// applyHeadAdvance removes all items strictly before headKey.
// If headKey is empty, it dequeues everything.
func applyHeadAdvance(eventsCh chan<- StreamEventBatch, currentKvs []fdb.KeyValue, headKey fdb.Key) ([]fdb.KeyValue, error) {
	// if the head key is empty, this signals that the queue is empty
	if len(headKey) == 0 {
		eventBatch := StreamEventBatch{Type: StreamEventTypeDequeued, Items: make([][]byte, len(currentKvs))}
		for i, kv := range currentKvs {
			eventBatch.Items[i] = kv.Value
		}
		eventsCh <- eventBatch
		return nil, nil
	}

	// otherwise, dequeue all items up to (but not including) the new head key
	headIdx := -1
	for i, kv := range currentKvs {
		if bytes.Equal(kv.Key, headKey) {
			headIdx = i
			break
		}
	}
	if headIdx == -1 {
		return currentKvs, fmt.Errorf("stream desync: head key not found")
	}

	eventBatch := StreamEventBatch{Type: StreamEventTypeDequeued, Items: make([][]byte, len(currentKvs[:headIdx]))}
	for i, kv := range currentKvs[:headIdx] {
		eventBatch.Items[i] = kv.Value
	}
	eventsCh <- eventBatch
	return currentKvs[headIdx:], nil
}
