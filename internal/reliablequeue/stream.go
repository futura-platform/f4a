package reliablequeue

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliablewatch"
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
	enqueued []fdb.KeyValue
	headKey  fdb.Key
	tailKey  fdb.Key
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
	begin, end := q.subspace.FDBRangeKeys()
	var currentKvs []fdb.KeyValue
	var initialHeadKey fdb.Key
	var initialTailKey fdb.Key
	_, err = q.t.Transact(func(tx fdb.Transaction) (any, error) {
		initialEpochWatch = tx.Watch(q.epochKey)
		// walk the entire subspace to get the initial values
		currentKvs, err = tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(currentKvs) > 0 {
			// if there are initial values, set the head and tail of the queue
			initialHeadKey = currentKvs[0].Key
			initialTailKey = currentKvs[len(currentKvs)-1].Key
		}
		return nil, nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to construct initial state: %w", err)
	}

	initialValues = make([][]byte, len(currentKvs))
	for i, kv := range currentKvs {
		initialValues[i] = kv.Value
	}

	onEpochCh, onEpochErrCh := reliablewatch.WatchCh(
		ctx,
		q.t,
		q.epochKey,
		epochChunk{headKey: initialHeadKey, tailKey: initialTailKey},
		initialEpochWatch,
		func(rt fdb.ReadTransaction, _ fdb.KeyConvertible, last epochChunk) (epochChunk, error) {
			// walk the tail to get the new items
			var tailStart fdb.KeyConvertible = begin
			if len(last.tailKey) > 0 {
				tailStart = last.tailKey
			}
			newKvs, err := rt.GetRange(fdb.KeyRange{Begin: tailStart, End: end}, fdb.RangeOptions{}).GetSliceWithError()
			if err != nil {
				return epochChunk{}, err
			}
			if len(newKvs) > 0 && len(last.tailKey) > 0 && bytes.Equal(newKvs[0].Key, last.tailKey) {
				newKvs = newKvs[1:]
			}

			newTail := last.tailKey
			if len(newKvs) > 0 {
				newTail = newKvs[len(newKvs)-1].Key
			}

			// walk the head to get the removed items
			var headStart fdb.KeyConvertible = begin
			if len(last.headKey) > 0 {
				headStart = last.headKey
			}
			headKvs, err := rt.GetRange(
				// start at the current head key as a heuristic to minimize the number of items to fetch
				fdb.KeyRange{Begin: headStart, End: end},
				fdb.RangeOptions{Limit: 1},
			).GetSliceWithError()
			if err != nil {
				return epochChunk{}, err
			}
			var newHead fdb.Key
			if len(headKvs) > 0 {
				newHead = headKvs[0].Key
			}

			return epochChunk{
				enqueued: newKvs,
				headKey:  newHead,
				tailKey:  newTail,
			}, nil
		},
	)

	eventsCh := make(chan StreamEventBatch)
	_errCh := make(chan error)

	// start goroutine to listen for changes to the queue
	go func() {
		defer close(eventsCh)
		defer close(_errCh)
		// dont need a loop condition, since the channel is closed when the stream loop exits, which is linked to the context being cancelled.
		for {
			// waiting for the client to consume the events without buffering
			// is an intentional design decision to maximize batching opportunities.
			// Since our algorithm gaurantees eventual consistency, waiting more between events raises the probability of batching more items.
			select {
			case c := <-onEpochCh:
				// enqueue all new items
				if len(c.enqueued) > 0 {
					currentKvs = append(currentKvs, c.enqueued...)
					eventBatch := StreamEventBatch{Type: StreamEventTypeEnqueued, Items: make([][]byte, len(c.enqueued))}
					for i, kv := range c.enqueued {
						eventBatch.Items[i] = kv.Value
					}
					eventsCh <- eventBatch
				}

				// dequeue all removed items
				var err error
				currentKvs, err = applyHeadAdvance(eventsCh, currentKvs, c.headKey)
				if err != nil {
					_errCh <- err
					return
				}

				// error handling, errors are fatal and will cancel the stream
			case err := <-onEpochErrCh:
				_errCh <- err
				return
			}
		}
	}()

	return initialValues, eventsCh, _errCh, nil
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
