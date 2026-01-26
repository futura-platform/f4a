package reliableset

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliablewatch"
)

type epochChunk struct {
	entries []LogEntry
	tailKey fdb.KeyConvertible
}

// Stream establishes the necessary things for the consumer to construct the list of queued items, and have it update in realtime.
// It is gauranteed to eventually send every change that happens to the queue, in order (unless there is an error).
// The events channel is a channel of batches of events, each batch is a slice of StreamEvent.
func (s *Set) Stream(ctx context.Context) (
	initialValues mapset.Set[string],
	events <-chan []LogEntry,
	errCh <-chan error,
	err error,
) {
	var initialEpochWatch fdb.FutureNil
	var initialTail fdb.KeyConvertible
	_, err = s.db.Transact(func(tx fdb.Transaction) (any, error) {
		initialEpochWatch = tx.Watch(s.epochKey)
		initialValues, initialTail, err = s.Items(tx)
		if err != nil {
			return nil, err
		}
		s.registerCursor(tx, initialTail)
		return nil, nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	eventsCh := make(chan []LogEntry)
	_errCh := make(chan error)
	onEpochCh, onEpochErrCh := reliablewatch.WatchCh(ctx, s.db, s.epochKey, epochChunk{tailKey: initialTail}, initialEpochWatch,
		func(tx fdb.ReadTransaction, _ fdb.KeyConvertible, l epochChunk) (epochChunk, error) {
			logEntries, err := s.readLog(tx, l.tailKey)
			if err != nil {
				return epochChunk{}, err
			}
			tailKey := l.tailKey
			entries := make([]LogEntry, len(logEntries))
			for i, logEntry := range logEntries {
				tailKey = logEntry.key
				entries[i] = logEntry.entry
			}
			return epochChunk{tailKey: tailKey, entries: entries}, nil
		},
	)
	go func() {
		defer close(eventsCh)
		defer close(_errCh)
		go s.leaseLoop(ctx)
		for {
			select {
			case c, ok := <-onEpochCh:
				if !ok {
					return
				}
				if len(c.entries) == 0 {
					continue
				}
				eventsCh <- c.entries
				if err := s.advanceCursor(ctx, c.tailKey); err != nil {
					_errCh <- err
					return
				}
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
