package reliableset

import (
	"bytes"
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliablewatch"
)

type epochChunk struct {
	readVersion int64
	// previousTail is the last log key we have processed (exclusive start for next read).
	previousTail fdb.Key
	// currentTail is the log tail observed at readVersion (used to detect if work is needed).
	currentTail fdb.Key
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
	var initialReadVersion int64
	var initialTail fdb.Key
	_, err = s.t.Transact(func(tx fdb.Transaction) (any, error) {
		initialEpochWatch = tx.Watch(s.epochKey)
		initialReadVersion = tx.GetReadVersion().MustGet()
		var tailErr error
		initialTail, tailErr = s.logTail(tx)
		if tailErr != nil {
			return nil, tailErr
		}
		s.registerCursor(tx, initialTail)
		return nil, nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	initialValues, _, err = s.itemsAtReadVersion(ctx, initialReadVersion)
	if err != nil {
		return nil, nil, nil, err
	}

	eventsCh := make(chan []LogEntry)
	_errCh := make(chan error)
	onEpochCh, onEpochErrCh := reliablewatch.WatchCh(ctx, s.t, s.epochKey, epochChunk{currentTail: initialTail}, initialEpochWatch,
		func(tx fdb.ReadTransaction, _ fdb.KeyConvertible, l epochChunk) (epochChunk, error) {
			readVersion := tx.GetReadVersion().MustGet()
			tailKey, err := s.logTail(tx)
			if err != nil {
				return epochChunk{}, err
			}
			return epochChunk{
				previousTail: l.currentTail,
				currentTail:  tailKey,
				readVersion:  readVersion,
			}, nil
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
				} else if bytes.Equal(c.previousTail, c.currentTail) {
					panic("epoch changed without advancing log tail")
				}
				logEntries, err := s.readLog(ctx, c.previousTail, &c.readVersion)
				if err != nil {
					_errCh <- err
					return
				}
				if len(logEntries) == 0 {
					continue
				}
				tailKey := c.previousTail
				entries := make([]LogEntry, len(logEntries))
				for i, logEntry := range logEntries {
					tailKey = logEntry.key.FDBKey()
					entries[i] = logEntry.entry
				}
				eventsCh <- entries
				if err := s.advanceCursor(ctx, tailKey); err != nil {
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
