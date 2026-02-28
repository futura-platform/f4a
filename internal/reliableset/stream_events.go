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

// StreamEvents establishes the necessary things for the consumer to construct
// the list of queued items, and have it update in realtime.
// It is gauranteed to eventually send every change that happens to the queue,
// in order (unless there is an error).
// The events channel sends batches of changes.
// These changes are directly forwarded from the log, so they are NOT deduplicated/absolute.
func (s *Set) StreamEvents(ctx context.Context) (
	initialValues mapset.Set[string],
	events <-chan []LogEntry,
	errCh <-chan error,
	err error,
) {
	streamCtx, streamCancel := context.WithCancel(ctx)
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
	_errCh := make(chan error, 1)
	onEpochCh, onEpochErrCh := reliablewatch.WatchCh(streamCtx, s.db, s.epochKey, epochChunk{tailKey: initialTail}, initialEpochWatch,
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
		defer streamCancel()
		defer close(eventsCh)
		defer close(_errCh)
		go s.leaseLoop(streamCtx)
		for onEpochCh != nil || onEpochErrCh != nil {
			select {
			case c, ok := <-onEpochCh:
				if !ok {
					onEpochCh = nil
					continue
				}
				if len(c.entries) == 0 {
					continue
				}
				if err := sendStreamBatch(streamCtx, eventsCh, c.entries); err != nil {
					sendStreamErr(_errCh, err)
					return
				}
				if err := s.advanceCursor(streamCtx, c.tailKey); err != nil {
					sendStreamErr(_errCh, err)
					return
				}
			case err, ok := <-onEpochErrCh:
				if !ok {
					onEpochErrCh = nil
					continue
				}
				sendStreamErr(_errCh, err)
				return
			}
		}
	}()
	return initialValues, eventsCh, _errCh, nil
}

func sendStreamBatch(ctx context.Context, ch chan<- []LogEntry, batch []LogEntry) error {
	select {
	case ch <- batch:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func sendStreamErr(errCh chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case errCh <- err:
	default:
	}
}
