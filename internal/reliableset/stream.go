package reliableset

import (
	"context"
	"errors"
	"fmt"

	mapset "github.com/deckarep/golang-set/v2"
)

// Stream establishes the necessary things for the consumer to construct the
// list of queued items and have it update in realtime.
//
// Unlike streamEvents, the emitted batches only include the absolute net state
// changes per incoming raw batch.
func (s *set) Stream(ctx context.Context) (
	initialValues mapset.Set[string],
	events <-chan []LogEntry,
	errCh <-chan error,
	err error,
) {
	initialValues, rawEventsCh, rawErrCh, err := s.streamEvents(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	eventsCh := make(chan []LogEntry)
	_errCh := make(chan error, 1)

	go func() {
		defer close(eventsCh)
		defer close(_errCh)

		currentState := initialValues.Clone()
		for rawEventsCh != nil || rawErrCh != nil {
			select {
			case batch, ok := <-rawEventsCh:
				if !ok {
					rawEventsCh = nil
					continue
				}

				absoluteBatch, err := resolveAbsoluteBatch(currentState, batch)
				if err != nil {
					sendStreamErr(_errCh, err)
					return
				} else if len(absoluteBatch) == 0 {
					continue
				}
				if err := sendStreamBatch(ctx, eventsCh, absoluteBatch); err != nil {
					sendStreamErr(_errCh, err)
					return
				}
			case err, ok := <-rawErrCh:
				if !ok {
					rawErrCh = nil
					continue
				}
				if errors.Is(err, errCursorEvicted) {
					currentState, rawEventsCh, rawErrCh, err = s.resyncStream(ctx, currentState, eventsCh)
					if err == nil {
						continue
					}
				}
				sendStreamErr(_errCh, err)
				return
			}
		}
	}()
	return initialValues, eventsCh, _errCh, nil
}

// resyncStream re-establishes the raw event stream after the compactor evicted
// this stream's cursor for lagging too far (see evictCursors). It emits the net
// difference between the consumer's state and the fresh snapshot, so consumers
// absorb the gap as ordinary absolute changes. It mirrors streamEvents' return
// shape: the fresh state and the new raw channels.
func (s *set) resyncStream(
	ctx context.Context,
	currentState mapset.Set[string],
	eventsCh chan<- []LogEntry,
) (mapset.Set[string], <-chan []LogEntry, <-chan error, error) {
	newInitial, rawEventsCh, rawErrCh, err := s.streamEvents(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if reconciliation := diffStates(currentState, newInitial); len(reconciliation) > 0 {
		if err := sendStreamBatch(ctx, eventsCh, reconciliation); err != nil {
			return nil, nil, nil, err
		}
	}
	return newInitial.Clone(), rawEventsCh, rawErrCh, nil
}

// diffStates returns the absolute operations that transform `from` into `to`.
func diffStates(from, to mapset.Set[string]) []LogEntry {
	entries := make([]LogEntry, 0)
	from.Each(func(item string) bool {
		if !to.ContainsOne(item) {
			entries = append(entries, LogEntry{Op: LogOperationRemove, Value: []byte(item)})
		}
		return false
	})
	to.Each(func(item string) bool {
		if !from.ContainsOne(item) {
			entries = append(entries, LogEntry{Op: LogOperationAdd, Value: []byte(item)})
		}
		return false
	})
	return entries
}

// resolveAbsoluteBatch resolves the absolute batch of changes from the relative batch + the current state.
// Redundant changes are collapsed. This has a runtime complexity of O(2b) where b is the number of items in the batch.
func resolveAbsoluteBatch(currentState mapset.Set[string], batch []LogEntry) ([]LogEntry, error) {
	touchedOrder := make([]string, 0, len(batch))
	beforeMembership := make(map[string]bool, len(batch))

	for _, entry := range batch {
		item := string(entry.Value)
		if _, seen := beforeMembership[item]; !seen {
			beforeMembership[item] = currentState.ContainsOne(item)
			touchedOrder = append(touchedOrder, item)
		}

		switch entry.Op {
		case LogOperationAdd:
			currentState.Add(item)
		case LogOperationRemove:
			currentState.Remove(item)
		default:
			return nil, fmt.Errorf("unknown stream operation: %d", entry.Op)
		}
	}

	absolute := make([]LogEntry, 0, len(touchedOrder))
	for _, item := range touchedOrder {
		afterMembership := currentState.ContainsOne(item)
		if beforeMembership[item] == afterMembership {
			// no change case
			continue
		}
		relevantValue := []byte(item)
		op := LogOperationAdd
		if !afterMembership {
			op = LogOperationRemove
		}
		absolute = append(absolute, LogEntry{Op: op, Value: relevantValue})
	}
	return absolute, nil
}
