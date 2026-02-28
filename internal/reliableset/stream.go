package reliableset

import (
	"context"
	"fmt"

	mapset "github.com/deckarep/golang-set/v2"
)

// Stream establishes the necessary things for the consumer to construct the
// list of queued items and have it update in realtime.
//
// Unlike StreamEvents, the emitted batches only include the absolute net state
// changes per incoming raw batch.
func (s *Set) Stream(ctx context.Context) (
	initialValues mapset.Set[string],
	events <-chan []LogEntry,
	errCh <-chan error,
	err error,
) {
	initialValues, rawEventsCh, rawErrCh, err := s.StreamEvents(ctx)
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
				sendStreamErr(_errCh, err)
				return
			}
		}
	}()
	return initialValues, eventsCh, _errCh, nil
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
