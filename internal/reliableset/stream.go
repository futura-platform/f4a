package reliableset

import (
	"context"
	"errors"
	"fmt"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// Stream is a live view of a set: its current membership (Snapshot) and the
// raw log of changes that produced it (Events). A batch is folded into the
// membership before it is sent, and Events is unbuffered.
type Stream[T comparable] struct {
	mu      sync.Mutex
	members mapset.Set[T]

	events chan []TLogEntry[T]
	errCh  chan error
}

// Snapshot returns a copy of the current membership.
func (s *Stream[T]) Snapshot() mapset.Set[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.members.Clone()
}

// Events yields the log as written, in order, and closes when the stream ends.
func (s *Stream[T]) Events() <-chan []TLogEntry[T] {
	return s.events
}

// Err yields why the stream ended, if it ended with an error, and closes after Events.
func (s *Stream[T]) Err() <-chan error {
	return s.errCh
}

func (s *Stream[T]) fold(batch []TLogEntry[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return foldInto(s.members, batch)
}

// reconcile replaces the membership and returns the entries that get there from the old one.
func (s *Stream[T]) reconcile(fresh mapset.Set[T]) []TLogEntry[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := diffStates(s.members, fresh)
	s.members = fresh
	return entries
}

func (s *set) Stream(ctx context.Context) (*Stream[string], error) {
	return streamWith(ctx, s, rawSerializer{})
}

func streamWith[T comparable](ctx context.Context, s *set, parser dbutil.Serializer[T]) (stream *Stream[T], err error) {
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			streamCancel()
		}
	}()

	initialValues, rawEventsCh, rawErrCh, err := s.streamEvents(streamCtx)
	if err != nil {
		return nil, err
	}
	members, err := convertSet(initialValues, parser)
	if err != nil {
		return nil, err
	}

	stream = &Stream[T]{
		members: members,
		events:  make(chan []TLogEntry[T]),
		errCh:   make(chan error, 1),
	}

	go func() {
		defer streamCancel()
		defer close(stream.errCh)
		defer close(stream.events)

		for rawEventsCh != nil || rawErrCh != nil {
			select {
			case rawBatch, ok := <-rawEventsCh:
				if !ok {
					rawEventsCh = nil
					continue
				}
				batch, err := convertBatch(rawBatch, parser)
				if err == nil {
					err = stream.fold(batch)
				}
				if err == nil {
					err = sendStreamBatch(streamCtx, stream.events, batch)
				}
				if err != nil {
					sendStreamErr(stream.errCh, err)
					return
				}
			case err, ok := <-rawErrCh:
				if !ok {
					rawErrCh = nil
					continue
				}
				if errors.Is(err, errCursorEvicted) {
					rawEventsCh, rawErrCh, err = resyncStream(streamCtx, s, parser, stream)
					if err == nil {
						continue
					}
				}
				sendStreamErr(stream.errCh, err)
				return
			}
		}
	}()
	return stream, nil
}

// resyncStream re-establishes the raw stream after a cursor eviction (see
// evictCursors) and delivers the membership difference as ordinary entries.
func resyncStream[T comparable](
	ctx context.Context,
	s *set,
	parser dbutil.Serializer[T],
	stream *Stream[T],
) (<-chan []LogEntry, <-chan error, error) {
	freshValues, rawEventsCh, rawErrCh, err := s.streamEvents(ctx)
	if err != nil {
		return nil, nil, err
	}
	fresh, err := convertSet(freshValues, parser)
	if err != nil {
		return nil, nil, err
	}
	if reconciliation := stream.reconcile(fresh); len(reconciliation) > 0 {
		if err := sendStreamBatch(ctx, stream.events, reconciliation); err != nil {
			return nil, nil, err
		}
	}
	return rawEventsCh, rawErrCh, nil
}

// diffStates returns the operations that transform `from` into `to`.
func diffStates[T comparable](from, to mapset.Set[T]) []TLogEntry[T] {
	entries := make([]TLogEntry[T], 0)
	from.Each(func(item T) bool {
		if !to.ContainsOne(item) {
			entries = append(entries, TLogEntry[T]{Op: LogOperationRemove, Value: item})
		}
		return false
	})
	to.Each(func(item T) bool {
		if !from.ContainsOne(item) {
			entries = append(entries, TLogEntry[T]{Op: LogOperationAdd, Value: item})
		}
		return false
	})
	return entries
}

func foldInto[T comparable](members mapset.Set[T], batch []TLogEntry[T]) error {
	for _, entry := range batch {
		switch entry.Op {
		case LogOperationAdd:
			members.Add(entry.Value)
		case LogOperationRemove:
			members.Remove(entry.Value)
		default:
			return fmt.Errorf("unknown stream operation: %d", entry.Op)
		}
	}
	return nil
}

func convertBatch[T comparable](raw []LogEntry, parser dbutil.Serializer[T]) ([]TLogEntry[T], error) {
	batch := make([]TLogEntry[T], len(raw))
	for i, entry := range raw {
		value, err := parser.Unmarshal(entry.Value)
		if err != nil {
			return nil, err
		}
		batch[i] = TLogEntry[T]{Op: entry.Op, Value: value}
	}
	return batch, nil
}

func convertSet[T comparable](raw mapset.Set[string], parser dbutil.Serializer[T]) (mapset.Set[T], error) {
	items := mapset.NewSetWithSize[T](raw.Cardinality())
	for item := range raw.Iter() {
		value, err := parser.Unmarshal([]byte(item))
		if err != nil {
			return nil, err
		}
		items.Add(value)
	}
	return items, nil
}

type rawSerializer struct{}

func (rawSerializer) Marshal(v string) []byte            { return []byte(v) }
func (rawSerializer) Unmarshal(b []byte) (string, error) { return string(b), nil }
