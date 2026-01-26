package reliableset

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/pkg/util"
	testutil "github.com/futura-platform/f4a/pkg/util/test"
	"github.com/stretchr/testify/require"
)

func TestSetStreamInitialSnapshotAndSequence(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "stream_sequence")
		initialItems := [][]byte{
			[]byte("first"),
			[]byte("second"),
			[]byte("third"),
		}
		addBatch(t, db, set, initialItems)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		initialValues, events, errCh, err := set.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		require.True(t, stateSetsEqual(initialValues, mapset.NewSet[string]("first", "second", "third")))
		local := cloneSet(initialValues)
		expected := cloneSet(initialValues)

		addItem(t, db, set, []byte("fourth"))
		expected.Add("fourth")
		awaitSetState(t, ctx, events, errCh, &local, expected)
		requireSetMatchesDB(t, db, set, expected)

		removeItem(t, db, set, []byte("second"))
		expected.Remove("second")
		awaitSetState(t, ctx, events, errCh, &local, expected)
		requireSetMatchesDB(t, db, set, expected)

		addItem(t, db, set, []byte("fifth"))
		addItem(t, db, set, []byte("sixth"))
		expected.Add("fifth")
		expected.Add("sixth")
		awaitSetState(t, ctx, events, errCh, &local, expected)
		requireSetMatchesDB(t, db, set, expected)
	})
}

func TestSetStreamEmptyTransitions(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "stream_empty")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		initialValues, events, errCh, err := set.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		require.True(t, initialValues.Cardinality() == 0)
		local := cloneSet(initialValues)
		expected := cloneSet(initialValues)

		payload := []byte("only")
		addItem(t, db, set, payload)
		expected.Add(string(payload))
		awaitSetState(t, ctx, events, errCh, &local, expected)
		requireSetMatchesDB(t, db, set, expected)

		removeItem(t, db, set, payload)
		expected.Remove(string(payload))
		awaitSetState(t, ctx, events, errCh, &local, expected)
		requireSetMatchesDB(t, db, set, expected)
	})
}

func TestSetStreamAddBatchSingleEvent(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "stream_add_batch")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		_, events, errCh, err := set.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		const batchSize = 12
		items := make([][]byte, 0, batchSize)
		for i := range batchSize {
			items = append(items, makePayload(i))
		}

		addBatch(t, db, set, items)

		batch := readNextBatch(t, ctx, events, errCh)
		require.Len(t, batch, batchSize)
		for i, entry := range batch {
			require.Equal(t, LogOperationAdd, entry.Op)
			require.Equal(t, items[i], entry.Value)
		}

		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestSetStreamRemoveBatchSingleEvent(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "stream_remove_batch")

		const totalItems = 10
		items := make([][]byte, 0, totalItems)
		for i := range totalItems {
			items = append(items, makePayload(i))
		}
		addBatch(t, db, set, items)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		_, events, errCh, err := set.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		toRemove := items[:6]
		removeBatch(t, db, set, toRemove)

		batch := readNextBatch(t, ctx, events, errCh)
		require.Len(t, batch, len(toRemove))
		for i, entry := range batch {
			require.Equal(t, LogOperationRemove, entry.Op)
			require.Equal(t, toRemove[i], entry.Value)
		}

		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestSetStreamHighActivity(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "stream_high_activity")

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		initialValues, events, errCh, err := set.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		local := cloneSet(initialValues)
		expected := cloneSet(initialValues)
		rng := rand.New(rand.NewPCG(1, 2))
		nextID := 0

		const (
			operations = 60
			batchSize  = 5
		)
		for i := range operations {
			addOp := expected.Cardinality() == 0 || rng.IntN(100) < 65
			if addOp {
				payload := makePayload(nextID)
				nextID++
				addItem(t, db, set, payload)
				expected.Add(string(payload))
			} else {
				var target []byte
				if expected.Cardinality() == 0 {
					target = []byte("missing")
				} else {
					target = pickRandomItem(rng, expected)
				}
				removeItem(t, db, set, target)
				expected.Remove(string(target))
			}

			if (i+1)%batchSize == 0 {
				awaitSetState(t, ctx, events, errCh, &local, expected)
				requireSetMatchesDB(t, db, set, expected)
			}
		}
		awaitSetState(t, ctx, events, errCh, &local, expected)
		requireSetMatchesDB(t, db, set, expected)
	})
}

func FuzzSetStreamConcurrentReadersWriters(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{7, 6, 5, 4, 3, 2, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}

		seed1, seed2 := seedsFromBytes(data)
		rng := rand.New(rand.NewPCG(seed1, seed2))
		writerCount := 1 + rng.IntN(4)
		readerCount := 1 + rng.IntN(4)
		baseDuration := time.Duration(700+rng.IntN(500)) * time.Millisecond
		maxOps := 40 + rng.IntN(80)
		addBias := 50 + rng.IntN(40)

		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			require.NoError(t, db.Options().SetTransactionRetryLimit(10))
			writerSet := newSet(t, db, "stream_fuzz_concurrent")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			type opKind int
			const (
				opAdd opKind = iota
				opRemove
			)

			type setOp struct {
				kind opKind
				item []byte
			}

			type readerState struct {
				mu     sync.Mutex
				local  mapset.Set[string]
				events <-chan []LogEntry
				errCh  <-chan error
			}

			stopReaders := make(chan struct{})
			var stopReadersOnce sync.Once
			var readerCancelMu sync.Mutex
			readerCancels := make([]context.CancelFunc, 0, readerCount)

			cancelReaders := func() {
				stopReadersOnce.Do(func() {
					close(stopReaders)
					readerCancelMu.Lock()
					cancels := append([]context.CancelFunc(nil), readerCancels...)
					readerCancelMu.Unlock()
					for _, cancel := range cancels {
						cancel()
					}
				})
			}

			var opMu sync.Mutex
			opCond := sync.NewCond(&opMu)
			var nextOp uint64 = 1
			var stopOps bool

			stopWriters := make(chan struct{})
			var stopWritersOnce sync.Once

			errCh := make(chan error, 1)
			var errOnce sync.Once

			recordErr := func(err error) {
				if err == nil {
					return
				}
				errOnce.Do(func() {
					errCh <- err
					stopWritersOnce.Do(func() {
						close(stopWriters)
					})
					opMu.Lock()
					stopOps = true
					opMu.Unlock()
					opCond.Broadcast()
					cancelReaders()
				})
			}

			applySetOp := func(op setOp) error {
				_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
					switch op.kind {
					case opAdd:
						return nil, writerSet.Add(tx, op.item)
					case opRemove:
						return nil, writerSet.Remove(tx, op.item)
					default:
						return nil, fmt.Errorf("unknown operation kind: %d", op.kind)
					}
				})
				return err
			}

			readers := make([]*readerState, 0, readerCount)
			var readerWG sync.WaitGroup
			for range readerCount {
				streamCtx, streamCancel := context.WithCancel(ctx)
				readerCancelMu.Lock()
				readerCancels = append(readerCancels, streamCancel)
				readerCancelMu.Unlock()

				readerSet, err := CreateOrOpen(db.Database, setPath(db, "stream_fuzz_concurrent"), testMaxItemSize)
				require.NoError(t, err)
				initialValues, events, streamErrCh, err := readerSet.Stream(streamCtx)
				require.NoError(t, err)

				reader := &readerState{
					local:  cloneSet(initialValues),
					events: events,
					errCh:  streamErrCh,
				}
				readers = append(readers, reader)

				readerWG.Add(1)
				go func(r *readerState) {
					defer readerWG.Done()
					for {
						select {
						case batch, ok := <-r.events:
							if !ok {
								select {
								case <-stopReaders:
									return
								default:
									recordErr(errors.New("stream events channel closed"))
									return
								}
							}
							if len(batch) == 0 {
								continue
							}
							r.mu.Lock()
							updated, err := applyStreamBatch(r.local, batch)
							if err != nil {
								r.mu.Unlock()
								recordErr(err)
								return
							}
							r.local = updated
							r.mu.Unlock()
						case err, ok := <-r.errCh:
							if !ok {
								return
							}
							select {
							case <-stopReaders:
								return
							default:
								recordErr(fmt.Errorf("stream error: %w", err))
								return
							}
						}
					}
				}(reader)
			}

			snapshot := func(r *readerState) mapset.Set[string] {
				r.mu.Lock()
				defer r.mu.Unlock()
				return cloneSet(r.local)
			}

			for i := 1; i < len(readers); i++ {
				if !stateSetsEqual(snapshot(readers[0]), snapshot(readers[i])) {
					t.Fatalf("initial reader state mismatch: %v vs %v", snapshot(readers[0]), snapshot(readers[i]))
				}
			}

			var canonicalMu sync.Mutex
			canonical := snapshot(readers[0])
			nextID := 0
			var opSeq uint64

			var writerWG sync.WaitGroup
			for i := range writerCount {
				writerWG.Add(1)
				writerSeed1 := seed1 + uint64(i+1)*13
				writerSeed2 := seed2 + uint64(i+1)*17
				writerRng := rand.New(rand.NewPCG(writerSeed1, writerSeed2))
				writerDuration := baseDuration + time.Duration(writerRng.IntN(400))*time.Millisecond
				writerBias := addBias + writerRng.IntN(20) - 10
				if writerBias < 40 {
					writerBias = 40
				}
				if writerBias > 95 {
					writerBias = 95
				}

				go func(rng *rand.Rand, duration time.Duration, bias int) {
					defer writerWG.Done()
					start := time.Now()
					ops := 0
					for time.Since(start) < duration && ops < maxOps {
						select {
						case <-stopWriters:
							return
						default:
						}

						var op setOp
						var opID uint64

						canonicalMu.Lock()
						opSeq++
						opID = opSeq
						if rng.IntN(100) < bias {
							payload := makePayload(nextID)
							nextID++
							canonical.Add(string(payload))
							op.kind = opAdd
							op.item = payload
						} else {
							var payload []byte
							if canonical.Cardinality() == 0 {
								payload = []byte("missing")
							} else {
								payload = pickRandomItem(rng, canonical)
								canonical.Remove(string(payload))
							}
							op.kind = opRemove
							op.item = payload
						}
						canonicalMu.Unlock()

						opMu.Lock()
						for opID != nextOp && !stopOps {
							opCond.Wait()
						}
						if stopOps {
							opMu.Unlock()
							return
						}
						opMu.Unlock()

						if err := applySetOp(op); err != nil {
							recordErr(err)
							return
						}

						opMu.Lock()
						nextOp++
						opMu.Unlock()
						opCond.Broadcast()

						ops++
						time.Sleep(time.Duration(rng.IntN(12)+4) * time.Millisecond)
					}
				}(writerRng, writerDuration, writerBias)
			}

			writerWG.Wait()
			select {
			case err := <-errCh:
				t.Fatalf("stream fuzz error: %v", err)
			default:
			}

			canonicalMu.Lock()
			expected := cloneSet(canonical)
			canonicalMu.Unlock()

			deadline := time.Now().Add(2 * time.Second)
			for {
				select {
				case err := <-errCh:
					t.Fatalf("stream fuzz error: %v", err)
				default:
				}

				allMatch := true
				for _, reader := range readers {
					if !stateSetsEqual(snapshot(reader), expected) {
						allMatch = false
						break
					}
				}
				if allMatch {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("readers did not converge to canonical state within 2s: expected %v", expected)
				}
				time.Sleep(25 * time.Millisecond)
			}

			requireSetMatchesDB(t, db, writerSet, expected)

			cancelReaders()
			readerWG.Wait()

			select {
			case err := <-errCh:
				t.Fatalf("stream fuzz error: %v", err)
			default:
			}
		})
	})
}

func applyStreamBatch(current mapset.Set[string], batch []LogEntry) (mapset.Set[string], error) {
	if current == nil {
		current = mapset.NewSet[string]()
	}
	for _, entry := range batch {
		switch entry.Op {
		case LogOperationAdd:
			current.Add(string(entry.Value))
		case LogOperationRemove:
			current.Remove(string(entry.Value))
		default:
			return current, fmt.Errorf("unknown stream operation: %d", entry.Op)
		}
	}
	return current, nil
}

func awaitSetState(
	t *testing.T,
	ctx context.Context,
	events <-chan []LogEntry,
	errCh <-chan error,
	local *mapset.Set[string],
	expected mapset.Set[string],
) {
	t.Helper()
	for {
		if stateSetsEqual(*local, expected) {
			select {
			case batch, ok := <-events:
				if !ok {
					t.Fatal("events channel closed")
				}
				if len(batch) == 0 {
					continue
				}
				var err error
				*local, err = applyStreamBatch(*local, batch)
				require.NoError(t, err)
				continue
			case err, ok := <-errCh:
				if !ok {
					t.Fatal("error channel closed")
				}
				t.Fatalf("stream error: %v", err)
			case <-time.After(50 * time.Millisecond):
				return
			case <-ctx.Done():
				t.Fatalf("timeout waiting for set state: %v", ctx.Err())
			}
		}

		select {
		case batch, ok := <-events:
			if !ok {
				t.Fatal("events channel closed")
			}
			if len(batch) == 0 {
				continue
			}
			var err error
			*local, err = applyStreamBatch(*local, batch)
			require.NoError(t, err)
		case err, ok := <-errCh:
			if !ok {
				t.Fatal("error channel closed")
			}
			t.Fatalf("stream error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for set state: %v", ctx.Err())
		}
	}
}

func readNextBatch(
	t *testing.T,
	ctx context.Context,
	events <-chan []LogEntry,
	errCh <-chan error,
) []LogEntry {
	t.Helper()
	select {
	case batch, ok := <-events:
		if !ok {
			t.Fatal("events channel closed")
		}
		return batch
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed")
		}
		t.Fatalf("stream error: %v", err)
	case <-ctx.Done():
		t.Fatalf("timeout waiting for batch: %v", ctx.Err())
	}
	return nil
}

func assertNoExtraBatch(
	t *testing.T,
	ctx context.Context,
	events <-chan []LogEntry,
	errCh <-chan error,
) {
	t.Helper()
	select {
	case batch, ok := <-events:
		if !ok {
			t.Fatal("events channel closed")
		}
		t.Fatalf("unexpected extra batch: entries=%d", len(batch))
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed")
		}
		t.Fatalf("stream error: %v", err)
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		t.Fatalf("timeout waiting for idle stream: %v", ctx.Err())
	}
}

func drainStream(t *testing.T, cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("stream error: %v", err)
		}
	case <-time.After(2 * time.Second):
	}
}

func makePayload(id int) []byte {
	return fmt.Appendf(nil, "item-%d", id)
}

func pickRandomItem(rng *rand.Rand, items mapset.Set[string]) []byte {
	if items == nil || items.Cardinality() == 0 {
		return nil
	}
	values := items.ToSlice()
	return []byte(values[rng.IntN(len(values))])
}

func seedsFromBytes(data []byte) (uint64, uint64) {
	var seed1 uint64 = 1
	var seed2 uint64 = 2
	for i, b := range data {
		if i%2 == 0 {
			seed1 = seed1*1664525 + uint64(b) + 1013904223
		} else {
			seed2 = seed2*22695477 + uint64(b) + 1
		}
	}
	if seed1 == 0 {
		seed1 = 1
	}
	if seed2 == 0 {
		seed2 = 2
	}
	return seed1, seed2
}
