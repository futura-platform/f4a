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
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestSetStreamInitialSnapshotAndSequence(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_sequence")
		initialItems := [][]byte{
			[]byte("first"),
			[]byte("second"),
			[]byte("third"),
		}
		addBatch(t, db, set, initialItems)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		initialValues, events, errCh := stream.Snapshot(), stream.Events(), stream.Err()
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
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_empty")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		initialValues, events, errCh := stream.Snapshot(), stream.Events(), stream.Err()
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

func TestSetStreamCancelWhileBlockedOnSendReturnsContextError(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_cancel_blocked_send")

		ctx, cancel := context.WithCancel(t.Context())
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		errCh := stream.Err()

		addItem(t, db, set, []byte("blocked"))

		time.Sleep(100 * time.Millisecond)
		cancel()

		select {
		case err, ok := <-errCh:
			require.True(t, ok)
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for canceled stream error")
		}
	})
}

func TestSetStreamAddBatchSingleEvent(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_add_batch")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		events, errCh := stream.Events(), stream.Err()
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
			require.Equal(t, string(items[i]), entry.Value)
		}

		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestSetStreamDuplicateOperationsDeliveredAsWritten(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_duplicate_operations")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		events, errCh := stream.Events(), stream.Err()
		defer drainStream(t, cancel, errCh)

		payload := []byte("dup")
		addBatch(t, db, set, [][]byte{payload, payload})

		batch := readNextBatch(t, ctx, events, errCh)
		require.Equal(t, []TLogEntry[string]{
			{Op: LogOperationAdd, Value: "dup"},
			{Op: LogOperationAdd, Value: "dup"},
		}, batch)
		require.True(t, stateSetsEqual(stream.Snapshot(), mapset.NewSet[string]("dup")))

		removeBatch(t, db, set, [][]byte{payload, payload})

		batch = readNextBatch(t, ctx, events, errCh)
		require.Equal(t, []TLogEntry[string]{
			{Op: LogOperationRemove, Value: "dup"},
			{Op: LogOperationRemove, Value: "dup"},
		}, batch)
		require.True(t, stream.Snapshot().IsEmpty())

		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestSetStreamOpposingOperationsWithinSingleBatch(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_opposing_operations")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		events, errCh := stream.Events(), stream.Err()
		defer drainStream(t, cancel, errCh)

		payload := []byte("value")

		// add then remove in one transaction: both entries are delivered, in
		// order, and the membership is unchanged.
		applyLogBatch(t, db, set, []LogEntry{
			{Op: LogOperationAdd, Value: payload},
			{Op: LogOperationRemove, Value: payload},
		})
		batch := readNextBatch(t, ctx, events, errCh)
		require.Equal(t, []TLogEntry[string]{
			{Op: LogOperationAdd, Value: "value"},
			{Op: LogOperationRemove, Value: "value"},
		}, batch)
		require.True(t, stream.Snapshot().IsEmpty())
		requireSetMatchesDB(t, db, set, mapset.NewSet[string]())

		// remove then add in one transaction: a consumer folding the batch
		// sees a net add, and the membership holds the item.
		applyLogBatch(t, db, set, []LogEntry{
			{Op: LogOperationRemove, Value: payload},
			{Op: LogOperationAdd, Value: payload},
		})
		batch = readNextBatch(t, ctx, events, errCh)
		require.Equal(t, []TLogEntry[string]{
			{Op: LogOperationRemove, Value: "value"},
			{Op: LogOperationAdd, Value: "value"},
		}, batch)
		require.True(t, stateSetsEqual(stream.Snapshot(), mapset.NewSet[string]("value")))
		requireSetMatchesDB(t, db, set, mapset.NewSet[string](string(payload)))

		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestSetStreamSnapshotHoldsItemRemovedAndReAddedAcrossBatches(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_snapshot_remove_readd")
		addItem(t, db, set, []byte("x"))

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		events, errCh := stream.Events(), stream.Err()
		defer drainStream(t, cancel, errCh)
		require.True(t, stateSetsEqual(stream.Snapshot(), mapset.NewSet[string]("x")))

		// Two commits the consumer never reads between: the stream may read
		// them as one chunk or two, and either way the membership is right.
		removeItem(t, db, set, []byte("x"))
		addItem(t, db, set, []byte("x"))

		var seen []TLogEntry[string]
		for len(seen) < 2 {
			seen = append(seen, readNextBatch(t, ctx, events, errCh)...)
		}
		require.Equal(t, []TLogEntry[string]{
			{Op: LogOperationRemove, Value: "x"},
			{Op: LogOperationAdd, Value: "x"},
		}, seen)
		expected := mapset.NewSet[string]("x")
		require.True(t, stateSetsEqual(stream.Snapshot(), expected))
		requireSetMatchesDB(t, db, set, expected)
		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestSetStreamSnapshotIsFoldedBeforeTheBatchIsSent(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_snapshot_before_send")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		events, errCh := stream.Events(), stream.Err()
		defer drainStream(t, cancel, errCh)

		addBatch(t, db, set, [][]byte{[]byte("a"), []byte("b")})
		// Events is unbuffered, so the membership can only reach {a, b} while
		// the batch is still unreceived if the fold precedes the send.
		expected := mapset.NewSet[string]("a", "b")
		require.Eventually(t, func() bool {
			return stateSetsEqual(stream.Snapshot(), expected)
		}, 10*time.Second, 5*time.Millisecond)
		batch := readNextBatch(t, ctx, events, errCh)
		require.Len(t, batch, 2)
	})
}

func TestSetStreamSnapshotIsACopy(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_snapshot_copy")
		addItem(t, db, set, []byte("a"))

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, stream.Err())

		snapshot := stream.Snapshot()
		snapshot.Add("z")
		snapshot.Remove("a")
		require.True(t, stateSetsEqual(stream.Snapshot(), mapset.NewSet[string]("a")))
	})
}

func TestSetStreamEndsWithClosedChannels(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_ends_closed")

		ctx, cancel := context.WithCancel(t.Context())
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		cancel()

		select {
		case _, ok := <-stream.Events():
			require.False(t, ok, "expected Events to close, got a batch")
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Events to close")
		}
		select {
		case err, ok := <-stream.Err():
			if ok {
				require.ErrorIs(t, err, context.Canceled)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Err")
		}
		_, ok := <-stream.Err()
		require.False(t, ok, "expected Err to close after Events")
	})
}

func TestSetStreamRemoveBatchSingleEvent(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_remove_batch")

		const totalItems = 10
		items := make([][]byte, 0, totalItems)
		for i := range totalItems {
			items = append(items, makePayload(i))
		}
		addBatch(t, db, set, items)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		events, errCh := stream.Events(), stream.Err()
		defer drainStream(t, cancel, errCh)

		toRemove := items[:6]
		removeBatch(t, db, set, toRemove)

		batch := readNextBatch(t, ctx, events, errCh)
		require.Len(t, batch, len(toRemove))
		for i, entry := range batch {
			require.Equal(t, LogOperationRemove, entry.Op)
			require.Equal(t, string(toRemove[i]), entry.Value)
		}

		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestStreamResyncsAfterCursorEviction(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_resync_eviction")
		addBatch(t, db, set, [][]byte{[]byte("a"), []byte("b")})

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		initialValues, events, errCh := stream.Snapshot(), stream.Events(), stream.Err()
		defer drainStream(t, cancel, errCh)
		require.True(t, stateSetsEqual(initialValues, mapset.NewSet[string]("a", "b")))
		local := cloneSet(initialValues)

		// Wedge the whole delivery pipeline so later writes are provably unread:
		// wedge-1 blocks the Stream goroutine on its send to us, wedge-2 blocks
		// the streamEvents goroutine on its send to Stream, and wedge-3 blocks
		// the watch goroutine on its send to streamEvents — after which nothing
		// reads the log until we resume consuming.
		addItem(t, db, set, []byte("wedge-1"))
		time.Sleep(300 * time.Millisecond)
		addItem(t, db, set, []byte("wedge-2"))
		time.Sleep(300 * time.Millisecond)
		addItem(t, db, set, []byte("wedge-3"))
		time.Sleep(300 * time.Millisecond)

		// Behind the wedge: remove "a", evict the stream's cursor, and compact.
		// The removal is folded into the snapshot and its log entry cleared, so
		// the stream can never observe it from the log — only a re-sync from the
		// snapshot can reveal it.
		removeItem(t, db, set, []byte("a"))
		cursorID := readSingleCursorID(t, db, set)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			tx.Clear(set.cursorKey(cursorID, cursorKeyTail))
			tx.Clear(set.cursorKey(cursorID, cursorKeyLease))
			tx.Clear(set.cursorKey(cursorID, cursorKeyHint))
			return nil, nil
		})
		require.NoError(t, err)
		require.NoError(t, set.compactor.compactLog(t.Context(), db))

		addItem(t, db, set, []byte("c"))

		// The stream must detect the eviction, re-sync, and converge on the true
		// state — including the removal of "a" it never saw in the log — without
		// surfacing any error to us.
		expected := mapset.NewSet[string]("b", "wedge-1", "wedge-2", "wedge-3", "c")
		awaitSetState(t, ctx, events, errCh, &local, expected)
		require.True(t, stateSetsEqual(stream.Snapshot(), expected))
		requireSetMatchesDB(t, db, set, expected)
	})
}

func TestSetStreamHighActivity(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		set := newSet(t, db, "stream_high_activity")

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		stream, err := set.Stream(ctx)
		require.NoError(t, err)
		initialValues, events, errCh := stream.Snapshot(), stream.Events(), stream.Err()
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

		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
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
				stream *Stream[string]
				events <-chan []TLogEntry[string]
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

				readerSet, err := Open(db, db, setPath(db, "stream_fuzz_concurrent"))
				require.NoError(t, err)
				stream, err := readerSet.Stream(streamCtx)
				require.NoError(t, err)

				reader := &readerState{
					local:  stream.Snapshot(),
					stream: stream,
					events: stream.Events(),
					errCh:  stream.Err(),
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
					if !stateSetsEqual(snapshot(reader), expected) || !stateSetsEqual(reader.stream.Snapshot(), expected) {
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

func applyStreamBatch(current mapset.Set[string], batch []TLogEntry[string]) (mapset.Set[string], error) {
	if current == nil {
		current = mapset.NewSet[string]()
	}
	return current, foldInto(current, batch)
}

func applyLogBatch(t testing.TB, db dbutil.DbRoot, set *set, batch []LogEntry) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, entry := range batch {
			switch entry.Op {
			case LogOperationAdd:
				if err := set.Add(tx, entry.Value); err != nil {
					return nil, err
				}
			case LogOperationRemove:
				if err := set.Remove(tx, entry.Value); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unknown log operation: %d", entry.Op)
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func awaitSetState(
	t *testing.T,
	ctx context.Context,
	events <-chan []TLogEntry[string],
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
	events <-chan []TLogEntry[string],
	errCh <-chan error,
) []TLogEntry[string] {
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
	events <-chan []TLogEntry[string],
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
