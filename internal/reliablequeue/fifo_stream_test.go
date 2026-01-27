package reliablequeue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/util"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestFIFOStreamInitialSnapshotAndSequence(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		queue := newFIFO(db, "stream_sequence")
		initialItems := [][]byte{
			[]byte("first"),
			[]byte("second"),
			[]byte("third"),
		}
		for _, item := range initialItems {
			enqueue(t, db, queue, item)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		initialValues, events, errCh, err := queue.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		require.Equal(t, initialItems, initialValues)
		local := cloneQueue(initialValues)
		expected := cloneQueue(initialValues)

		enqueue(t, db, queue, []byte("fourth"))
		expected = append(expected, []byte("fourth"))
		awaitQueueState(t, ctx, events, errCh, &local, expected)
		requireQueueMatchesDB(t, db, queue, expected)

		item, err := dequeue(t, db, queue)
		require.NoError(t, err)
		require.Equal(t, expected[0], item)
		expected = expected[1:]
		awaitQueueState(t, ctx, events, errCh, &local, expected)
		requireQueueMatchesDB(t, db, queue, expected)

		enqueue(t, db, queue, []byte("fifth"))
		enqueue(t, db, queue, []byte("sixth"))
		expected = append(expected, []byte("fifth"), []byte("sixth"))
		awaitQueueState(t, ctx, events, errCh, &local, expected)
		requireQueueMatchesDB(t, db, queue, expected)
	})
}

func TestFIFOStreamEmptyQueueTransitions(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		queue := newFIFO(db, "stream_empty")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		initialValues, events, errCh, err := queue.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		require.Len(t, initialValues, 0)
		local := cloneQueue(initialValues)
		expected := cloneQueue(initialValues)

		payload := []byte("only")
		enqueue(t, db, queue, payload)
		expected = append(expected, payload)
		awaitQueueState(t, ctx, events, errCh, &local, expected)
		requireQueueMatchesDB(t, db, queue, expected)

		item, err := dequeue(t, db, queue)
		require.NoError(t, err)
		require.Equal(t, payload, item)
		expected = expected[1:]
		awaitQueueState(t, ctx, events, errCh, &local, expected)
		requireQueueMatchesDB(t, db, queue, expected)

		_, err = dequeueAllowEmpty(t, db, queue)
		require.ErrorIs(t, err, ErrQueueEmpty)
		awaitQueueState(t, ctx, events, errCh, &local, expected)
		requireQueueMatchesDB(t, db, queue, expected)
	})
}

func TestFIFOStreamEnqueueBatchSingleEvent(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		queue := newFIFO(db, "stream_enqueue_batch")

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		initialValues, events, errCh, err := queue.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		require.Empty(t, initialValues)

		const batchSize = 12
		items := make([][]byte, 0, batchSize)
		for i := range batchSize {
			items = append(items, makePayload(i))
		}

		enqueueBatch(t, db, queue, items)

		batch := readNextBatch(t, ctx, events, errCh)
		require.Equal(t, StreamEventTypeEnqueued, batch.Type)
		require.Equal(t, items, batch.Items)

		drainOptionalEmptyDequeue(t, ctx, events, errCh)
		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestFIFOStreamDequeueBatchSingleEvent(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		queue := newFIFO(db, "stream_dequeue_batch")

		const totalItems = 10
		items := make([][]byte, 0, totalItems)
		for i := range totalItems {
			items = append(items, makePayload(i))
		}
		enqueueBatch(t, db, queue, items)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		initialValues, events, errCh, err := queue.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		require.Equal(t, items, initialValues)

		const dequeueCount = 7
		dequeued, err := dequeueBatch(t, db, queue, dequeueCount)
		require.NoError(t, err)
		require.Equal(t, items[:dequeueCount], dequeued)

		batch := readNextBatch(t, ctx, events, errCh)
		require.Equal(t, StreamEventTypeDequeued, batch.Type)
		require.Equal(t, dequeued, batch.Items)

		assertNoExtraBatch(t, ctx, events, errCh)
	})
}

func TestFIFOStreamHighActivity(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		queue := newFIFO(db, "stream_high_activity")

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		initialValues, events, errCh, err := queue.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		local := cloneQueue(initialValues)
		expected := cloneQueue(initialValues)
		rng := rand.New(rand.NewPCG(1, 2))
		nextID := 0

		const (
			operations = 60
			batchSize  = 5
		)
		for i := range operations {
			enqueueOp := len(expected) == 0 || rng.IntN(100) < 65
			if enqueueOp {
				payload := makePayload(nextID)
				nextID++
				enqueue(t, db, queue, payload)
				expected = append(expected, payload)
			} else {
				item, err := dequeueAllowEmpty(t, db, queue)
				if err == nil {
					require.NotEmpty(t, expected)
					require.Equal(t, expected[0], item)
					expected = expected[1:]
				} else if !errors.Is(err, ErrQueueEmpty) {
					require.NoError(t, err)
				}
			}

			if (i+1)%batchSize == 0 {
				awaitQueueState(t, ctx, events, errCh, &local, expected)
				requireQueueMatchesDB(t, db, queue, expected)
			}
		}
		awaitQueueState(t, ctx, events, errCh, &local, expected)
		requireQueueMatchesDB(t, db, queue, expected)
	})
}

func FuzzFIFOStreamHighActivity(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 0})
	f.Add([]byte{1, 1, 1, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 {
			return
		}
		if len(ops) > 64 {
			ops = ops[:64]
		}

		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			queue := newFIFO(db, "stream_fuzz")
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			initialValues, events, errCh, err := queue.Stream(ctx)
			require.NoError(t, err)
			defer drainStream(t, cancel, errCh)

			local := cloneQueue(initialValues)
			expected := cloneQueue(initialValues)
			nextID := 0

			const batchSize = 4
			for i, op := range ops {
				if op%2 == 0 {
					payload := makePayload(nextID)
					nextID++
					enqueue(t, db, queue, payload)
					expected = append(expected, payload)
				} else {
					item, err := dequeueAllowEmpty(t, db, queue)
					if err == nil {
						require.NotEmpty(t, expected)
						require.Equal(t, expected[0], item)
						expected = expected[1:]
					} else if !errors.Is(err, ErrQueueEmpty) {
						require.NoError(t, err)
					}
				}

				if (i+1)%batchSize == 0 {
					awaitQueueState(t, ctx, events, errCh, &local, expected)
				}
			}

			awaitQueueState(t, ctx, events, errCh, &local, expected)
			requireQueueMatchesDB(t, db, queue, expected)
		})
	})
}

func FuzzFIFOStreamConcurrentReadersWriters(f *testing.F) {
	// This fuzz test simulates concurrent writers/readers against a FIFO stream.
	// Writers update a mutex-protected canonical queue first, then apply the same
	// operation to the distributed queue in a globally ordered sequence so the
	// canonical state defines the expected outcome. Readers continuously consume
	// Stream events to maintain their own local queues and must converge to the
	// canonical state within 1 second after writers finish. The fuzz input
	// controls writer/reader counts, per-writer durations, and operation mix.
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
		enqueueBias := 50 + rng.IntN(40)

		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			queue := newFIFO(db, "stream_fuzz_concurrent")
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()

			type opKind int
			const (
				opEnqueue opKind = iota
				opDequeue
			)

			type queueOp struct {
				kind       opKind
				items      [][]byte
				allowEmpty bool
			}

			type readerState struct {
				mu     sync.Mutex
				local  [][]byte
				events <-chan StreamEventBatch
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

			applyQueueOp := func(op queueOp) error {
				switch op.kind {
				case opEnqueue:
					if len(op.items) == 0 {
						return nil
					}
					_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
						for _, item := range op.items {
							if err := queue.Enqueue(tx, item); err != nil {
								return nil, err
							}
						}
						return nil, nil
					})
					return err
				case opDequeue:
					var item []byte
					_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
						var err error
						item, err = queue.Dequeue(tx)
						return nil, err
					})
					if op.allowEmpty {
						if err == nil {
							return fmt.Errorf("expected empty dequeue, got %q", string(item))
						}
						if !errors.Is(err, ErrQueueEmpty) {
							return err
						}
						return nil
					}
					if err != nil {
						return err
					}
					if len(op.items) != 1 {
						return fmt.Errorf("missing expected dequeue item")
					}
					if !bytes.Equal(item, op.items[0]) {
						return fmt.Errorf("dequeue mismatch: got %q want %q", string(item), string(op.items[0]))
					}
					return nil
				default:
					return fmt.Errorf("unknown operation kind: %d", op.kind)
				}
			}

			readers := make([]*readerState, 0, readerCount)
			var readerWG sync.WaitGroup
			for range readerCount {
				streamCtx, streamCancel := context.WithCancel(ctx)
				readerCancelMu.Lock()
				readerCancels = append(readerCancels, streamCancel)
				readerCancelMu.Unlock()

				initialValues, events, streamErrCh, err := queue.Stream(streamCtx)
				require.NoError(t, err)

				reader := &readerState{
					local:  cloneQueue(initialValues),
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

			snapshot := func(r *readerState) [][]byte {
				r.mu.Lock()
				defer r.mu.Unlock()
				return cloneQueue(r.local)
			}

			for i := 1; i < len(readers); i++ {
				if !queuesEqual(snapshot(readers[0]), snapshot(readers[i])) {
					t.Fatalf("initial reader state mismatch: %q vs %q", snapshot(readers[0]), snapshot(readers[i]))
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
				writerBias := enqueueBias + writerRng.IntN(20) - 10
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

						var op queueOp
						var opID uint64

						canonicalMu.Lock()
						opSeq++
						opID = opSeq
						if rng.IntN(100) < bias {
							payload := makePayload(nextID)
							nextID++
							canonical = append(canonical, payload)
							op.kind = opEnqueue
							op.items = [][]byte{payload}
						} else {
							op.kind = opDequeue
							if len(canonical) == 0 {
								op.allowEmpty = true
							} else {
								item := canonical[0]
								canonical = canonical[1:]
								op.items = [][]byte{item}
							}
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

						if err := applyQueueOp(op); err != nil {
							recordErr(err)
							return
						}

						opMu.Lock()
						nextOp++
						opMu.Unlock()
						opCond.Broadcast()

						ops++
						time.Sleep(time.Duration(rng.IntN(6)) * time.Millisecond)
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
			expected := cloneQueue(canonical)
			canonicalMu.Unlock()

			deadline := time.Now().Add(1 * time.Second)
			for {
				select {
				case err := <-errCh:
					t.Fatalf("stream fuzz error: %v", err)
				default:
				}

				allMatch := true
				for _, reader := range readers {
					if !queuesEqual(snapshot(reader), expected) {
						allMatch = false
						break
					}
				}
				if allMatch {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("readers did not converge to canonical state within 1s: expected %q", expected)
				}
				time.Sleep(20 * time.Millisecond)
			}

			requireQueueMatchesDB(t, db, queue, expected)

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

func applyStreamBatch(current [][]byte, batch StreamEventBatch) ([][]byte, error) {
	switch batch.Type {
	case StreamEventTypeEnqueued:
		if len(batch.Items) == 0 {
			return current, nil
		}
		return append(current, batch.Items...), nil
	case StreamEventTypeDequeued:
		for _, item := range batch.Items {
			if len(current) == 0 {
				return current, fmt.Errorf("dequeue event on empty queue")
			}
			if !bytes.Equal(current[0], item) {
				return current, fmt.Errorf("dequeue mismatch: got %q want %q", string(item), string(current[0]))
			}
			current = current[1:]
		}
		return current, nil
	default:
		return current, fmt.Errorf("unknown stream event type: %d", batch.Type)
	}
}

func awaitQueueState(
	t *testing.T,
	ctx context.Context,
	events <-chan StreamEventBatch,
	errCh <-chan error,
	local *[][]byte,
	expected [][]byte,
) {
	t.Helper()
	for {
		if queuesEqual(*local, expected) {
			select {
			case batch, ok := <-events:
				if !ok {
					t.Fatal("events channel closed")
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
				t.Fatalf("timeout waiting for queue state: %v", ctx.Err())
			}
		}

		select {
		case batch, ok := <-events:
			if !ok {
				t.Fatal("events channel closed")
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
			t.Fatalf("timeout waiting for queue state: %v", ctx.Err())
		}
	}
}

func readNextBatch(
	t *testing.T,
	ctx context.Context,
	events <-chan StreamEventBatch,
	errCh <-chan error,
) StreamEventBatch {
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
	return StreamEventBatch{}
}

func drainOptionalEmptyDequeue(
	t *testing.T,
	ctx context.Context,
	events <-chan StreamEventBatch,
	errCh <-chan error,
) {
	t.Helper()
	select {
	case batch, ok := <-events:
		if !ok {
			t.Fatal("events channel closed")
		}
		if batch.Type != StreamEventTypeDequeued || len(batch.Items) != 0 {
			t.Fatalf("unexpected batch: type=%v items=%d", batch.Type, len(batch.Items))
		}
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed")
		}
		t.Fatalf("stream error: %v", err)
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		t.Fatalf("timeout waiting for optional batch: %v", ctx.Err())
	}
}

func assertNoExtraBatch(
	t *testing.T,
	ctx context.Context,
	events <-chan StreamEventBatch,
	errCh <-chan error,
) {
	t.Helper()
	select {
	case batch, ok := <-events:
		if !ok {
			t.Fatal("events channel closed")
		}
		t.Fatalf("unexpected extra batch: type=%v items=%d", batch.Type, len(batch.Items))
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

func cloneQueue(items [][]byte) [][]byte {
	if items == nil {
		return nil
	}
	copied := make([][]byte, len(items))
	copy(copied, items)
	return copied
}

func queuesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func readQueueValues(t testing.TB, db util.DbRoot, queue *FIFO) [][]byte {
	t.Helper()
	var kvs []fdb.KeyValue
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		begin, end := queue.subspace.FDBRangeKeys()
		var err error
		kvs, err = tx.GetRange(
			fdb.KeyRange{Begin: begin, End: end},
			fdb.RangeOptions{Mode: fdb.StreamingModeWantAll},
		).GetSliceWithError()
		return nil, err
	})
	require.NoError(t, err)

	values := make([][]byte, len(kvs))
	for i, kv := range kvs {
		values[i] = kv.Value
	}
	return values
}

func requireQueueMatchesDB(t *testing.T, db util.DbRoot, queue *FIFO, expected [][]byte) {
	t.Helper()
	actual := readQueueValues(t, db, queue)
	require.True(t, queuesEqual(expected, actual), "queue mismatch: expected %q got %q", expected, actual)
}

func dequeueAllowEmpty(t testing.TB, db util.DbRoot, queue *FIFO) ([]byte, error) {
	t.Helper()
	var item []byte
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		var err error
		item, err = queue.Dequeue(tx)
		return nil, err
	})
	return item, err
}

func enqueueBatch(t testing.TB, db util.DbRoot, queue *FIFO, items [][]byte) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, item := range items {
			if err := queue.Enqueue(tx, item); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func dequeueBatch(t testing.TB, db util.DbRoot, queue *FIFO, count int) ([][]byte, error) {
	t.Helper()
	items := make([][]byte, 0, count)
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for range count {
			item, err := queue.Dequeue(tx)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

func drainStream(t *testing.T, cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()
	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
	}
}

func makePayload(id int) []byte {
	return fmt.Appendf(nil, "item-%d", id)
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
