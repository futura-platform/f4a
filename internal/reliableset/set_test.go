package reliableset

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/pkg/util"
	testutil "github.com/futura-platform/f4a/pkg/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setPath(db util.DbRoot, name string) []string {
	path := append([]string{}, db.Root.GetPath()...)
	path = append(path, "set", name)
	return path
}

func newSet(t testing.TB, db util.DbRoot, name string) *Set {
	t.Helper()
	set, err := CreateOrOpen(db.Database, setPath(db, name), testMaxItemSize)
	require.NoError(t, err)
	return set
}

const (
	testMaxItemSize   = 1024
	largeLogEntries   = 15000
	largeLogBatchSize = 200
)

func makeMaxPayload(id int) []byte {
	payload := make([]byte, testMaxItemSize)
	binary.LittleEndian.PutUint64(payload, uint64(id))
	return payload
}

func addLargeLog(t testing.TB, db util.DbRoot, set *Set, total int) {
	t.Helper()
	for start := 0; start < total; start += largeLogBatchSize {
		end := start + largeLogBatchSize
		if end > total {
			end = total
		}
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			for i := start; i < end; i++ {
				if err := set.Add(tx, makeMaxPayload(i)); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		require.NoError(t, err)
	}
}

func addItem(t testing.TB, db util.DbRoot, set *Set, item []byte) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		return nil, set.Add(tx, item)
	})
	require.NoError(t, err)
}

func removeItem(t testing.TB, db util.DbRoot, set *Set, item []byte) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		return nil, set.Remove(tx, item)
	})
	require.NoError(t, err)
}

func addBatch(t testing.TB, db util.DbRoot, set *Set, items [][]byte) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, item := range items {
			if err := set.Add(tx, item); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func removeBatch(t testing.TB, db util.DbRoot, set *Set, items [][]byte) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		for _, item := range items {
			if err := set.Remove(tx, item); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func readSetValues(t testing.TB, set *Set) mapset.Set[string] {
	t.Helper()
	var items mapset.Set[string]
	var err error
	items, _, err = set.Items(context.Background())
	require.NoError(t, err)
	return items
}

func requireSetMatchesDB(t *testing.T, db util.DbRoot, set *Set, expected mapset.Set[string]) {
	t.Helper()
	actual := readSetValues(t, set)
	require.True(t, stateSetsEqual(actual, expected), "set mismatch: expected %v got %v", expected, actual)
}

func TestSetAddRemove(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "basic")
		items := [][]byte{
			[]byte("alpha"),
			[]byte("bravo"),
			[]byte("charlie"),
		}
		addBatch(t, db, set, items)

		removeItem(t, db, set, []byte("bravo"))
		expected := mapset.NewSet[string]("alpha", "charlie")
		requireSetMatchesDB(t, db, set, expected)

		removeItem(t, db, set, []byte("bravo"))
		requireSetMatchesDB(t, db, set, expected)

		addItem(t, db, set, []byte("bravo"))
		expected.Add("bravo")
		requireSetMatchesDB(t, db, set, expected)
	})
}

func TestSetCreateOrOpenReusesPath(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		path := setPath(db, "reopen")
		set1, err := CreateOrOpen(db.Database, path, testMaxItemSize)
		require.NoError(t, err)

		addItem(t, db, set1, []byte("payload"))

		set2, err := CreateOrOpen(db.Database, path, testMaxItemSize)
		require.NoError(t, err)

		items := readSetValues(t, set2)
		require.True(t, stateSetsEqual(items, mapset.NewSet[string]("payload")))
	})
}

func TestSetEpochKeyChangesOnOperations(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "epoch_key_invariant")

		readEpoch := func() []byte {
			var val []byte
			_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				val = tx.Get(set.epochKey).MustGet()
				return nil, nil
			})
			require.NoError(t, err)
			return val
		}

		epoch0 := readEpoch()

		addItem(t, db, set, []byte("item1"))
		epoch1 := readEpoch()
		assert.NotEqual(t, epoch0, epoch1, "Add should change epochKey")

		addItem(t, db, set, []byte("item2"))
		epoch2 := readEpoch()
		assert.NotEqual(t, epoch1, epoch2, "second Add should change epochKey")

		removeItem(t, db, set, []byte("item1"))
		epoch3 := readEpoch()
		assert.NotEqual(t, epoch2, epoch3, "Remove should change epochKey")
	})
}

func TestSetItemSizeLimit(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "size_limit")
		set.maxItemSize = 32
		tooLarge := make([]byte, set.maxItemSize+1)

		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			return nil, set.Add(tx, tooLarge)
		})
		require.ErrorIs(t, err, ErrEntryTooLarge)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			return nil, set.Remove(tx, tooLarge)
		})
		require.ErrorIs(t, err, ErrEntryTooLarge)
	})
}

func TestSetCompactLog(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "compact")
		addItem(t, db, set, []byte("a"))
		addItem(t, db, set, []byte("b"))
		addItem(t, db, set, []byte("c"))
		removeItem(t, db, set, []byte("b"))

		err := set.compactLog(t.Context())
		require.NoError(t, err)

		expected := mapset.NewSet[string]("a", "c")
		requireSetMatchesDB(t, db, set, expected)

		begin, _ := set.logSubspace.FDBRangeKeys()
		logEntries, err := set.readLog(t.Context(), begin, nil)
		require.NoError(t, err)
		require.Empty(t, logEntries)
	})
}

func TestSetItemsLargeLog(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "items_large_log")
		addLargeLog(t, db, set, largeLogEntries)

		var items mapset.Set[string]
		var err error
		items, _, err = set.Items(t.Context())
		require.NoError(t, err)
		require.Equal(t, largeLogEntries, items.Cardinality())
	})
}

func TestSetItemsPinnedReadVersion(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "items_pinned")
		addItem(t, db, set, []byte("first"))

		var readVersion int64
		_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			readVersion = tx.GetReadVersion().MustGet()
			return nil, nil
		})
		require.NoError(t, err)

		addItem(t, db, set, []byte("second"))

		items, _, err := set.itemsAtReadVersion(context.Background(), readVersion)
		require.NoError(t, err)
		require.True(t, stateSetsEqual(items, mapset.NewSet[string]("first")))
	})
}

func TestSetCompactLargeLog(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "compact_large_log")
		addLargeLog(t, db, set, largeLogEntries)

		err := set.compactLog(t.Context())
		require.NoError(t, err)

		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			begin, end := set.logSubspace.FDBRangeKeys()
			kvs, err := tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{
				Limit: 1,
			}).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			require.Empty(t, kvs)
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestCursorRegistrationAndAdvance(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "cursor_registration")

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, events, errCh, err := set.Stream(ctx)
		require.NoError(t, err)
		defer drainStream(t, cancel, errCh)

		begin, _ := set.logSubspace.FDBRangeKeys()
		tail, lease := readCursor(t, db, set, set.consumerID)
		require.Equal(t, begin.FDBKey(), tail)
		require.True(t, lease.After(time.Now()))

		addItem(t, db, set, []byte("next"))
		_ = readNextBatch(t, ctx, events, errCh)

		lastKey := readLastLogKey(t, db, set)
		waitForCursorTail(t, db, set, set.consumerID, lastKey)
	})
}

func TestCompactionRespectsActiveCursor(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "compact_cursor")
		addItem(t, db, set, []byte("a"))
		addItem(t, db, set, []byte("b"))
		addItem(t, db, set, []byte("c"))

		logEntries := readLogEntries(t, db, set)
		require.Len(t, logEntries, 3)

		activeID := "active"
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			tx.Set(set.cursorKey(activeID, cursorKeyTail), logEntries[0].key.FDBKey())
			tx.Set(set.cursorKey(activeID, cursorKeyLease), encodeLease(time.Now().Add(time.Minute)))
			return nil, nil
		})
		require.NoError(t, err)

		err = set.compactLog(t.Context())
		require.NoError(t, err)

		remaining := readLogEntries(t, db, set)
		require.Len(t, remaining, 2)
		require.Equal(t, logEntries[1].key.FDBKey(), remaining[0].key.FDBKey())
	})
}

func TestCleanDeadCursors(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		set := newSet(t, db, "cursor_gc")
		expiredID := "expired"
		activeID := "active"
		now := time.Now()

		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			tx.Set(set.cursorKey(expiredID, cursorKeyTail), []byte("dead-tail"))
			tx.Set(set.cursorKey(expiredID, cursorKeyLease), encodeLease(now.Add(-time.Minute)))
			tx.Set(set.cursorKey(activeID, cursorKeyTail), []byte("live-tail"))
			tx.Set(set.cursorKey(activeID, cursorKeyLease), encodeLease(now.Add(time.Minute)))
			return nil, nil
		})
		require.NoError(t, err)

		index, err := set.cursorIndex(t.Context(), nil)
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			return nil, set.cleanDeadCursors(tx, index, now)
		})
		require.NoError(t, err)

		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			require.Nil(t, tx.Get(set.cursorKey(expiredID, cursorKeyTail)).MustGet())
			require.Nil(t, tx.Get(set.cursorKey(expiredID, cursorKeyLease)).MustGet())
			require.NotNil(t, tx.Get(set.cursorKey(activeID, cursorKeyTail)).MustGet())
			require.NotNil(t, tx.Get(set.cursorKey(activeID, cursorKeyLease)).MustGet())
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func BenchmarkSetAdd(b *testing.B) {
	testutil.WithEphemeralDBRoot(b, func(db util.DbRoot) {
		set := newSet(b, db, "bench_add")
		payload := []byte("payload")

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			if _, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				return nil, set.Add(tx, payload)
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSetRemove(b *testing.B) {
	testutil.WithEphemeralDBRoot(b, func(db util.DbRoot) {
		set := newSet(b, db, "bench_remove")
		payload := []byte("payload")

		b.StopTimer()
		addItem(b, db, set, payload)
		b.ReportAllocs()
		b.ResetTimer()
		b.StartTimer()

		for b.Loop() {
			if _, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				return nil, set.Remove(tx, payload)
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func stateSetsEqual(a, b mapset.Set[string]) bool {
	if a == nil || a.Cardinality() == 0 {
		return b == nil || b.Cardinality() == 0
	}
	if b == nil || b.Cardinality() == 0 {
		return a.Cardinality() == 0
	}
	return a.Equal(b)
}

func cloneSet(in mapset.Set[string]) mapset.Set[string] {
	if in == nil || in.Cardinality() == 0 {
		return mapset.NewSet[string]()
	}
	out := mapset.NewSet[string]()
	for _, key := range in.ToSlice() {
		out.Add(key)
	}
	return out
}

func readLastLogKey(t testing.TB, db util.DbRoot, set *Set) fdb.Key {
	t.Helper()
	var key fdb.Key
	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		begin, end := set.logSubspace.FDBRangeKeys()
		kvs, err := tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{
			Limit:   1,
			Reverse: true,
		}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(kvs) > 0 {
			key = kvs[0].Key
		}
		return nil, nil
	})
	require.NoError(t, err)
	return key
}

func readCursor(t testing.TB, db util.DbRoot, set *Set, id string) (fdb.Key, time.Time) {
	t.Helper()
	var tail fdb.Key
	var lease time.Time
	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		tail = tx.Get(set.cursorKey(id, cursorKeyTail)).MustGet()
		raw := tx.Get(set.cursorKey(id, cursorKeyLease)).MustGet()
		var ok bool
		lease, ok = decodeLease(raw)
		if !ok {
			return nil, fmt.Errorf("invalid lease value")
		}
		return nil, nil
	})
	require.NoError(t, err)
	return tail, lease
}

func readLogEntries(t testing.TB, db util.DbRoot, set *Set) []KeyedLogEntry {
	t.Helper()
	begin, _ := set.logSubspace.FDBRangeKeys()
	entries, err := set.readLog(context.Background(), begin, nil)
	require.NoError(t, err)
	return entries
}

func waitForCursorTail(t *testing.T, db util.DbRoot, set *Set, id string, expected fdb.Key) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		tail, _ := readCursor(t, db, set, id)
		if bytes.Equal(tail, expected) {
			return
		}
		if time.Now().After(deadline) {
			require.Equal(t, expected, tail)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
