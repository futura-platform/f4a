package reliableset

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/pkg/util/db"
)

type LogOperation byte

const (
	LogOperationAdd LogOperation = iota
	LogOperationRemove
)

var (
	ErrEntryTooLarge = fmt.Errorf("entry is too large")
)

// Add adds a value to the set. This is gauranteed to be contention free.
// (except for rare versionstamp collisions).
func (s *Set) Add(tx fdb.Transaction, value []byte) error {
	if len(value) > s.maxItemSize {
		return fmt.Errorf("%w: %d > %d", ErrEntryTooLarge, len(value), s.maxItemSize)
	}
	return s.writeLog(tx, LogEntry{Op: LogOperationAdd, Value: value})
}

// Remove removes a value from the set. This is gauranteed to be contention free.
// (except for rare versionstamp collisions).
func (s *Set) Remove(tx fdb.Transaction, value []byte) error {
	if len(value) > s.maxItemSize {
		return fmt.Errorf("%w: %d > %d", ErrEntryTooLarge, len(value), s.maxItemSize)
	}
	return s.writeLog(tx, LogEntry{Op: LogOperationRemove, Value: value})
}

type LogEntry struct {
	Op    LogOperation
	Value []byte
}

func (e LogEntry) MarshalBinary() ([]byte, error) {
	return append([]byte{byte(e.Op)}, e.Value...), nil
}

func (e *LogEntry) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("log entry is too short")
	}
	e.Op = LogOperation(data[0])
	e.Value = data[1:]
	return nil
}

// writeLog writes a log entry to the set. It is gauranteed to be contention free
// (except for rare versionstamp collisions).
func (s *Set) writeLog(tx fdb.Transaction, entry LogEntry) error {
	logKey, err := s.logSubspace.PackWithVersionstamp(tuple.Tuple{
		tuple.IncompleteVersionstamp(0),
		atomic.AddUint64(&s.logCounter, 1),
	})
	if err != nil {
		return err
	}
	entryBytes, err := entry.MarshalBinary()
	if err != nil {
		return err
	}
	tx.SetVersionstampedKey(logKey, entryBytes)
	dbutil.AtomicIncrement(tx, s.epochKey)
	return nil
}

type KeyedLogEntry struct {
	key   fdb.KeyConvertible
	entry LogEntry
}

// readLog reads the log entries from the log subspace starting at (but not including) the given key.
// It returns the log entries in the order they were written.
func (s *Set) readLog(ctx context.Context, begin fdb.KeyConvertible, readVersion *int64) ([]KeyedLogEntry, error) {
	_, end := s.logSubspace.FDBRangeKeys()
	start := begin
	key := begin.FDBKey()
	if len(key) > 0 {
		start = dbutil.KeyAfter(key)
	}
	opts := dbutil.IterateUnboundedOptions{
		BatchSize:   s.logBatchSize(),
		ReadVersion: readVersion,
		Snapshot:    true,
	}
	entries := make([]KeyedLogEntry, 0)
	var entryIdx int
	for chunk, err := range dbutil.IterateUnbounded(ctx, s.t, fdb.KeyRange{Begin: start, End: end}, opts) {
		if err != nil {
			return nil, fmt.Errorf("failed to iterate log range: %w", err)
		}
		for _, e := range chunk {
			var entry LogEntry
			if err := entry.UnmarshalBinary(e.Value); err != nil {
				return nil, fmt.Errorf("failed to unmarshal log entry[%d]: %w", entryIdx, err)
			}
			entries = append(entries, KeyedLogEntry{key: e.Key, entry: entry})
			entryIdx++
		}
	}
	return entries, nil
}

func (s *Set) logTail(tx fdb.ReadTransaction) (fdb.Key, error) {
	begin, end := s.logSubspace.FDBRangeKeys()
	kvs, err := tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{
		Limit:   1,
		Reverse: true,
	}).GetSliceWithError()
	if err != nil {
		return nil, err
	} else if len(kvs) == 0 {
		return begin.FDBKey(), nil
	}
	return kvs[0].Key, nil
}
