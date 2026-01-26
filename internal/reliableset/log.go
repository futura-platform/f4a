package reliableset

import (
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

const (
	entrySizeLimit = 1024
)

var (
	ErrEntryTooLarge = fmt.Errorf("entry is too large")
)

// Add adds a value to the set. This is gauranteed to be contention free.
// (except for rare versionstamp collisions).
func (s *Set) Add(tx fdb.Transaction, value []byte) error {
	if len(value) > entrySizeLimit {
		return fmt.Errorf("%w: %d > %d", ErrEntryTooLarge, len(value), entrySizeLimit)
	}
	return s.writeLog(tx, LogEntry{Op: LogOperationAdd, Value: value})
}

// Remove removes a value from the set. This is gauranteed to be contention free.
// (except for rare versionstamp collisions).
func (s *Set) Remove(tx fdb.Transaction, value []byte) error {
	if len(value) > entrySizeLimit {
		return fmt.Errorf("%w: %d > %d", ErrEntryTooLarge, len(value), entrySizeLimit)
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
func (s *Set) readLog(tx fdb.ReadTransaction, begin fdb.KeyConvertible) ([]KeyedLogEntry, error) {
	_, end := s.logSubspace.FDBRangeKeys()
	start := begin
	key := begin.FDBKey()
	if len(key) > 0 {
		start = dbutil.KeyAfter(key)
	}
	logEntries, err := tx.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		return nil, fmt.Errorf("failed to get range: %w", err)
	}
	entries := make([]KeyedLogEntry, 0, len(logEntries))
	for i, e := range logEntries {
		var entry LogEntry
		err := entry.UnmarshalBinary(e.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal log entry[%d]: %w", i, err)
		}
		entries = append(entries, KeyedLogEntry{key: e.Key, entry: entry})
	}
	return entries, nil
}
