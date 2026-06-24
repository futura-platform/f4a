package reliableset

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

type LogOperation byte

const (
	LogOperationAdd LogOperation = iota
	LogOperationRemove
)

const (
	entrySizeLimit   = 1024
	AddOverheadBytes = 39
)

var (
	ErrEntryTooLarge = fmt.Errorf("entry is too large")
)

// Add adds a value to the set. This is gauranteed to be contention free.
func (s *Set) Add(tx fdb.Transaction, value []byte) error {
	if len(value) > entrySizeLimit {
		return fmt.Errorf("%w: %d > %d", ErrEntryTooLarge, len(value), entrySizeLimit)
	}
	return s.writeLog(tx, LogEntry{Op: LogOperationAdd, Value: value})
}

// Remove removes a value from the set. This is gauranteed to be contention free.
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
func (s *Set) writeLog(tx fdb.Transaction, entry LogEntry) error {
	logKey, err := s.logSubspace.PackWithVersionstamp(
		dbutil.IncompleteGloballyOrderedVersionstamp(),
	)
	if err != nil {
		return err
	}
	entryBytes, err := entry.MarshalBinary()
	if err != nil {
		return err
	}
	tx.SetVersionstampedKey(logKey, entryBytes)
	dbutil.AtomicIncrement(tx, s.epochKey, 1)
	return nil
}

type KeyedLogEntry struct {
	key   fdb.KeyConvertible
	entry LogEntry
}

// readLog reads the log entries from the log subspace starting at (but not including) the given key.
// It returns the log entries in the order they were written.
func (s *Set) readLog(ctx context.Context, tr fdb.ReadTransactor, begin fdb.KeyConvertible) ([]KeyedLogEntry, error) {
	_, end := s.logSubspace.FDBRangeKeys()
	start := begin
	key := begin.FDBKey()
	if len(key) > 0 {
		start = dbutil.KeyAfter(key)
	}
	entries := make([]KeyedLogEntry, 0)
	for kvOrErr := range dbutil.UnboundedIterate(ctx, tr, fdb.KeyRange{Begin: start, End: end}, 256) {
		if err, ok := kvOrErr.Left(); ok {
			return nil, err
		}
		kv := kvOrErr.MustRight()
		var entry LogEntry
		err := entry.UnmarshalBinary(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal log entry: %w", err)
		}
		entries = append(entries, KeyedLogEntry{key: kv.Key, entry: entry})
	}
	return entries, nil
}
