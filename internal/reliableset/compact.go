package reliableset

import (
	"context"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

const (
	compactionInterval = 30 * time.Second
)

func (s *Set) runCompactionLoop() error {
	ticker := time.NewTicker(compactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.compactionContext.Done():
			return nil
		case <-ticker.C:
			_, err := s.db.Transact(func(tx fdb.Transaction) (any, error) {
				return nil, s.compactLog(tx)
			})
			if err != nil {
				// TODO: handle this logging more gracefully
				fmt.Printf("failed to compact log: %v\n", err)
				continue
			}
		}
	}
}

// compactLog compacts the current log into the snapshot.
func (s *Set) compactLog(tx fdb.Transaction) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.compactionLock.Acquire(ctx); err != nil {
		return fmt.Errorf("failed to acquire compaction lock: %w", err)
	}
	defer s.compactionLock.Release()

	begin, end := s.logSubspace.FDBRangeKeys()
	clearEnd := end
	now := time.Now()
	index, err := s.cursorIndex(tx)
	if err != nil {
		return err
	}
	if err := cleanDeadCursors(tx, index, now); err != nil {
		return err
	}
	active := activeCursors(index, now)

	if minTail, ok := minActiveTail(active); ok {
		if len(minTail) == 0 {
			return fmt.Errorf("expected min active tail to be non-empty, but got empty")
		}
		clearEnd = dbutil.KeyAfter(minTail)
	}
	compactionRange := fdb.KeyRange{Begin: begin, End: clearEnd}
	logEntries, err := tx.GetRange(compactionRange, fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		return err
	} else if len(logEntries) == 0 {
		return nil
	}
	for _, logEntry := range logEntries {
		var entry LogEntry
		err := entry.UnmarshalBinary(logEntry.Value)
		if err != nil {
			return fmt.Errorf("failed to unmarshal log entry: %w", err)
		}
		switch entry.Op {
		case LogOperationAdd:
			tx.Set(s.snapshotSubspace.Pack(tuple.Tuple{entry.Value}), entry.Value)
		case LogOperationRemove:
			tx.Clear(s.snapshotSubspace.Pack(tuple.Tuple{entry.Value}))
		default:
			return fmt.Errorf("invalid log operation: %d", entry.Op)
		}
	}
	tx.ClearRange(compactionRange)
	return nil
}
