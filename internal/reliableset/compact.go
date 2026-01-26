package reliableset

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/f4a/internal/reliablelock"
	dbutil "github.com/futura-platform/f4a/pkg/util/db"
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
			err := s.compactLog(s.compactionContext)
			if err != nil {
				// TODO: handle this logging more gracefully
				fmt.Printf("failed to compact log: %v\n", err)
				continue
			}
		}
	}
}

// compactLog compacts the current log into the snapshot.
func (s *Set) compactLog(ctx context.Context) error {
	if err := s.compactionLock.Acquire(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, reliablelock.ErrNotOwner) || errors.Is(err, reliablelock.ErrNotHeld) {
			// theres no need to compact the log if someone else is already doing it
			return nil
		}
		return err
	}
	defer func() {
		_ = s.compactionLock.Release()
	}()

	readVersion, err := s.currentReadVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current read version: %w", err)
	}
	now := time.Now()
	index, err := s.cursorIndex(ctx, &readVersion)
	if err != nil {
		return fmt.Errorf("failed to get cursor index: %w", err)
	}
	return s.compactLogWithIndex(ctx, index, now, readVersion)
}

func (s *Set) compactLogWithIndex(ctx context.Context, index cursorIndex, now time.Time, readVersion int64) error {
	begin, end := s.logSubspace.FDBRangeKeys()
	clearEnd := end
	active := activeCursors(index, now)

	if minTail, ok := minActiveTail(active); ok {
		if len(minTail) == 0 {
			return fmt.Errorf("expected min active tail to be non-empty, but got empty")
		}
		clearEnd = dbutil.KeyAfter(minTail)
	}
	_, err := s.t.Transact(func(tx fdb.Transaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, s.cleanDeadCursors(tx, index, now)
	})
	if err != nil {
		return err
	}

	opts := dbutil.IterateUnboundedOptions{
		BatchSize:   s.compactionBatchSize(),
		ReadVersion: &readVersion,
		Snapshot:    true,
	}
	compactionRange := fdb.KeyRange{Begin: begin, End: clearEnd}
	entryIdx := 0
	for chunk, err := range dbutil.IterateUnbounded(ctx, s.t, compactionRange, opts) {
		if err != nil {
			return err
		}
		if len(chunk) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		entries := make([]LogEntry, len(chunk))
		for i, logEntry := range chunk {
			var entry LogEntry
			if err := entry.UnmarshalBinary(logEntry.Value); err != nil {
				return fmt.Errorf("failed to unmarshal log entry[%d]: %w", entryIdx, err)
			}
			entries[i] = entry
			entryIdx++
		}
		firstKey := chunk[0].Key
		lastKey := chunk[len(chunk)-1].Key
		_, err = s.t.Transact(func(tx fdb.Transaction) (any, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for _, entry := range entries {
				switch entry.Op {
				case LogOperationAdd:
					tx.Set(s.snapshotSubspace.Pack(tuple.Tuple{entry.Value}), entry.Value)
				case LogOperationRemove:
					tx.Clear(s.snapshotSubspace.Pack(tuple.Tuple{entry.Value}))
				default:
					return nil, fmt.Errorf("invalid log operation: %d", entry.Op)
				}
			}
			tx.ClearRange(fdb.KeyRange{Begin: firstKey, End: dbutil.KeyAfter(lastKey)})
			return nil, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
