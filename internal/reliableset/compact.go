package reliableset

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/constants"
)

const (
	compactionInterval = 30 * time.Second

	// Keep a headroom buffer for internal FDB accounting.
	compactionTxSafetyMarginBytes = constants.MaxTransactionAffectedSizeBytes / 8
	// Cursor scans and lock bookkeeping share this transaction.
	compactionFixedOverheadBytes = 64 * 1024
	// Approximate key bytes for a log record key.
	compactionLogKeyEstimateBytes = 256
	// Snapshot key packs the full value plus tuple/subspace metadata.
	compactionSnapshotKeyOverheadBytes = 256
)

func (c *setCompactor) runCompactionLoop() error {
	ticker := time.NewTicker(compactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.runCtx.Done():
			return nil
		case <-ticker.C:
			err := c.compactLog(c.runCtx, c.set.db)
			if err != nil {
				slog.Error("reliableset: failed to compact log", "error", err)
				continue
			}
		}
	}
}

// compactLog compacts the entire current log into the snapshot.
// It batches over multiple transactions to avoid exceeding the transaction size limit.
// Failures are partial, so the compaction will continue from the last successful chunk.
func (c *setCompactor) compactLog(ctx context.Context, db dbutil.DbRoot) error {
	c.ensureLock()
	if err := c.lock.Acquire(ctx); err != nil {
		return fmt.Errorf("failed to acquire compaction lock: %w", err)
	}
	defer c.lock.Release()

	for more := true; more; {
		_, err := db.TransactContext(ctx, func(tx fdb.Transaction) (_ any, err error) {
			more, err = c.compactLogChunk(tx)
			return nil, err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// compactLogChunk compacts a single chunk of the current log into the snapshot.
// It maximizes the number of log entries processed in a single transaction.
func (c *setCompactor) compactLogChunk(tx fdb.Transaction) (more bool, err error) {
	begin, end := c.set.logSubspace.FDBRangeKeys()
	clearEnd := end
	now := time.Now()
	index, err := c.set.makeCursorIndex(tx)
	if err != nil {
		return false, err
	}
	if err := cleanDeadCursors(tx, index, now); err != nil {
		return false, err
	}
	active := activeCursors(index, now)

	if minTail, ok := minActiveTail(active); ok {
		if len(minTail) == 0 {
			return false, fmt.Errorf("expected min active tail to be non-empty, but got empty")
		}
		// clear all items up to the min active tail
		// (we need to let that client(s) catch up before we can compact the log)
		clearEnd = dbutil.KeyAfter(minTail)
	}

	const maxTxAffectedBytes = constants.MaxTransactionAffectedSizeBytes - compactionTxSafetyMarginBytes
	const maxCompactionEntryBytes = ((entrySizeLimit + 1) + // read log value
		compactionLogKeyEstimateBytes + // read log key
		compactionLogKeyEstimateBytes + // clear log key
		(entrySizeLimit + compactionSnapshotKeyOverheadBytes) + // snapshot key
		entrySizeLimit) // snapshot value (for add)
	maxReadEntries := max((maxTxAffectedBytes-compactionFixedOverheadBytes)/maxCompactionEntryBytes, 1)
	compactionRange := fdb.KeyRange{Begin: begin, End: clearEnd}
	logEntries := tx.GetRange(compactionRange, fdb.RangeOptions{
		Limit: maxReadEntries + 1,
		Mode:  fdb.StreamingModeExact,
	}).GetSliceOrPanic()
	if len(logEntries) == 0 {
		return false, nil
	}

	readMore := len(logEntries) > maxReadEntries
	if readMore {
		logEntries = logEntries[:maxReadEntries]
	}

	usedBytes := compactionFixedOverheadBytes + estimateCursorAccountingBytes(index, now)
	processed := 0
	for _, logEntry := range logEntries {
		var entry LogEntry
		err := entry.UnmarshalBinary(logEntry.Value)
		if err != nil {
			return false, fmt.Errorf("failed to unmarshal log entry: %w", err)
		}
		snapshotKey := c.set.snapshotSubspace.Pack(tuple.Tuple{entry.Value})
		entryBytes, err := estimatedCompactionEntryBytes(logEntry, snapshotKey, entry.Op, entry.Value)
		if err != nil {
			return false, err
		}
		if processed > 0 && usedBytes+entryBytes > maxTxAffectedBytes {
			more = true
			break
		}
		switch entry.Op {
		case LogOperationAdd:
			tx.Set(snapshotKey, entry.Value)
		case LogOperationRemove:
			tx.Clear(snapshotKey)
		default:
			return false, fmt.Errorf("invalid log operation: %d", entry.Op)
		}
		usedBytes += entryBytes
		processed++
	}
	if processed == 0 {
		return false, fmt.Errorf("single compaction entry exceeds tx budget")
	}

	chunkRange := fdb.KeyRange{
		Begin: logEntries[0].Key,
		End:   dbutil.KeyAfter(logEntries[processed-1].Key),
	}
	tx.ClearRange(chunkRange)
	if !more {
		more = readMore || processed < len(logEntries)
	}
	return more, nil
}

func estimateCursorAccountingBytes(index cursorIndex, now time.Time) int {
	total := 0
	for _, keys := range index.keysByID {
		for _, key := range keys {
			total += len(key)
		}
	}
	for _, tail := range index.tails {
		total += len(tail)
	}
	total += len(index.leases) * 8
	for id, keys := range index.keysByID {
		lease, ok := index.leases[id]
		if ok && lease.After(now) {
			continue
		}
		for _, key := range keys {
			total += len(key)
		}
	}
	return total
}

func estimatedCompactionEntryBytes(logEntry fdb.KeyValue, snapshotKey []byte, op LogOperation, value []byte) (int, error) {
	// read: log key + log value, clear: cleared log key bytes
	total := len(logEntry.Key) + len(logEntry.Value) + len(logEntry.Key)
	switch op {
	case LogOperationAdd:
		// write: snapshot key + value
		total += len(snapshotKey) + len(value)
	case LogOperationRemove:
		// clear: snapshot key bytes
		total += len(snapshotKey)
	default:
		return 0, fmt.Errorf("invalid log operation: %d", op)
	}
	return total, nil
}
