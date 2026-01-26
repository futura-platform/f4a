package reliableset

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliablelock"
)

// Set is a log-structured set built on FoundationDB.
// It is gauranteed to be contention free on write operations
// (unless versiontimestamp collisions occur across FDB shards).
//
// It is optimized for high throughput mutations, and realtime consumption (with an in memory snapshot).
// It is NOT optimized for consumption without an in memory snapshot.
type Set struct {
	t fdb.Transactor

	// maxItemSize caps the size of values stored in the log.
	// It also drives batch sizing for unbounded reads.
	maxItemSize int

	// this key should be incremented for every new log entry
	epochKey fdb.Key

	snapshotSubspace directory.DirectorySubspace
	logSubspace      directory.DirectorySubspace
	cursorSubspace   directory.DirectorySubspace

	// enqueueCounter disambiguates versionstamp keys within a transaction.
	logCounter uint64

	consumerID   string
	consumerHint string

	compactionContext context.Context
	compactionCancel  context.CancelFunc
	compactionLock    *reliablelock.Lock[string]
}

const (
	targetIterateBatchBytes    = 4 * 1024 * 1024
	compactionTargetBatchBytes = 512 * 1024
	logEntryOverheadBytes      = 64
	snapshotEntryOverhead      = 128
	cursorEntryBytesEstimate   = 256
)

func CreateOrOpen(t fdb.Transactor, path []string, maxItemSize int) (*Set, error) {
	if maxItemSize <= 0 {
		return nil, fmt.Errorf("max item size must be greater than 0")
	}
	setSubspace, err := directory.CreateOrOpen(t, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open set: %w", err)
	}
	snapshotSubspace, err := setSubspace.CreateOrOpen(t, []string{"snapshot"}, nil)
	if err != nil {
		return nil, err
	}
	logSubspace, err := setSubspace.CreateOrOpen(t, []string{"log"}, nil)
	if err != nil {
		return nil, err
	}
	cursorSubspace, err := setSubspace.CreateOrOpen(t, []string{"cursor"}, nil)
	if err != nil {
		return nil, err
	}
	metadataSubspace, err := setSubspace.CreateOrOpen(t, []string{"metadata"}, nil)
	if err != nil {
		return nil, err
	}
	consumerID, consumerHint := newConsumerID()
	compactionContext, compactionCancel := context.WithCancel(context.Background())
	s := &Set{
		t:                 t,
		maxItemSize:       maxItemSize,
		epochKey:          metadataSubspace.Pack(tuple.Tuple{"epoch"}),
		snapshotSubspace:  snapshotSubspace,
		logSubspace:       logSubspace,
		cursorSubspace:    cursorSubspace,
		consumerID:        consumerID,
		consumerHint:      consumerHint,
		compactionContext: compactionContext,
		compactionCancel:  compactionCancel,
	}
	s.compactionLock = reliablelock.NewLock[string](
		s.t,
		metadataSubspace.Pack(tuple.Tuple{"compactionLock"}),
		s.consumerID,
		reliablelock.WithLeaseDuration(2*compactionInterval),
		reliablelock.WithRefreshInterval(compactionInterval/2),
	)
	go s.runCompactionLoop()
	return s, nil
}

func (s *Set) Items(ctx context.Context) (items mapset.Set[string], tail fdb.KeyConvertible, err error) {
	readVersion, err := s.currentReadVersion(ctx)
	if err != nil {
		return nil, nil, err
	}
	return s.itemsAtReadVersion(ctx, readVersion)
}

func (s *Set) itemsAtReadVersion(ctx context.Context, readVersion int64) (items mapset.Set[string], tail fdb.KeyConvertible, err error) {
	snapshot, err := s.snapshot(ctx, &readVersion)
	if err != nil {
		return nil, nil, err
	}
	begin, _ := s.logSubspace.FDBRangeKeys()
	logEntries, err := s.readLog(ctx, begin, &readVersion)
	if err != nil {
		return nil, nil, err
	}
	tail = begin
	for _, l := range logEntries {
		switch l.entry.Op {
		case LogOperationAdd:
			snapshot.Add(string(l.entry.Value))
		case LogOperationRemove:
			snapshot.Remove(string(l.entry.Value))
		default:
			return nil, nil, fmt.Errorf("unknown log operation: %d", l.entry.Op)
		}
		tail = l.key
	}
	return snapshot, tail, nil
}

func (s *Set) batchSizeForItemBytes(itemBytes int) int {
	return s.batchSizeForTarget(itemBytes, targetIterateBatchBytes)
}

func (s *Set) batchSizeForTarget(itemBytes int, targetBytes int) int {
	batch := targetBytes / itemBytes
	if batch < 1 {
		return 1
	}
	return batch
}

func (s *Set) logBatchSize() int {
	return s.batchSizeForItemBytes(s.maxItemSize + logEntryOverheadBytes)
}

func (s *Set) snapshotBatchSize() int {
	return s.batchSizeForItemBytes(s.maxItemSize*2 + snapshotEntryOverhead)
}

func (s *Set) cursorBatchSize() int {
	return s.batchSizeForItemBytes(cursorEntryBytesEstimate)
}

func (s *Set) compactionBatchSize() int {
	return s.batchSizeForTarget(
		s.maxItemSize*2+snapshotEntryOverhead+logEntryOverheadBytes,
		compactionTargetBatchBytes,
	)
}

func (s *Set) currentReadVersion(ctx context.Context) (int64, error) {
	var readVersion int64
	_, err := s.t.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		readVersion = tx.GetReadVersion().MustGet()
		return nil, nil
	})
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return readVersion, nil
}
