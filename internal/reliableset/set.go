package reliableset

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/cenkalti/backoff/v4"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliablelock"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// set is a log-structured set built on FoundationDB.
// It is gauranteed to be contention free on write operations
type set struct {
	db dbutil.DbRoot

	// human-readable identity (the directory path) for error messages
	name string

	// this key should be incremented for every new log entry
	epochKey fdb.Key
	// item count of the snapshot, maintained transactionally by compaction
	cardinalityKey fdb.Key

	setDirectories

	compactor *setCompactor

	clearFunc func(fdb.Transaction) (bool, error)
}
type setDirectories struct {
	snapshotSubspace       directory.DirectorySubspace
	logSubspace            directory.DirectorySubspace
	cursorSubspace         directory.DirectorySubspace
	metadataSubspace       directory.DirectorySubspace
	compactionLockSubspace directory.DirectorySubspace
}

const (
	metadataDirectory = "metadata"
	cardinalityField  = "cardinality"
)

func newSetDirectories[T fdb.ReadTransactor](
	tr T,
	path []string,
	directoryConstructor func(tr T, path []string) (directory.DirectorySubspace, error),
) (d setDirectories, err error) {
	d.snapshotSubspace, err = directoryConstructor(tr, append(append([]string{}, path...), "snapshot"))
	if err != nil {
		return d, fmt.Errorf("failed to create snapshot subspace: %w", err)
	}
	d.logSubspace, err = directoryConstructor(tr, append(append([]string{}, path...), "log"))
	if err != nil {
		return d, fmt.Errorf("failed to create log subspace: %w", err)
	}
	d.cursorSubspace, err = directoryConstructor(tr, append(append([]string{}, path...), "cursor"))
	if err != nil {
		return d, fmt.Errorf("failed to create cursor subspace: %w", err)
	}
	d.metadataSubspace, err = directoryConstructor(tr, append(append([]string{}, path...), metadataDirectory))
	if err != nil {
		return d, fmt.Errorf("failed to create metadata subspace: %w", err)
	}
	d.compactionLockSubspace, err = directoryConstructor(tr, append(append([]string{}, path...), "compaction_lock"))
	if err != nil {
		return d, fmt.Errorf("failed to create compaction lock subspace: %w", err)
	}
	return d, err
}

func constructWith[T fdb.ReadTransactor](
	db dbutil.DbRoot,
	tr T,
	path []string,
	directoryConstructor func(tr T, path []string) (directory.DirectorySubspace, error),
	clearFunc func(fdb.Transaction) (bool, error),
) (*set, error) {
	dirs, err := newSetDirectories(tr, path, directoryConstructor)
	if err != nil {
		return nil, fmt.Errorf("failed to create directories: %w", err)
	}
	s := &set{
		db:             db,
		name:           strings.Join(path, "/"),
		epochKey:       dirs.metadataSubspace.Pack(tuple.Tuple{"epoch"}),
		cardinalityKey: dirs.metadataSubspace.Pack(tuple.Tuple{cardinalityField}),
		setDirectories: dirs,
		clearFunc:      clearFunc,
	}
	s.compactor = newSetCompactor(s, dirs.compactionLockSubspace)
	return s, nil
}

func Create(tr fdb.Transactor, db dbutil.DbRoot, path []string) (*set, error) {
	var set *set
	_, err := tr.Transact(func(t fdb.Transaction) (any, error) {
		var err error
		set, err = constructWith(
			db,
			t,
			path,
			func(tr fdb.Transaction, path []string) (directory.DirectorySubspace, error) {
				return db.Root.Create(tr, path, nil)
			},
			func(t fdb.Transaction) (bool, error) {
				return db.Root.Remove(t, path)
			},
		)
		return nil, err
	})
	return set, err
}

func Open(tr fdb.ReadTransactor, db dbutil.DbRoot, path []string) (*set, error) {
	var set *set
	_, err := tr.ReadTransact(func(t fdb.ReadTransaction) (any, error) {
		var err error
		set, err = constructWith(
			db,
			t,
			path,
			func(tr fdb.ReadTransaction, path []string) (directory.DirectorySubspace, error) {
				return db.Root.Open(tr, path, nil)
			},
			func(t fdb.Transaction) (bool, error) {
				return db.Root.Remove(t, path)
			},
		)
		return nil, err
	})
	return set, err
}

func CreateOrOpen(tr fdb.Transactor, db dbutil.DbRoot, path []string) (*set, error) {
	var set *set
	_, err := tr.Transact(func(t fdb.Transaction) (any, error) {
		var err error
		set, err = constructWith(
			db,
			t,
			path,
			func(tr fdb.Transaction, path []string) (directory.DirectorySubspace, error) {
				return db.Root.CreateOrOpen(tr, path, nil)
			},
			func(t fdb.Transaction) (bool, error) {
				return db.Root.Remove(t, path)
			},
		)
		return nil, err
	})
	return set, err
}

func (s *set) RunCompactor() (cancel func()) {
	return s.compactor.Run()
}

func (s *set) releaseRuntime() {
	s.compactor.release()
}

func (s *set) Items(ctx context.Context, db fdb.Database) (
	items mapset.Set[string],
	tail fdb.KeyConvertible,
	err error,
) {
	items, tail, activeLease, err := s.leasedItems(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	return items, tail, activeLease.BestEffortRelease(ctx, backoff.WithMaxElapsedTime(10*time.Second))
}

func (s *set) leasedItems(ctx context.Context, db fdb.Database) (
	items mapset.Set[string],
	tail fdb.KeyConvertible,
	compactionLease *reliablelock.ActiveLease,
	err error,
) {
	// TODO: make compactor.lock use a RW lock so that this is not a bottleneck.
	l, err := s.compactor.lock.Acquire(ctx, db, reliablelock.DefaultLeaseOptions())
	if err != nil {
		return nil, nil, nil, err
	}
	activeLease, err := l.Activate(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if err != nil {
			activeLease.BestEffortRelease(ctx, backoff.WithMaxElapsedTime(10*time.Second))
		}
	}()

	snapshot, err := s.snapshot(ctx, db)
	if err != nil {
		return nil, nil, nil, err
	}
	begin, _ := s.logSubspace.FDBRangeKeys()
	logEntries, err := s.readLog(ctx, db, begin)
	if err != nil {
		return nil, nil, nil, err
	}
	tail = begin
	for _, l := range logEntries {
		switch l.entry.Op {
		case LogOperationAdd:
			snapshot.Add(string(l.entry.Value))
		case LogOperationRemove:
			snapshot.Remove(string(l.entry.Value))
		default:
			return nil, nil, nil, fmt.Errorf("unknown log operation: %d", l.entry.Op)
		}
		tail = l.key
	}
	return snapshot, tail, activeLease, nil
}

// Cardinality returns the number of items in the set as of the last completed
// compaction. Log entries that have not been compacted yet are not reflected,
// so the value is eventually consistent with Items.
func (s *set) Cardinality(t fdb.ReadTransaction) (int64, error) {
	return readCardinality(t, s.cardinalityKey)
}

// ReadCardinality resolves the current metadata directory without opening the rest of the set.
func ReadCardinality(t fdb.ReadTransaction, db dbutil.DbRoot, path []string) (int64, error) {
	metadata, err := db.Root.Open(t, append(append([]string{}, path...), metadataDirectory), nil)
	if err != nil {
		return 0, err
	}
	return readCardinality(t, metadata.Pack(tuple.Tuple{cardinalityField}))
}

func readCardinality(t fdb.ReadTransaction, key fdb.Key) (int64, error) {
	raw, err := t.Get(key).Get()
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid cardinality value length: %d", len(raw))
	}
	return int64(binary.LittleEndian.Uint64(raw)), nil
}

// Clear stops background runtime and removes this set directory recursively.
// It is idempotent.
func (s *set) Clear(tx fdb.Transaction) error {
	s.releaseRuntime()
	_, err := s.clearFunc(tx)
	if err != nil {
		return fmt.Errorf("failed to remove set directory: %w", err)
	}
	return nil
}
