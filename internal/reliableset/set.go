package reliableset

import (
	"context"
	"fmt"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliablelock"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// Set is a log-structured set built on FoundationDB.
// It is gauranteed to be contention free on write operations
// (unless versiontimestamp collisions occur across FDB shards).
type Set struct {
	db fdb.Database

	// this key should be incremented for every new log entry
	epochKey fdb.Key

	setDirectories

	// enqueueCounter disambiguates versionstamp keys within a transaction.
	logCounter uint64

	consumerID   string
	consumerHint string

	compactionContext context.Context
	compactionCancel  context.CancelFunc
	compactionLock    *reliablelock.Lock[string]
	compactionDone    chan struct{}

	releaseOnce sync.Once
	clearOnce   sync.Once
	clearErr    error
	clearFunc   func() (bool, error)
}
type setDirectories struct {
	snapshotSubspace directory.DirectorySubspace
	logSubspace      directory.DirectorySubspace
	cursorSubspace   directory.DirectorySubspace
	metadataSubspace directory.DirectorySubspace
}

func newSetDirectories(
	path []string,
	directoryConstructor func(path []string) (directory.DirectorySubspace, error),
) (d setDirectories, err error) {
	d.snapshotSubspace, err = directoryConstructor(append(append([]string{}, path...), "snapshot"))
	if err != nil {
		return d, fmt.Errorf("failed to create snapshot subspace: %w", err)
	}
	d.logSubspace, err = directoryConstructor(append(append([]string{}, path...), "log"))
	if err != nil {
		return d, fmt.Errorf("failed to create log subspace: %w", err)
	}
	d.cursorSubspace, err = directoryConstructor(append(append([]string{}, path...), "cursor"))
	if err != nil {
		return d, fmt.Errorf("failed to create cursor subspace: %w", err)
	}
	d.metadataSubspace, err = directoryConstructor(append(append([]string{}, path...), "metadata"))
	if err != nil {
		return d, fmt.Errorf("failed to create metadata subspace: %w", err)
	}
	return d, nil
}

func constructWith(
	db dbutil.DbRoot,
	path []string,
	directoryConstructor func(path []string) (directory.DirectorySubspace, error),
	clearFunc func() (bool, error),
) (*Set, context.CancelFunc, error) {
	dirs, err := newSetDirectories(path, directoryConstructor)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create directories: %w", err)
	}
	s := &Set{
		db:             db.Database,
		setDirectories: dirs,
		clearFunc:      clearFunc,
	}
	s.initRuntime()
	return s, s.releaseRuntime, nil
}

func Create(db dbutil.DbRoot, path []string) (*Set, context.CancelFunc, error) {
	return constructWith(
		db,
		path,
		func(path []string) (directory.DirectorySubspace, error) {
			return db.Root.Create(db, path, nil)
		},
		func() (bool, error) {
			return db.Root.Remove(db, path)
		},
	)
}

func Open(db dbutil.DbRoot, path []string) (*Set, context.CancelFunc, error) {
	return constructWith(
		db,
		path,
		func(path []string) (directory.DirectorySubspace, error) {
			return db.Root.Open(db, path, nil)
		},
		func() (bool, error) {
			return db.Root.Remove(db, path)
		},
	)
}

func CreateOrOpen(db dbutil.DbRoot, path []string) (*Set, context.CancelFunc, error) {
	return constructWith(
		db,
		path,
		func(path []string) (directory.DirectorySubspace, error) {
			return db.Root.CreateOrOpen(db, path, nil)
		},
		func() (bool, error) {
			return db.Root.Remove(db, path)
		},
	)
}

func (s *Set) initRuntime() {
	s.epochKey = s.metadataSubspace.Pack(tuple.Tuple{"epoch"})
	s.consumerID, s.consumerHint = newConsumerID()
	s.compactionContext, s.compactionCancel = context.WithCancel(context.Background())
	s.compactionDone = make(chan struct{})
	s.compactionLock = reliablelock.NewLock[string](
		s.db,
		s.metadataSubspace.Pack(tuple.Tuple{"compactionLock"}),
		s.consumerID,
		reliablelock.WithLeaseDuration(2*compactionInterval),
		reliablelock.WithRefreshInterval(compactionInterval/2),
	)

	go func() {
		defer close(s.compactionDone)
		_ = s.runCompactionLoop()
	}()
}

func (s *Set) releaseRuntime() {
	s.releaseOnce.Do(func() {
		s.compactionCancel()
		<-s.compactionDone
	})
}

func (s *Set) Items(tx fdb.ReadTransaction) (items mapset.Set[string], tail fdb.KeyConvertible, err error) {
	snapshot, err := s.snapshot(tx)
	if err != nil {
		return nil, nil, err
	}
	begin, _ := s.logSubspace.FDBRangeKeys()
	logEntries, err := s.readLog(tx, begin)
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

// Clear stops background runtime and removes this set directory recursively.
// It is idempotent.
func (s *Set) Clear() error {
	s.clearOnce.Do(func() {
		s.releaseRuntime()
		removed, err := s.clearFunc()
		if err != nil {
			s.clearErr = fmt.Errorf("failed to remove set directory: %w", err)
			return
		}
		if !removed {
			// Already removed; treat as idempotent success.
			return
		}
	})
	return s.clearErr
}
