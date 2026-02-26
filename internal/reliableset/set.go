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

	closeOnce sync.Once
	closeErr  error
}
type setDirectories struct {
	rootSubspace     directory.DirectorySubspace
	snapshotSubspace directory.DirectorySubspace
	logSubspace      directory.DirectorySubspace
	cursorSubspace   directory.DirectorySubspace
	metadataSubspace directory.DirectorySubspace
}

func newSetDirectories[T fdb.ReadTransaction](
	tx T,
	path []string,
	directoryConstructor func(t T, path []string, layer []byte) (directory.DirectorySubspace, error),
) (d setDirectories, err error) {
	d.rootSubspace, err = directoryConstructor(tx, path, nil)
	if err != nil {
		return d, fmt.Errorf("failed to open root set directory: %w", err)
	}
	d.snapshotSubspace, err = directoryConstructor(tx, append(path, "snapshot"), nil)
	if err != nil {
		return d, fmt.Errorf("failed to create snapshot subspace: %w", err)
	}
	d.logSubspace, err = directoryConstructor(tx, append(path, "log"), nil)
	if err != nil {
		return d, fmt.Errorf("failed to create log subspace: %w", err)
	}
	d.cursorSubspace, err = directoryConstructor(tx, append(path, "cursor"), nil)
	if err != nil {
		return d, fmt.Errorf("failed to create cursor subspace: %w", err)
	}
	d.metadataSubspace, err = directoryConstructor(tx, append(path, "metadata"), nil)
	if err != nil {
		return d, fmt.Errorf("failed to create metadata subspace: %w", err)
	}
	return d, nil
}

func constructWith(
	db fdb.Transactor,
	path []string,
	directoryConstructor func(t fdb.Transaction, path []string, layer []byte) (directory.DirectorySubspace, error),
) (*Set, error) {
	var s *Set
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		dirs, err := newSetDirectories(tx, path, directoryConstructor)
		if err != nil {
			return nil, fmt.Errorf("failed to create directories: %w", err)
		}
		s = &Set{
			db:             tx.GetDatabase(),
			setDirectories: dirs,
		}
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create set: %w", err)
	}
	s.initRuntime()
	return s, nil
}

func Create(db dbutil.DbRoot, path []string) (*Set, error) {
	return constructWith(db, path, func(t fdb.Transaction, path []string, layer []byte) (directory.DirectorySubspace, error) {
		return db.Root.Create(t, path, layer)
	})
}

func Open(db dbutil.DbRoot, path []string) (*Set, error) {
	return constructWith(db, path, func(t fdb.Transaction, path []string, layer []byte) (directory.DirectorySubspace, error) {
		return db.Root.Open(t, path, layer)
	})
}

func CreateOrOpen(db dbutil.DbRoot, path []string) (*Set, error) {
	return constructWith(db, path, func(t fdb.Transaction, path []string, layer []byte) (directory.DirectorySubspace, error) {
		return db.Root.CreateOrOpen(t, path, layer)
	})
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

// Close stops background runtime and removes this set directory recursively.
// It is idempotent.
func (s *Set) Close() error {
	s.closeOnce.Do(func() {
		s.compactionCancel()
		<-s.compactionDone

		removed, err := s.rootSubspace.Remove(s.db, []string{})
		if err != nil {
			s.closeErr = fmt.Errorf("failed to remove set directory: %w", err)
			return
		}
		if !removed {
			s.closeErr = fmt.Errorf("failed to remove set directory: not found")
		}
	})
	return s.closeErr
}
