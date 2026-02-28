package reliableset

import (
	"fmt"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	mapset "github.com/deckarep/golang-set/v2"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// Set is a log-structured set built on FoundationDB.
// It is gauranteed to be contention free on write operations
// (unless versiontimestamp collisions occur across FDB shards).
type Set struct {
	db dbutil.DbRoot

	// this key should be incremented for every new log entry
	epochKey fdb.Key

	setDirectories

	// enqueueCounter disambiguates versionstamp keys within a transaction.
	logCounter uint64

	consumerID   string
	consumerHint string

	compactor *setCompactor

	clearLock sync.Mutex
	clearOnce sync.Once
	clearFunc func() (bool, error)
}
type setDirectories struct {
	snapshotSubspace directory.DirectorySubspace
	logSubspace      directory.DirectorySubspace
	cursorSubspace   directory.DirectorySubspace
	metadataSubspace directory.DirectorySubspace
}

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
	d.metadataSubspace, err = directoryConstructor(tr, append(append([]string{}, path...), "metadata"))
	if err != nil {
		return d, fmt.Errorf("failed to create metadata subspace: %w", err)
	}
	return d, err
}

func constructWith[T fdb.ReadTransactor](
	db dbutil.DbRoot,
	tr T,
	path []string,
	directoryConstructor func(tr T, path []string) (directory.DirectorySubspace, error),
	clearFunc func() (bool, error),
) (*Set, error) {
	dirs, err := newSetDirectories(tr, path, directoryConstructor)
	if err != nil {
		return nil, fmt.Errorf("failed to create directories: %w", err)
	}
	consumerID, consumerHint := newConsumerID()
	s := &Set{
		db:             db,
		epochKey:       dirs.metadataSubspace.Pack(tuple.Tuple{"epoch"}),
		setDirectories: dirs,
		consumerID:     consumerID,
		consumerHint:   consumerHint,
		clearFunc:      clearFunc,
	}
	s.compactor = newSetCompactor(s)
	return s, nil
}

func Create(tr fdb.Transactor, db dbutil.DbRoot, path []string) (*Set, error) {
	var set *Set
	_, err := tr.Transact(func(t fdb.Transaction) (any, error) {
		var err error
		set, err = constructWith(
			db,
			t,
			path,
			func(tr fdb.Transaction, path []string) (directory.DirectorySubspace, error) {
				return db.Root.Create(tr, path, nil)
			},
			func() (bool, error) {
				return db.Root.Remove(db, path)
			},
		)
		return nil, err
	})
	return set, err
}

func Open(tr fdb.ReadTransactor, db dbutil.DbRoot, path []string) (*Set, error) {
	var set *Set
	_, err := tr.ReadTransact(func(t fdb.ReadTransaction) (any, error) {
		var err error
		set, err = constructWith(
			db,
			t,
			path,
			func(tr fdb.ReadTransaction, path []string) (directory.DirectorySubspace, error) {
				return db.Root.Open(tr, path, nil)
			},
			func() (bool, error) {
				return db.Root.Remove(db, path)
			},
		)
		return nil, err
	})
	return set, err
}

func CreateOrOpen(tr fdb.Transactor, db dbutil.DbRoot, path []string) (*Set, error) {
	var set *Set
	_, err := tr.Transact(func(t fdb.Transaction) (any, error) {
		var err error
		set, err = constructWith(
			db,
			t,
			path,
			func(tr fdb.Transaction, path []string) (directory.DirectorySubspace, error) {
				return db.Root.CreateOrOpen(tr, path, nil)
			},
			func() (bool, error) {
				return db.Root.Remove(db, path)
			},
		)
		return nil, err
	})
	return set, err
}

func (s *Set) RunCompactor() (cancel func()) {
	return s.compactor.Run()
}

func (s *Set) releaseRuntime() {
	s.compactor.release()
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
	s.clearLock.Lock()
	defer s.clearLock.Unlock()

	var clearErr error
	s.clearOnce.Do(func() {
		s.releaseRuntime()
		removed, err := s.clearFunc()
		if err != nil {
			clearErr = fmt.Errorf("failed to remove set directory: %w", err)
			return
		}
		if !removed {
			// Already removed; treat as idempotent success.
			return
		}
	})
	if clearErr != nil {
		// reset the clearOnce to allow future retries
		s.clearOnce = sync.Once{}
	}
	return clearErr
}
