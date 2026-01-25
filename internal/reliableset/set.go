package reliableset

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	mapset "github.com/deckarep/golang-set/v2"
)

// Set is a log-structured set built on FoundationDB.
// It is gauranteed to be contention free on write operations
// (unless versiontimestamp collisions occur across FDB shards).
type Set struct {
	t fdb.Transactor

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
}

func CreateOrOpen(t fdb.Transactor, path []string) (*Set, error) {
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
		epochKey:          metadataSubspace.Pack(tuple.Tuple{"epoch"}),
		snapshotSubspace:  snapshotSubspace,
		logSubspace:       logSubspace,
		cursorSubspace:    cursorSubspace,
		consumerID:        consumerID,
		consumerHint:      consumerHint,
		compactionContext: compactionContext,
		compactionCancel:  compactionCancel,
	}
	go s.runCompactionLoop()
	return s, nil
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
		switch l.entry.op {
		case LogOperationAdd:
			snapshot.Add(string(l.entry.value))
		case LogOperationRemove:
			snapshot.Remove(string(l.entry.value))
		default:
			return nil, nil, fmt.Errorf("unknown log operation: %d", l.entry.op)
		}
		tail = l.key
	}
	return snapshot, tail, nil
}
