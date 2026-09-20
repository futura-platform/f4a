package task

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

var (
	ErrNotFound      = errors.New("task does not exist")
	ErrAlreadyExists = errors.New("task already exists")
)

// TasksDirectory holds every task. A task is a subspace of it keyed by id, not a directory of its own: a
// directory takes its prefix from one allocator shared by the whole database and costs a chain of reads to
// open, while a subspace is computed locally.
type TasksDirectory struct{ d directory.DirectorySubspace }

func CreateOrOpenTasksDirectory(db dbutil.DbRoot) (TasksDirectory, error) {
	dir, err := db.Root.CreateOrOpen(db, []string{"tasks"}, nil)
	return TasksDirectory{dir}, err
}

// Create creates the task, or fails with ErrAlreadyExists.
func (d TasksDirectory) Create(db fdb.Transactor, id Id) (TaskKey, error) {
	k := TaskKey{d: d.d, id: id}
	_, err := db.Transact(func(t fdb.Transaction) (any, error) {
		created, err := t.Get(k.existenceKey()).Get()
		if err != nil {
			return nil, err
		}
		if created != nil {
			return nil, ErrAlreadyExists
		}
		t.Set(k.existenceKey(), []byte{})
		return nil, nil
	})
	if err != nil {
		return TaskKey{}, err
	}
	return k, nil
}

// Open opens the task, or fails with ErrNotFound.
func (d TasksDirectory) Open(db fdb.ReadTransactor, id Id) (TaskKey, error) {
	k := TaskKey{d: d.d, id: id}
	_, err := db.ReadTransact(func(t fdb.ReadTransaction) (any, error) {
		created, err := t.Get(k.existenceKey()).Get()
		if err != nil {
			return nil, err
		}
		if created == nil {
			return nil, ErrNotFound
		}
		return nil, nil
	})
	if err != nil {
		return TaskKey{}, err
	}
	return k, nil
}

// TaskKey addresses one task's keys in the tasks directory: its fields, packed as (id, field), and the
// subspaces of its settlement state and lock under (id).
type TaskKey struct {
	d  subspace.Subspace
	id Id
}

func (k TaskKey) Id() Id {
	return k.id
}

// keyspace is everything the task owns.
func (k TaskKey) keyspace() subspace.Subspace {
	return k.d.Sub(string(k.id))
}

// existenceField is set by Create and required by Open: the task's record, where its directory node was.
const existenceField = "exists"

func (k TaskKey) existenceKey() fdb.Key {
	return k.d.Pack(tuple.Tuple{string(k.id), existenceField})
}

// Clear removes the task and everything it owns.
func (k TaskKey) Clear(t fdb.Transaction) error {
	t.ClearRange(k.keyspace())
	return nil
}
