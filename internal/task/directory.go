package task

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/pkg/util"
)

type TasksDirectory struct{ d directory.DirectorySubspace }

func CreateOrOpenTasksDirectory(db util.DbRoot) (TasksDirectory, error) {
	dir, err := db.Root.CreateOrOpen(db, []string{"tasks"}, nil)
	return TasksDirectory{dir}, err
}

func (d TasksDirectory) Create(db fdb.Transactor, id Id) (TaskKey, error) {
	path := []string{id.String()}
	task, err := d.d.Create(db, path, nil)
	if err != nil {
		return TaskKey{}, err
	}
	return TaskKey{
		d: task,
		clearFunc: func(t fdb.Transaction) error {
			_, err := d.d.Remove(t, path)
			return err
		},
		id: id,
	}, nil
}

func (d TasksDirectory) Open(db fdb.Transactor, id Id) (TaskKey, error) {
	path := []string{id.String()}
	task, err := d.d.Open(db, path, nil)
	if err != nil {
		return TaskKey{}, err
	}
	return TaskKey{
		d: task,
		clearFunc: func(t fdb.Transaction) error {
			_, err := d.d.Remove(t, path)
			return err
		},
		id: id,
	}, nil
}

type TaskKey struct {
	d         directory.DirectorySubspace
	clearFunc func(t fdb.Transaction) error

	id Id
}

func (k TaskKey) Id() Id {
	return k.id
}

func (k TaskKey) Clear(t fdb.Transaction) error {
	return k.clearFunc(t)
}
