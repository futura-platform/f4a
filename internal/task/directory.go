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

func (d TasksDirectory) TaskKey(db fdb.Transactor, id Id) (TaskKey, error) {
	task, err := d.d.CreateOrOpen(db, []string{id.String()}, nil)
	if err != nil {
		return TaskKey{}, err
	}
	return TaskKey{d: task, id: id}, nil
}

type TaskKey struct {
	d  directory.DirectorySubspace
	id Id
}

func (k TaskKey) Id() Id {
	return k.id
}
