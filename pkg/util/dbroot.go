package util

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

type DbRoot struct {
	fdb.Database
	Root directory.DirectorySubspace
}

func CreateOrOpenDbRoot(db fdb.Database, path []string) (DbRoot, error) {
	root, err := directory.CreateOrOpen(db, path, nil)
	if err != nil {
		return DbRoot{}, err
	}
	return DbRoot{
		Database: db,
		Root:     root,
	}, nil
}
