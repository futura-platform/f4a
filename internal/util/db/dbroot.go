package dbutil

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/pkg/constants"
)

type DbRoot struct {
	fdb.Database
	Root directory.DirectorySubspace
}

func CreateOrOpenDbRoot(path []string, openDb func() (fdb.Database, error)) (DbRoot, error) {
	err := fdb.APIVersion(constants.FDB_API_VERSION)
	if err != nil {
		return DbRoot{}, err
	}
	db, err := openDb()
	if err != nil {
		return DbRoot{}, err
	}
	root, err := directory.CreateOrOpen(db, path, nil)
	if err != nil {
		return DbRoot{}, err
	}
	return DbRoot{
		Database: db,
		Root:     root,
	}, nil
}

func CreateOrOpenDefaultDbRoot() (DbRoot, error) {
	return CreateOrOpenDbRoot([]string{"f4a"}, fdb.OpenDefault)
}
