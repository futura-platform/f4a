package servicestate

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func createOrOpenReadySet(tr fdb.Transactor, db dbutil.DbRoot) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(tr, db, []string{"ready"})
}

func createOrOpenSuspendedSet(tr fdb.Transactor, db dbutil.DbRoot) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(tr, db, []string{"suspended"})
}
