package servicestate

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func CreateOrOpenReadySet(tr fdb.Transactor, db dbutil.DbRoot) (*reliableset.Set, context.CancelFunc, error) {
	return reliableset.CreateOrOpen(tr, db, []string{"ready"})
}

func CreateOrOpenSuspendedSet(tr fdb.Transactor, db dbutil.DbRoot) (*reliableset.Set, context.CancelFunc, error) {
	return reliableset.CreateOrOpen(tr, db, []string{"suspended"})
}
