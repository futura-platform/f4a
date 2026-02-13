package servicestate

import (
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func CreateOrOpenReadySet(dbRoot dbutil.DbRoot) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(dbRoot, []string{"ready"})
}

func CreateOrOpenSuspendedSet(dbRoot dbutil.DbRoot) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(dbRoot, []string{"suspended"})
}
