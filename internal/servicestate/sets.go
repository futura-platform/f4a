package servicestate

import (
	"context"

	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func CreateOrOpenReadySet(dbRoot dbutil.DbRoot) (*reliableset.Set, context.CancelFunc, error) {
	return reliableset.CreateOrOpen(dbRoot, []string{"ready"})
}

func CreateOrOpenSuspendedSet(dbRoot dbutil.DbRoot) (*reliableset.Set, context.CancelFunc, error) {
	return reliableset.CreateOrOpen(dbRoot, []string{"suspended"})
}
