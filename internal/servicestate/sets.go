package servicestate

import (
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/util"
)

func CreateOrOpenReadySet(dbRoot util.DbRoot) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(dbRoot, []string{"ready"})
}

func CreateOrOpenSuspendedSet(dbRoot util.DbRoot) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(dbRoot, []string{"suspended"})
}
