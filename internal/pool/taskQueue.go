package pool

import (
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func taskSetPath(runnerId string) []string {
	return []string{"task_queue", runnerId}
}

func OpenTaskSetForRunner(db dbutil.DbRoot, runnerId string) (*reliableset.Set, error) {
	return reliableset.Open(db, taskSetPath(runnerId))
}

func CreateOrOpenTaskSetForRunner(db dbutil.DbRoot, runnerId string) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(db, taskSetPath(runnerId))
}
