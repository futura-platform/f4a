package pool

import (
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/pkg/util"
)

func taskSetPath(runnerId string) []string {
	return []string{"task_queue", runnerId}
}

func OpenTaskSetForRunner(db util.DbRoot, runnerId string) (*reliableset.Set, error) {
	return reliableset.Open(db, taskSetPath(runnerId))
}

func CreateOrOpenTaskSetForRunner(db util.DbRoot, runnerId string) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(db, taskSetPath(runnerId))
}
