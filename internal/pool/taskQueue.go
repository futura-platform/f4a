package pool

import (
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/pkg/util"
)

const (
	taskQueueMaxItemSize = 1024
)

func CreateOrOpenTaskQueueForRunner(db util.DbRoot, runnerId string) (*reliableset.Set, error) {
	return reliableset.CreateOrOpen(db.Database, []string{"task_queue", runnerId}, taskQueueMaxItemSize)
}
