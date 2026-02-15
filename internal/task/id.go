package task

import (
	"github.com/google/uuid"
)

// note: in order to spread the load evenly on FDB, this should NOT be monotonically increasing.
type Id string

func NewId() Id {
	return Id(uuid.New().String())
}
