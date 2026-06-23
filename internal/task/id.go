package task

import (
	"github.com/google/uuid"
)

// note: in order to spread the load evenly on FDB, this should NOT be monotonically increasing.
type Id string

func NewId() Id {
	return Id(uuid.New().String())
}

const (
	MAX_ID_LENGTH = 64
)

func (id Id) MarshalBinary() ([]byte, error) {
	return []byte(id), nil
}

func (id *Id) UnmarshalBinary(data []byte) error {
	*id = Id(string(data))
	return nil
}
