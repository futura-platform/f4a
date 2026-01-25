package task

import (
	"fmt"

	"github.com/google/uuid"
)

// note: in order to spread the load evenly on FDB, this should NOT be monotonically increasing.
type Id struct{ uuid.UUID }

func NewId() Id {
	return Id{uuid.New()}
}

func (id Id) String() string {
	return id.UUID.String()
}

func IdFromBytes(b []byte) (Id, error) {
	if len(b) != 16 {
		return Id{}, fmt.Errorf("invalid UUID length: %d", len(b))
	}
	var uuid uuid.UUID
	copy(uuid[:], b)
	return Id{uuid}, nil
}

func (id Id) Bytes() []byte {
	return id.UUID[:]
}

func IdFromString(s string) (Id, error) {
	uuid, err := uuid.Parse(s)
	if err != nil {
		return Id{}, err
	}
	return Id{uuid}, nil
}
