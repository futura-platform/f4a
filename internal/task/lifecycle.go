package task

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/pkg/util/db"
)

type LifecycleStatus byte

const (
	LifecycleStatusPending LifecycleStatus = iota
	LifecycleStatusSuspended
	LifecycleStatusRunning
)

func (s LifecycleStatus) String() string {
	switch s {
	case LifecycleStatusPending:
		return "pending"
	case LifecycleStatusRunning:
		return "running"
	case LifecycleStatusSuspended:
		return "suspended"
	}
	return "unknown"
}

type lifecycleStatusSerializer struct{}

// Marshal implements dbutil.serializable.
func (l lifecycleStatusSerializer) Marshal(v LifecycleStatus) []byte {
	return []byte{byte(v)}
}

// Unmarshal implements dbutil.serializable.
func (l lifecycleStatusSerializer) Unmarshal(bytes []byte) (LifecycleStatus, error) {
	if len(bytes) != 1 {
		return 0, fmt.Errorf("invalid lifecycle status: %v", bytes)
	}
	s := bytes[0]
	if s > byte(LifecycleStatusRunning) {
		return 0, fmt.Errorf("invalid lifecycle status: %d", s)
	}
	return LifecycleStatus(s), nil
}

func (k TaskKey) LifecycleStatus() dbutil.TypedKey[LifecycleStatus] {
	return dbutil.NewTypedKey(
		k.d.Pack(tuple.Tuple{k.id.Bytes(), "lifecycle_status"}),
		lifecycleStatusSerializer{},
	)
}
