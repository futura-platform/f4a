package task

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

type finishedAtSerializer struct{}

// Marshal implements dbutil.serializable.
func (s finishedAtSerializer) Marshal(v *time.Time) []byte {
	if v == nil {
		return nil
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v.UnixNano()))
	return buf
}

// Unmarshal implements dbutil.serializable.
func (s finishedAtSerializer) Unmarshal(bytes []byte) (*time.Time, error) {
	if bytes == nil {
		return nil, nil
	}
	if len(bytes) != 8 {
		return nil, fmt.Errorf("invalid finished_at: expected 8 bytes, got %d", len(bytes))
	}
	v := time.Unix(0, int64(binary.LittleEndian.Uint64(bytes)))
	return &v, nil
}

func (k TaskKey) FinishedAt() dbutil.TypedKey[*time.Time] {
	return dbutil.NewTypedKey(
		k.d.Pack(tuple.Tuple{string(k.id), "finished_at"}),
		finishedAtSerializer{},
	)
}
