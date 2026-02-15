package task

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

type inputSerializer struct{}

// Marshal implements dbutil.serializable.
func (s inputSerializer) Marshal(v []byte) []byte {
	if v == nil {
		return nil
	}
	return append([]byte(nil), v...)
}

// Unmarshal implements dbutil.serializable.
func (s inputSerializer) Unmarshal(bytes []byte) ([]byte, error) {
	if bytes == nil {
		return nil, errors.New("missing input")
	}
	return append([]byte(nil), bytes...), nil
}

func (k TaskKey) Input() dbutil.TypedKey[[]byte] {
	return dbutil.NewTypedKey(
		k.d.Pack(tuple.Tuple{string(k.id), "input"}),
		inputSerializer{},
	)
}
