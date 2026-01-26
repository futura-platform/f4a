package task

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/pkg/util/db"
)

type runnerIdSerializer struct{}

// Marshal implements dbutil.serializable.
func (s runnerIdSerializer) Marshal(v string) []byte {
	return []byte(v)
}

// Unmarshal implements dbutil.serializable.
func (s runnerIdSerializer) Unmarshal(bytes []byte) (string, error) {
	return string(bytes), nil
}

func (k TaskKey) RunnerId() dbutil.TypedKey[string] {
	return dbutil.NewTypedKey(
		k.d.Pack(tuple.Tuple{k.id.Bytes(), "runner_id"}),
		runnerIdSerializer{},
	)
}
