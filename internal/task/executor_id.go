package task

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/execute"
)

type executorIdSerializer struct{}

// Marshal implements dbutil.serializable.
func (s executorIdSerializer) Marshal(v execute.ExecutorId) []byte {
	return []byte(v)
}

// Unmarshal implements dbutil.serializable.
func (s executorIdSerializer) Unmarshal(bytes []byte) (execute.ExecutorId, error) {
	if bytes == nil {
		return "", errors.New("missing executor id")
	}
	return execute.ExecutorId(string(bytes)), nil
}

func (k TaskKey) ExecutorId() dbutil.TypedKey[execute.ExecutorId] {
	return dbutil.NewTypedKey(
		k.d.Pack(tuple.Tuple{string(k.id), "executor_id"}),
		executorIdSerializer{},
	)
}
