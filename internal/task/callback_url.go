package task

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

type callbackUrlSerializer struct{}

// Marshal implements dbutil.serializable.
func (s callbackUrlSerializer) Marshal(v string) []byte {
	return []byte(v)
}

// Unmarshal implements dbutil.serializable.
func (s callbackUrlSerializer) Unmarshal(bytes []byte) (string, error) {
	if bytes == nil {
		return "", errors.New("missing callback url")
	}
	return string(bytes), nil
}

func (k TaskKey) CallbackUrl() dbutil.TypedKey[string] {
	return dbutil.NewTypedKey(
		k.d.Pack(tuple.Tuple{k.id.Bytes(), "callback_url"}),
		callbackUrlSerializer{},
	)
}
