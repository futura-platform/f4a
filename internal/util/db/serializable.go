package dbutil

type serializable[T any] interface {
	Marshal(v T) []byte
	Unmarshal([]byte) (T, error)
}
