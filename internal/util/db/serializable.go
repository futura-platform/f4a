package dbutil

type Serializer[T any] interface {
	Marshal(T) []byte
	Unmarshal([]byte) (T, error)
}
