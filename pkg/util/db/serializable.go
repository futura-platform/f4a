package dbutil

type serializable[T any] interface {
	Marshal(v T) []byte
	Unmarshal([]byte) (T, error)
}

// type genericSerializable[T any] struct {
// 	v         T
// 	marshal   func(T) []byte
// 	unmarshal func([]byte) (T, error)
// }

// // Marshal implements serializable.
// func (g *genericSerializable[T]) Marshal() []byte {
// 	return g.marshal(g.v)
// }

// // Unmarshal implements serializable.
// func (g *genericSerializable[T]) Unmarshal(b []byte) (T, error) {
// 	return g.unmarshal(b)
// }

// func NewGenericSerializable[T any](v T, marshal func(T) []byte, unmarshal func([]byte) (T, error)) serializable[T] {
// 	return &genericSerializable[T]{
// 		v:         v,
// 		marshal:   marshal,
// 		unmarshal: unmarshal,
// 	}
// }
