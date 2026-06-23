package reliableset

import "encoding"

type TSet[T encoding.BinaryUnmarshaler] struct {
	set Set
}
