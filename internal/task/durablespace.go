package task

import "github.com/apple/foundationdb/bindings/go/src/fdb/subspace"

func (k TaskKey) DurableObjectSpace(namespace string) subspace.Subspace {
	return k.keyspace().Sub(namespace, "durable")
}
