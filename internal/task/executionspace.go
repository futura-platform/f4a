package task

import "github.com/apple/foundationdb/bindings/go/src/fdb/subspace"

func (k TaskKey) MemoTable(namespace string) subspace.Subspace {
	return k.keyspace().Sub(namespace, "memo_table")
}

func (k TaskKey) CallOrder(namespace string) subspace.Subspace {
	return k.keyspace().Sub(namespace, "call_order")
}
