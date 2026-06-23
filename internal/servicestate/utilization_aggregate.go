package servicestate

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

type utilizationAggregate struct {
	directory directory.DirectorySubspace
}

func openUtilizationAggregate(tr fdb.ReadTransactor, db dbutil.DbRoot, path []string) (*utilizationAggregate, error) {
	subspace, err := db.Root.Open(tr, path, nil)
	if err != nil {
		return nil, err
	}
	return &utilizationAggregate{subspace}, nil
}

func createOrOpenUtilizationAggregate(tr fdb.Transactor, db dbutil.DbRoot, path []string) (*utilizationAggregate, error) {
	subspace, err := db.Root.CreateOrOpen(tr, path, nil)
	if err != nil {
		return nil, err
	}
	return &utilizationAggregate{subspace}, nil
}

type UtilizationDimension string

const (
	UtilizationDimensionCPU    UtilizationDimension = "cpu"
	UtilizationDimensionMemory UtilizationDimension = "memory"
)

func (u utilizationAggregate) add(tx fdb.Transaction, dimension UtilizationDimension, value int64) {
	dbutil.AtomicIncrement(tx, u.directory.Pack(tuple.Tuple{string(dimension)}), value)
}

func (u utilizationAggregate) get(tx fdb.ReadTransaction, dimension UtilizationDimension) (int64, error) {
	bytes, err := tx.Get(u.directory.Pack(tuple.Tuple{string(dimension)})).Get()
	if err != nil {
		return 0, err
	}
	if len(bytes) < 8 {
		return 0, nil
	}
	return int64(binary.LittleEndian.Uint64(bytes)), nil
}
