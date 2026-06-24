package servicestate

import (
	"encoding/binary"
	"fmt"

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

// add increments the value at the given key by the given amount.
// it DOES NOT cause confliction.
func (u utilizationAggregate) add(tx fdb.Transaction, dimension UtilizationDimension, value int64) {
	dbutil.AtomicIncrement(tx, u.directory.Pack(tuple.Tuple{string(dimension)}), value)
}

// set sets the value at the given key to the given value.
// it DOES cause confliction.
func (u utilizationAggregate) set(tx fdb.Transaction, dimension UtilizationDimension, value int64) {
	var one [8]byte
	binary.LittleEndian.PutUint64(one[:], uint64(value))
	tx.Set(u.directory.Pack(tuple.Tuple{string(dimension)}), one[:])
}

func (u utilizationAggregate) get(tx fdb.ReadTransaction, dimension UtilizationDimension) (int64, error) {
	bytes, err := tx.Get(u.directory.Pack(tuple.Tuple{string(dimension)})).Get()
	if err != nil {
		return 0, err
	}
	if len(bytes) == 0 {
		return 0, nil
	}
	if len(bytes) != 8 {
		return 0, fmt.Errorf("invalid utilization encoding size: %d", len(bytes))
	}
	return int64(binary.LittleEndian.Uint64(bytes)), nil
}
