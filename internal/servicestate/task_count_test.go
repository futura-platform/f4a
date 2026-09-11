package servicestate

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func TestCountTasksForRunners(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		for runner, count := range map[string]uint64{"first": 12, "second": 7, "unselected": 100} {
			set, err := CreateOrOpenTaskSetForRunner(db, db, runner)
			require.NoError(t, err)
			metadata, err := db.Root.Open(db, append(taskSetPath(runner), "metadata"), nil)
			require.NoError(t, err)
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(metadata.Pack(tuple.Tuple{"cardinality"}), binary.LittleEndian.AppendUint64(nil, count))
				return nil, nil
			})
			require.NoError(t, err)
			_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				value, err := set.Cardinality(tx)
				require.NoError(t, err)
				require.Equal(t, int64(count), value)
				return nil, nil
			})
			require.NoError(t, err)
		}

		value, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			return CountTasksForRunners(tx, db, []string{"first", "removed", "second"})
		})
		require.NoError(t, err)
		require.Equal(t, int64(19), value)

		metadata, err := db.Root.Open(db, append(taskSetPath("second"), "metadata"), nil)
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			tx.Set(metadata.Pack(tuple.Tuple{"cardinality"}), []byte{1})
			return nil, nil
		})
		require.NoError(t, err)
		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			return CountTasksForRunners(tx, db, []string{"first", "second"})
		})
		require.ErrorContains(t, err, "invalid cardinality value length")
	})
}

func TestCountTasksForRunnersMultipleReadWindows(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		runners := make([]string, cardinalityReadParallelism*3+1)
		var expected int64
		for i := range runners {
			runners[i] = fmt.Sprintf("runner-%d", i)
			if i%7 == 0 {
				continue
			}
			metadata, err := db.Root.Create(db, append(taskSetPath(runners[i]), "metadata"), nil)
			require.NoError(t, err)
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(metadata.Pack(tuple.Tuple{"cardinality"}), binary.LittleEndian.AppendUint64(nil, uint64(i+1)))
				return nil, nil
			})
			require.NoError(t, err)
			expected += int64(i + 1)
		}
		value, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			return CountTasksForRunners(tx, db, runners)
		})
		require.NoError(t, err)
		require.Equal(t, expected, value)
		value, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			return CountTasksForRunners(tx, db, nil)
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), value)
	})
}

func BenchmarkCountTasksForRunners(b *testing.B) {
	testutil.WithEphemeralDBRoot(b, func(db dbutil.DbRoot) {
		runners := make([]string, 64)
		for i := range runners {
			runners[i] = fmt.Sprintf("runner-%d", i)
			metadata, err := db.Root.Create(db, append(taskSetPath(runners[i]), "metadata"), nil)
			require.NoError(b, err)
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(metadata.Pack(tuple.Tuple{"cardinality"}), binary.LittleEndian.AppendUint64(nil, 64))
				return nil, nil
			})
			require.NoError(b, err)
		}
		for b.Loop() {
			value, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				return CountTasksForRunners(tx, db, runners)
			})
			require.NoError(b, err)
			require.Equal(b, int64(len(runners)*64), value)
		}
	})
}
