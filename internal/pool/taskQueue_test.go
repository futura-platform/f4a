package pool_test

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/pool"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		t.Run("starts empty", func(t *testing.T) {
			_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				runnerIds, err := pool.ListTaskSets(tx, db)
				assert.NoError(t, err)
				assert.Empty(t, runnerIds)
				return nil, nil
			})
			assert.NoError(t, err)
		})

		runnerId := "test-runner"
		t.Run("after a task set has been created", func(t *testing.T) {
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				taskSet, cancel, err := pool.CreateOrOpenTaskSetForRunner(db, runnerId)
				cancel()
				assert.NoError(t, err)
				assert.NotNil(t, taskSet)

				runnerIds, err := pool.ListTaskSets(tx, db)
				assert.NoError(t, err)
				assert.Equal(t, []string{runnerId}, runnerIds)
				return nil, nil
			})
			assert.NoError(t, err)
		})

		t.Run("after a task set has been deleted", func(t *testing.T) {
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				taskSet, cancel, err := pool.CreateOrOpenTaskSetForRunner(db, runnerId)
				cancel()
				assert.NoError(t, err)
				assert.NotNil(t, taskSet)

				taskSet.Clear()

				runnerIds, err := pool.ListTaskSets(tx, db)
				assert.NoError(t, err)
				assert.Empty(t, runnerIds)
				return nil, nil
			})
			assert.NoError(t, err)
		})
	})
}
