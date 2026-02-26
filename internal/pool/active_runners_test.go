package pool

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/assert"
)

func TestActiveRunners(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		activeRunners, err := CreateOrOpenActiveRunners(db)
		if err != nil {
			t.Fatal(err)
		}

		testRunnerId := "test-runner"
		t.Run("reports runner as inactive when no liveness marker is present", func(t *testing.T) {
			_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
				isActive := activeRunners.IsActive(tx, testRunnerId)
				assert.False(t, isActive.MustGet())

				return nil, nil
			})
			assert.NoError(t, err)
		})

		t.Run("reports runner as active when liveness marker is present", func(t *testing.T) {
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				activeRunners.SetActive(tx, testRunnerId, true)

				isActive := activeRunners.IsActive(tx, testRunnerId)
				assert.True(t, isActive.MustGet())
				return nil, nil
			})
			assert.NoError(t, err)
		})

		t.Run("reports runner as inactive when liveness marker is removed", func(t *testing.T) {
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				activeRunners.SetActive(tx, testRunnerId, false)

				isActive := activeRunners.IsActive(tx, testRunnerId)
				assert.False(t, isActive.MustGet())
				return nil, nil
			})
			assert.NoError(t, err)
		})
	})
}
