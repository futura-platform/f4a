package servicestate

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliablequeue"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dequeueDeadLetter(t *testing.T, db dbutil.DbRoot) (task.Id, bool) {
	t.Helper()

	queue, err := CreateOrOpenDeadLetterQueue(db)
	require.NoError(t, err)

	var (
		id    task.Id
		found bool
	)
	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		id, found = task.Id(""), false
		dequeued, err := queue.Dequeue(tx)
		if err != nil {
			if err == reliablequeue.ErrQueueEmpty {
				return nil, nil
			}
			return nil, err
		}
		id, found = dequeued, true
		return nil, nil
	})
	require.NoError(t, err)
	return id, found
}

func TestDeadLetterParkerEnqueuesTaskId(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		taskID := task.Id("dead-letter-task")
		parker := NewDeadLetterParker(db, taskID)

		require.NoError(t, parker.Park(t.Context(), "delivery exhausted: bad status"))

		id, found := dequeueDeadLetter(t, db)
		require.True(t, found, "expected a dead letter to be enqueued")
		assert.Equal(t, taskID, id)

		_, found = dequeueDeadLetter(t, db)
		assert.False(t, found, "expected exactly one dead letter")
	})
}

func TestDeadLetterParkerPreservesFIFOOrder(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		first := task.Id("dead-letter-first")
		second := task.Id("dead-letter-second")

		require.NoError(t, NewDeadLetterParker(db, first).Park(t.Context(), "failure one"))
		require.NoError(t, NewDeadLetterParker(db, second).Park(t.Context(), "failure two"))

		id, found := dequeueDeadLetter(t, db)
		require.True(t, found)
		assert.Equal(t, first, id)

		id, found = dequeueDeadLetter(t, db)
		require.True(t, found)
		assert.Equal(t, second, id)
	})
}
