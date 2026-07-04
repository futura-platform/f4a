package servicestate

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pullDeadLetter(t *testing.T, db dbutil.DbRoot, maxResults int) []*taskv1.DeadLetter {
	t.Helper()

	queue, err := CreateOrOpenDeadLetterQueue(db)
	require.NoError(t, err)

	var deadLetters []*taskv1.DeadLetter
	_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		deadLetters, err = queue.Pull(tx, maxResults)
		return nil, err
	})
	require.NoError(t, err)
	return deadLetters
}

func pullAndAckDeadLetter(t *testing.T, db dbutil.DbRoot) (task.Id, bool) {
	t.Helper()

	queue, err := CreateOrOpenDeadLetterQueue(db)
	require.NoError(t, err)

	var deadLetters []*taskv1.DeadLetter
	_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		deadLetters, err = queue.Pull(tx, 1)
		return nil, err
	})
	require.NoError(t, err)
	if len(deadLetters) == 0 {
		return "", false
	}

	taskID := task.Id(deadLetters[0].GetTaskId())
	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		queue.Acknowledge(tx, []task.Id{taskID})
		return nil, nil
	})
	require.NoError(t, err)
	return taskID, true
}

func parkTaskResult(t *testing.T, message string) *taskv1.TaskResult {
	t.Helper()
	result := &taskv1.TaskResult{}
	result.SetFailure(message)
	return result
}

func TestDeadLetterParkerParksTaskId(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		taskID := task.Id("dead-letter-task")
		parker := NewDeadLetterParker(db, taskID)
		result := parkTaskResult(t, "delivery exhausted: bad status")

		require.NoError(t, parker.Park(t.Context(), result))

		id, found := pullAndAckDeadLetter(t, db)
		require.True(t, found, "expected a dead letter to be parked")
		assert.Equal(t, taskID, id)

		_, found = pullAndAckDeadLetter(t, db)
		assert.False(t, found, "expected exactly one dead letter")
	})
}

func TestDeadLetterPullReturnsStoredResult(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		taskID := task.Id("dead-letter-task")
		result := parkTaskResult(t, "delivery exhausted: bad status")
		require.NoError(t, NewDeadLetterParker(db, taskID).Park(t.Context(), result))

		deadLetters := pullDeadLetter(t, db, 1)
		require.Len(t, deadLetters, 1)
		assert.Equal(t, string(taskID), deadLetters[0].GetTaskId())
		assert.Equal(t, result.GetFailure(), deadLetters[0].GetResult().GetFailure())
	})
}

func TestDeadLetterUnacknowledgedPullLeavesEntryInQueue(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		taskID := task.Id("dead-letter-task")
		result := parkTaskResult(t, "delivery exhausted: bad status")
		require.NoError(t, NewDeadLetterParker(db, taskID).Park(t.Context(), result))

		first := pullDeadLetter(t, db, 1)
		require.Len(t, first, 1)
		assert.Equal(t, string(taskID), first[0].GetTaskId())

		second := pullDeadLetter(t, db, 1)
		require.Len(t, second, 1, "pull without acknowledge must not remove the dead letter")
		assert.Equal(t, string(taskID), second[0].GetTaskId())
		assert.Equal(t, result.GetFailure(), second[0].GetResult().GetFailure())
	})
}

func TestDeadLetterParkerPullOrder(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		first := task.Id("dead-letter-a")
		second := task.Id("dead-letter-b")

		require.NoError(t, NewDeadLetterParker(db, first).Park(t.Context(), parkTaskResult(t, "failure one")))
		require.NoError(t, NewDeadLetterParker(db, second).Park(t.Context(), parkTaskResult(t, "failure two")))

		id, found := pullAndAckDeadLetter(t, db)
		require.True(t, found)
		assert.Equal(t, first, id)

		id, found = pullAndAckDeadLetter(t, db)
		require.True(t, found)
		assert.Equal(t, second, id)
	})
}
