package run

import (
	"testing"

	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/stretchr/testify/assert"
)

func TestRunnableId(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
		assert.NoError(t, err)
		id := task.NewId()
		tkey, err := tasksDirectory.Create(db, id)
		assert.NoError(t, err)
		runnable := NewRunnable(nil, execute.ExecutorId("test"), db, tkey)
		assert.Equal(t, id, runnable.Id())
	})
}
