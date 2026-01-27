package run

import (
	"testing"

	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/internal/util"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
)

func TestRunnableId(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
		tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
		assert.NoError(t, err)
		id := task.NewId()
		tkey, err := tasksDirectory.Create(db, id)
		assert.NoError(t, err)
		runnable := NewRunnable(nil, db.Database, tkey, executiontype.NewInMemoryContainer())
		assert.Equal(t, id, runnable.Id())
	})
}
