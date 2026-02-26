package task

import (
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeFutureByteSlice struct {
	value []byte
	err   error
}

func (f fakeFutureByteSlice) Get() ([]byte, error) {
	return f.value, f.err
}

func (f fakeFutureByteSlice) MustGet() []byte {
	if f.err != nil {
		panic(f.err)
	}
	return f.value
}

func (f fakeFutureByteSlice) BlockUntilReady() {}

func (f fakeFutureByteSlice) IsReady() bool { return true }

func (f fakeFutureByteSlice) Cancel() {}

var _ fdb.FutureByteSlice = fakeFutureByteSlice{}

func newLifecycleStatusFuture(status LifecycleStatus, err error) *dbutil.Future[LifecycleStatus] {
	serializer := lifecycleStatusSerializer{}
	return dbutil.NewFuture[LifecycleStatus](
		fakeFutureByteSlice{
			value: serializer.Marshal(status),
			err:   err,
		},
		serializer.Unmarshal,
	)
}

func newRunnerIDFuture(runnerID *string, err error) *dbutil.Future[*string] {
	serializer := runnerIdSerializer{}
	return dbutil.NewFuture[*string](
		fakeFutureByteSlice{
			value: serializer.Marshal(runnerID),
			err:   err,
		},
		serializer.Unmarshal,
	)
}

func TestAssignmentStateValidateRunnerLifecycleInvariant(t *testing.T) {
	runnerID := "runner-1"
	empty := ""

	t.Run("running requires non-empty runner id", func(t *testing.T) {
		assert.ErrorIs(t, AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusRunning, nil),
			RunnerIDFuture:        newRunnerIDFuture(nil, nil),
		}.ValidateRunnerLifecycleInvariant(), ErrRunningTaskMissingRunnerID)

		assert.ErrorIs(t, AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusRunning, nil),
			RunnerIDFuture:        newRunnerIDFuture(&empty, nil),
		}.ValidateRunnerLifecycleInvariant(), ErrRunningTaskMissingRunnerID)
	})

	t.Run("non-running requires nil runner id", func(t *testing.T) {
		assert.ErrorIs(t, AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusPending, nil),
			RunnerIDFuture:        newRunnerIDFuture(&runnerID, nil),
		}.ValidateRunnerLifecycleInvariant(), ErrNonRunningTaskHasRunnerID)

		assert.ErrorIs(t, AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusSuspended, nil),
			RunnerIDFuture:        newRunnerIDFuture(&runnerID, nil),
		}.ValidateRunnerLifecycleInvariant(), ErrNonRunningTaskHasRunnerID)
	})

	t.Run("valid combinations pass", func(t *testing.T) {
		assert.NoError(t, AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusRunning, nil),
			RunnerIDFuture:        newRunnerIDFuture(&runnerID, nil),
		}.ValidateRunnerLifecycleInvariant())

		assert.NoError(t, AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusPending, nil),
			RunnerIDFuture:        newRunnerIDFuture(nil, nil),
		}.ValidateRunnerLifecycleInvariant())

		assert.NoError(t, AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusSuspended, nil),
			RunnerIDFuture:        newRunnerIDFuture(nil, nil),
		}.ValidateRunnerLifecycleInvariant())
	})

	t.Run("future errors are surfaced", func(t *testing.T) {
		lifecycleErr := errors.New("lifecycle read failed")
		err := AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusPending, lifecycleErr),
			RunnerIDFuture:        newRunnerIDFuture(nil, nil),
		}.ValidateRunnerLifecycleInvariant()
		require.Error(t, err)
		require.ErrorIs(t, err, lifecycleErr)

		runnerErr := errors.New("runner read failed")
		err = AssignmentState{
			LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusPending, nil),
			RunnerIDFuture:        newRunnerIDFuture(nil, runnerErr),
		}.ValidateRunnerLifecycleInvariant()
		require.Error(t, err)
		require.ErrorIs(t, err, runnerErr)
	})
}

func TestAssignmentStateIsRunningOn(t *testing.T) {
	runnerID := "runner-1"
	otherRunnerID := "runner-2"

	ok, err := AssignmentState{
		LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusRunning, nil),
		RunnerIDFuture:        newRunnerIDFuture(&runnerID, nil),
	}.IsRunningOn(runnerID)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = AssignmentState{
		LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusRunning, nil),
		RunnerIDFuture:        newRunnerIDFuture(&otherRunnerID, nil),
	}.IsRunningOn(runnerID)
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = AssignmentState{
		LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusPending, nil),
		RunnerIDFuture:        newRunnerIDFuture(nil, nil),
	}.IsRunningOn(runnerID)
	require.NoError(t, err)
	assert.False(t, ok)

	lifecycleErr := errors.New("lifecycle read failed")
	_, err = AssignmentState{
		LifecycleStatusFuture: newLifecycleStatusFuture(LifecycleStatusPending, lifecycleErr),
		RunnerIDFuture:        newRunnerIDFuture(nil, nil),
	}.IsRunningOn(runnerID)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycleErr)
}
