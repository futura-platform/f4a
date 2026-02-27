package scheduler

import (
	"testing"

	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestRunnerSetCacheCachesSetUntilDeleteEvent(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const runnerID = "runner-cache-hit"
		ensureRunnerTaskSetExists(t, db, runnerID)

		setCache := &runnerSetCache{
			db:         db,
			activeSets: xsync.NewMap[string, *reliableset.Set](),
		}

		first, err := setCache.open(runnerID)
		require.NoError(t, err)

		setCache.OnAdd(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runnerID}}, false)
		setCache.OnUpdate(
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runnerID}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runnerID}},
		)

		stillCached, err := setCache.open(runnerID)
		require.NoError(t, err)
		require.Same(t, first, stillCached, "runner set should remain cached across add/update events")

		setCache.OnDelete(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runnerID}})
		reopened, err := setCache.open(runnerID)
		require.NoError(t, err)
		require.NotSame(t, first, reopened, "pod delete should invalidate the cached runner set")
	})
}

func TestRunnerSetCacheDeleteOnlyInvalidatesMatchingPod(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const (
			runnerA = "runner-a"
			runnerB = "runner-b"
		)
		ensureRunnerTaskSetExists(t, db, runnerA)
		ensureRunnerTaskSetExists(t, db, runnerB)

		setCache := &runnerSetCache{
			db:         db,
			activeSets: xsync.NewMap[string, *reliableset.Set](),
		}

		runnerASet, err := setCache.open(runnerA)
		require.NoError(t, err)
		runnerBSet, err := setCache.open(runnerB)
		require.NoError(t, err)

		setCache.OnDelete(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runnerA}})

		runnerAReopened, err := setCache.open(runnerA)
		require.NoError(t, err)
		runnerBStillCached, err := setCache.open(runnerB)
		require.NoError(t, err)

		require.NotSame(t, runnerASet, runnerAReopened, "deleted pod key should be invalidated")
		require.Same(t, runnerBSet, runnerBStillCached, "non-deleted pod keys should remain cached")
	})
}

func TestRunnerSetCacheDeleteInvalidatesOnDeletedFinalStateUnknown(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		const (
			runnerA = "runner-tombstone-a"
			runnerB = "runner-tombstone-b"
		)
		ensureRunnerTaskSetExists(t, db, runnerA)
		ensureRunnerTaskSetExists(t, db, runnerB)

		setCache := &runnerSetCache{
			db:         db,
			activeSets: xsync.NewMap[string, *reliableset.Set](),
		}

		runnerASet, err := setCache.open(runnerA)
		require.NoError(t, err)
		runnerBSet, err := setCache.open(runnerB)
		require.NoError(t, err)

		setCache.OnDelete(cache.DeletedFinalStateUnknown{
			Key: runnerA,
			Obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: runnerA},
			},
		})

		runnerAReopened, err := setCache.open(runnerA)
		require.NoError(t, err)
		runnerBStillCached, err := setCache.open(runnerB)
		require.NoError(t, err)

		require.NotSame(t, runnerASet, runnerAReopened, "tombstone delete should invalidate cached runner set")
		require.Same(t, runnerBSet, runnerBStillCached, "tombstone delete should not invalidate other runners")
	})
}

func ensureRunnerTaskSetExists(t *testing.T, db dbutil.DbRoot, runnerID string) {
	t.Helper()

	_, err := pool.CreateOrOpenTaskSetForRunner(db, db, runnerID)
	require.NoError(t, err)
}
