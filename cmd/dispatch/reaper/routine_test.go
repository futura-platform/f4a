package reaper

import (
	"context"
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	k8stesting "k8s.io/client-go/testing"
)

const (
	testNamespace = "test-ns"
	testRunnerId  = "runner-0"
)

// fakePodLister is a stand-in for the informer-backed namespace lister: it
// serves pods from a map, returns NotFound for anything else, and can be
// forced to fail every lookup.
type fakePodLister struct {
	pods map[string]*corev1.Pod
	err  error
}

func (f fakePodLister) List(labels.Selector) ([]*corev1.Pod, error) {
	return nil, nil
}

func (f fakePodLister) Get(name string) (*corev1.Pod, error) {
	if f.err != nil {
		return nil, f.err
	}
	if pod, ok := f.pods[name]; ok {
		return pod, nil
	}
	return nil, apierrors.NewNotFound(corev1.Resource("pods"), name)
}

func livePodsClient(pods []runtime.Object, getErr error) corev1client.PodInterface {
	clientset := fake.NewSimpleClientset(pods...)
	if getErr != nil {
		clientset.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, getErr
		})
	}
	return clientset.CoreV1().Pods(testNamespace)
}

// TestReapAllWiring verifies how reapAll consumes the liveness classification:
// a provably dead runner is reaped, a live one is untouched, and one of
// unknown liveness is kept for a later cycle with the skip surfaced in the
// pod_lookup_failures counter.
func TestReapAllWiring(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		placer, _, err := servicestate.CreateOrOpenTaskPlacer(db)
		require.NoError(t, err)
		activeRunners, err := pool.CreateOrOpenActiveRunners(db)
		require.NoError(t, err)
		taskDir, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)

		const (
			deadRunner    = "dead-runner"
			aliveRunner   = "alive-runner"
			unknownRunner = "unknown-runner"
		)
		for _, runnerId := range []string{deadRunner, aliveRunner, unknownRunner} {
			_, err := servicestate.CreateOrOpenTaskSetForRunner(db, db, runnerId)
			require.NoError(t, err)
		}

		alivePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      aliveRunner,
			Namespace: testNamespace,
		}}
		cached := fakePodLister{pods: map[string]*corev1.Pod{aliveRunner: alivePod}}

		lookupErr := errors.New("api server unavailable")
		clientset := fake.NewSimpleClientset(alivePod)
		clientset.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.(k8stesting.GetAction).GetName() == unknownRunner {
				return true, nil, lookupErr
			}
			return false, nil, nil
		})

		failedLookups, err := reapAll(
			t.Context(),
			db,
			placer,
			cached,
			clientset.CoreV1().Pods(testNamespace),
			activeRunners,
			taskDir,
		)
		require.NoError(t, err)

		var remaining []string
		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			var err error
			remaining, err = servicestate.ListTaskSets(tx, db)
			return nil, err
		})
		require.NoError(t, err)
		require.NotContains(t, remaining, deadRunner, "provably dead runner must be reaped")
		require.Contains(t, remaining, aliveRunner, "live runner must be untouched")
		require.Contains(t, remaining, unknownRunner, "unknown-liveness runner must be kept for a later cycle")

		require.EqualValues(t, 1, failedLookups, "the skipped lookup must be reported for the metric")
	})
}

func TestRunnerIsDead(t *testing.T) {
	runnerPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      testRunnerId,
		Namespace: testNamespace,
	}}
	lookupErr := errors.New("api server unavailable")

	cases := []struct {
		name     string
		cached   fakePodLister
		livePods []runtime.Object
		liveErr  error
		wantDead bool
		wantErr  error
	}{
		{
			name:   "cache sees pod: alive",
			cached: fakePodLister{pods: map[string]*corev1.Pod{testRunnerId: runnerPod}},
			// the live client must not matter; make it error to prove that
			liveErr: lookupErr,
		},
		{
			name:     "cache not found, live sees pod: alive",
			cached:   fakePodLister{},
			livePods: []runtime.Object{runnerPod},
		},
		{
			name:     "cache not found, live not found: dead",
			cached:   fakePodLister{},
			wantDead: true,
		},
		{
			name:    "cache not found, live lookup fails: unknown, not alive-and-fine",
			cached:  fakePodLister{},
			liveErr: lookupErr,
			wantErr: lookupErr,
		},
		{
			name:     "cache lookup fails, live not found: dead",
			cached:   fakePodLister{err: lookupErr},
			wantDead: true,
		},
		{
			name:     "cache lookup fails, live sees pod: alive",
			cached:   fakePodLister{err: lookupErr},
			livePods: []runtime.Object{runnerPod},
		},
		{
			name:    "cache lookup fails, live lookup fails: unknown",
			cached:  fakePodLister{err: lookupErr},
			liveErr: lookupErr,
			wantErr: lookupErr,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dead, err := runnerIsDead(
				context.Background(),
				tc.cached,
				livePodsClient(tc.livePods, tc.liveErr),
				testRunnerId,
			)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.False(t, dead)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantDead, dead)
		})
	}
}
