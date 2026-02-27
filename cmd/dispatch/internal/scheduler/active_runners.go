package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/futura-platform/f4a/cmd/dispatch/internal/k8s"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/puzpuzpuz/xsync/v4"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const activeRunnerReconcileWorkers = 2
const initialRunnerReconcileRetryInterval = 100 * time.Millisecond

// liveActiveRunnerSets returns a map of runner ID to its active runner set.
// It blocks until the pod informer cache has delivered an initial snapshot
// (which can be empty), so callers start with a known initial state.
// After that, the map is updated in realtime and reflects an eventually
// consistent view of active runner sets.
func liveActiveRunnerSets(
	ctx context.Context,
	db dbutil.DbRoot,
	clients *k8s.Clients,
	namespace string,
	statefulSetName string,
) (sets *xsync.Map[string, *reliableset.Set], cancel context.CancelFunc, err error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	if namespace == "" {
		return nil, nil, fmt.Errorf("namespace is required")
	}
	if statefulSetName == "" {
		return nil, nil, fmt.Errorf("statefulset name is required")
	}
	if clients == nil || clients.Core == nil {
		return nil, nil, fmt.Errorf("kubernetes core client is not initialized")
	}

	selector, err := statefulSetLabelSelector(runCtx, clients, namespace, statefulSetName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve statefulset selector: %w", err)
	}

	activeRunnerSets := xsync.NewMap[string, *reliableset.Set]()
	reconciler := newRunnerSetReconciler(
		activeRunnerSets,
		func(runnerID string) (*reliableset.Set, error) {
			set, cancelCompaction, err := pool.OpenTaskSetForRunner(db, db, runnerID)
			if err != nil {
				return nil, err
			}
			// Scheduler should not own compaction routines; workers do.
			cancelCompaction()
			return set, nil
		},
	)

	handlePodUpsert := func(pod *corev1.Pod) {
		if pod == nil {
			return
		}
		ready := podReady(pod)
		if !ready {
			// Drop immediately on not-ready transitions before async reconcile.
			activeRunnerSets.Delete(pod.Name)
		}
		reconciler.setDesiredReady(pod.Name, ready)
	}
	handlePodDelete := func(pod *corev1.Pod) {
		if pod == nil {
			return
		}
		activeRunnerSets.Delete(pod.Name)
		reconciler.setDesiredReady(pod.Name, false)
	}

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		clients.Core,
		0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector
		}),
	)
	podInformer := informerFactory.Core().V1().Pods().Informer()
	_, err = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return
			}
			handlePodUpsert(pod)
		},
		UpdateFunc: func(_, newObj any) {
			pod, ok := newObj.(*corev1.Pod)
			if !ok {
				return
			}
			handlePodUpsert(pod)
		},
		DeleteFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				pod, ok = tombstone.Obj.(*corev1.Pod)
				if !ok {
					return
				}
			}
			handlePodDelete(pod)
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to register pod event handler: %w", err)
	}

	informerFactory.Start(runCtx.Done())
	// Wait for initial pod state to arrive before returning to the scheduler.
	if !cache.WaitForCacheSync(runCtx.Done(), podInformer.HasSynced) {
		if runErr := runCtx.Err(); runErr != nil {
			return nil, nil, runErr
		}
		return nil, nil, fmt.Errorf("failed to sync pod informer cache")
	}

	initialReadyRunnerIDs := make([]string, 0)
	for _, obj := range podInformer.GetStore().List() {
		pod, ok := obj.(*corev1.Pod)
		if !ok || pod == nil {
			continue
		}
		ready := podReady(pod)
		// Seed desired state from the synced informer snapshot before scheduling starts.
		reconciler.setDesiredReady(pod.Name, ready)
		if ready {
			initialReadyRunnerIDs = append(initialReadyRunnerIDs, pod.Name)
		}
	}
	// One-time barrier: wait for all initially ready runners to reconcile to an
	// open task set (or become not-ready) before returning to the scheduler loop.
	if err := reconciler.waitForInitialReadyRunnerSets(runCtx, initialReadyRunnerIDs); err != nil {
		return nil, nil, fmt.Errorf("failed initial runner-set reconcile: %w", err)
	}

	for range activeRunnerReconcileWorkers {
		go reconciler.runWorker(runCtx)
	}

	go func() {
		<-runCtx.Done()
		reconciler.shutdown()
	}()

	return activeRunnerSets, cancel, nil
}

type runnerSetReconciler struct {
	activeRunnerSets *xsync.Map[string, *reliableset.Set]
	openTaskSet      func(runnerID string) (*reliableset.Set, error)
	queue            workqueue.TypedRateLimitingInterface[string]

	mu            sync.Mutex
	desiredReady  map[string]bool
	desiredEpochs map[string]uint64
}

func newRunnerSetReconciler(
	activeRunnerSets *xsync.Map[string, *reliableset.Set],
	openTaskSet func(runnerID string) (*reliableset.Set, error),
) *runnerSetReconciler {
	return &runnerSetReconciler{
		activeRunnerSets: activeRunnerSets,
		openTaskSet:      openTaskSet,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "active-runner-sets",
			},
		),
		desiredReady:  map[string]bool{},
		desiredEpochs: map[string]uint64{},
	}
}

func (r *runnerSetReconciler) shutdown() {
	r.queue.ShutDown()
}

func (r *runnerSetReconciler) setDesiredReady(runnerID string, ready bool) {
	r.mu.Lock()
	prev, existed := r.desiredReady[runnerID]
	if !existed || prev != ready {
		r.desiredReady[runnerID] = ready
		r.desiredEpochs[runnerID]++
	}
	r.mu.Unlock()

	r.queue.Add(runnerID)
}

func (r *runnerSetReconciler) runWorker(ctx context.Context) {
	for r.processNext(ctx) {
	}
}

func (r *runnerSetReconciler) processNext(ctx context.Context) bool {
	item, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(item)

	runnerID := item

	if err := r.reconcileRunner(ctx, runnerID); err != nil {
		if ctx.Err() != nil {
			r.queue.Forget(item)
			return false
		}
		r.queue.AddRateLimited(runnerID)
		return true
	}

	r.queue.Forget(item)
	return true
}

func (r *runnerSetReconciler) reconcileRunner(ctx context.Context, runnerID string) error {
	desiredReady, desiredEpoch := r.desiredSnapshot(runnerID)
	if !desiredReady {
		r.activeRunnerSets.Delete(runnerID)
		return nil
	}
	if _, exists := r.activeRunnerSets.Load(runnerID); exists {
		return nil
	}

	set, err := r.openTaskSet(runnerID)
	if err != nil {
		return fmt.Errorf("open task set for runner %q: %w", runnerID, err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if !r.shouldStoreReadySet(runnerID, desiredEpoch) {
		return nil
	}
	r.activeRunnerSets.Store(runnerID, set)
	return nil
}

func (r *runnerSetReconciler) waitForInitialReadyRunnerSets(ctx context.Context, runnerIDs []string) error {
	pending := make(map[string]struct{}, len(runnerIDs))
	for _, runnerID := range runnerIDs {
		pending[runnerID] = struct{}{}
	}
	if len(pending) == 0 {
		return nil
	}

	retryTicker := time.NewTicker(initialRunnerReconcileRetryInterval)
	defer retryTicker.Stop()

	for len(pending) > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		for runnerID := range pending {
			if _, exists := r.activeRunnerSets.Load(runnerID); exists {
				delete(pending, runnerID)
				continue
			}

			err := r.reconcileRunner(ctx, runnerID)
			if err != nil {
				// Keep pending and retry until success or context cancellation.
				continue
			}

			desiredReady, _ := r.desiredSnapshot(runnerID)
			if !desiredReady {
				delete(pending, runnerID)
				continue
			}
			if _, exists := r.activeRunnerSets.Load(runnerID); exists {
				delete(pending, runnerID)
			}
		}

		if len(pending) == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retryTicker.C:
		}
	}
	return nil
}

func (r *runnerSetReconciler) desiredSnapshot(runnerID string) (ready bool, epoch uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.desiredReady[runnerID], r.desiredEpochs[runnerID]
}

func (r *runnerSetReconciler) shouldStoreReadySet(runnerID string, expectedEpoch uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.desiredReady[runnerID] && r.desiredEpochs[runnerID] == expectedEpoch
}

func statefulSetLabelSelector(
	ctx context.Context,
	clients *k8s.Clients,
	namespace string,
	statefulSetName string,
) (string, error) {
	statefulSet, err := clients.Core.AppsV1().StatefulSets(namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to fetch statefulset %q: %w", statefulSetName, err)
	}
	if statefulSet.Spec.Selector == nil {
		return "", fmt.Errorf("statefulset %q has no selector", statefulSetName)
	}
	selector, err := metav1.LabelSelectorAsSelector(statefulSet.Spec.Selector)
	if err != nil {
		return "", fmt.Errorf("invalid selector on statefulset %q: %w", statefulSetName, err)
	}
	return selector.String(), nil
}

func podReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
