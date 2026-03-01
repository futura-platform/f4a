package scheduler

import (
	"context"
	"fmt"

	"github.com/futura-platform/f4a/cmd/dispatch/internal/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	corev1informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// liveRunnerPods returns a map of runner ID to its active runner set.
// It blocks until the pod informer cache has delivered an initial snapshot
// (which can be empty), so callers start with a known initial state.
// After that, the returned PodNamespaceLister follows its own semantics.
func liveRunnerPods(
	ctx context.Context,
	clients *k8s.Clients,
	namespace string,
	statefulSetName string,
) (runners corev1informer.PodInformer, _ context.CancelFunc, returnErr error) {
	sts, err := clients.Core.AppsV1().
		StatefulSets(namespace).
		Get(ctx, statefulSetName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch statefulset %q: %w", statefulSetName, err)
	}

	selector := metav1.FormatLabelSelector(sts.Spec.Selector)

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		clients.Core,
		0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector
		}),
	)
	podInformer := informerFactory.Core().V1().Pods()
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if returnErr != nil {
			cancel()
		}
	}()
	informerFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.Informer().HasSynced) {
		return nil, nil, fmt.Errorf("cache sync failed")
	}

	return podInformer, cancel, nil
}
