package k8s

import (
	"context"
	"fmt"
	"math"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

type WorkerUtilization struct {
	CPU    float64
	Memory float64
}

func WorkerUtilizationSnapshot(
	ctx context.Context,
	clients *Clients,
	namespace string,
	statefulSetName string,
) (map[string]WorkerUtilization, error) {
	if clients == nil || clients.Core == nil || clients.Metrics == nil {
		return nil, fmt.Errorf("k8s clients are not initialized")
	}

	selector, err := statefulSetSelector(ctx, clients, namespace, statefulSetName)
	if err != nil {
		return nil, err
	}

	pods, err := clients.Core.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods for statefulset %q: %w", statefulSetName, err)
	}

	metricsList, err := clients.Metrics.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pod metrics: %w", err)
	}

	metricsByPod := make(map[string]metricsv1beta1.PodMetrics, len(metricsList.Items))
	for _, item := range metricsList.Items {
		metricsByPod[item.Name] = item
	}

	utilizations := make(map[string]WorkerUtilization)
	for _, pod := range pods.Items {
		if !podReady(pod) {
			continue
		}
		metrics, ok := metricsByPod[pod.Name]
		if !ok {
			continue
		}
		utilization, ok := computePodUtilization(pod, metrics)
		if !ok {
			continue
		}
		utilizations[pod.Name] = utilization
	}

	return utilizations, nil
}

func statefulSetSelector(
	ctx context.Context,
	clients *Clients,
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

func computePodUtilization(pod corev1.Pod, metrics metricsv1beta1.PodMetrics) (WorkerUtilization, bool) {
	metricsByContainer := make(map[string]metricsv1beta1.ContainerMetrics, len(metrics.Containers))
	for _, container := range metrics.Containers {
		metricsByContainer[container.Name] = container
	}

	var cpuUsageMilli int64
	var memUsageBytes int64
	var cpuRequestMilli int64
	var memRequestBytes int64

	for _, container := range pod.Spec.Containers {
		metricsContainer, ok := metricsByContainer[container.Name]
		if !ok {
			continue
		}

		if cpuRequest, ok := requestOrLimitMilli(container, corev1.ResourceCPU); ok {
			if usage, ok := metricsContainer.Usage[corev1.ResourceCPU]; ok {
				cpuUsageMilli += usage.MilliValue()
				cpuRequestMilli += cpuRequest
			}
		}
		if memRequest, ok := requestOrLimitBytes(container, corev1.ResourceMemory); ok {
			if usage, ok := metricsContainer.Usage[corev1.ResourceMemory]; ok {
				memUsageBytes += usage.Value()
				memRequestBytes += memRequest
			}
		}
	}

	if cpuRequestMilli == 0 || memRequestBytes == 0 {
		return WorkerUtilization{}, false
	}

	cpuUtil := clamp01(float64(cpuUsageMilli) / float64(cpuRequestMilli))
	memUtil := clamp01(float64(memUsageBytes) / float64(memRequestBytes))
	return WorkerUtilization{CPU: cpuUtil, Memory: memUtil}, true
}

func requestOrLimitMilli(container corev1.Container, name corev1.ResourceName) (int64, bool) {
	if q, ok := container.Resources.Requests[name]; ok && !q.IsZero() {
		return q.MilliValue(), true
	}
	if q, ok := container.Resources.Limits[name]; ok && !q.IsZero() {
		return q.MilliValue(), true
	}
	return 0, false
}

func requestOrLimitBytes(container corev1.Container, name corev1.ResourceName) (int64, bool) {
	if q, ok := container.Resources.Requests[name]; ok && !q.IsZero() {
		return q.Value(), true
	}
	if q, ok := container.Resources.Limits[name]; ok && !q.IsZero() {
		return q.Value(), true
	}
	return 0, false
}

func clamp01(v float64) float64 {
	return math.Max(0, math.Min(1, v))
}

func podReady(pod corev1.Pod) bool {
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
