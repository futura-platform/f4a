package main

import (
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/cmd/dispatch/internal/k8s"
	"github.com/futura-platform/f4a/cmd/dispatch/internal/scheduler"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

func TestLoadConfigUsesDefaultBatchParallelism(t *testing.T) {
	setRequiredDispatchEnv(t)
	t.Setenv(constants.AssignmentBatchParallelism, "")

	cfg, leaderElectionName, err := loadConfig()
	require.NoError(t, err)
	require.Equal(t, "test-leader-election", leaderElectionName)
	require.Equal(t, scheduler.DefaultBatchParallelism, cfg.BatchTxParallelism)
}

func TestLoadConfigUsesEnvBatchParallelism(t *testing.T) {
	setRequiredDispatchEnv(t)
	t.Setenv(constants.AssignmentBatchParallelism, "7")

	cfg, _, err := loadConfig()
	require.NoError(t, err)
	require.Equal(t, 7, cfg.BatchTxParallelism)
}

func TestLoadConfigRejectsInvalidBatchParallelism(t *testing.T) {
	setRequiredDispatchEnv(t)
	t.Setenv(constants.AssignmentBatchParallelism, "0")

	_, _, err := loadConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, constants.AssignmentBatchParallelism)
}

func setRequiredDispatchEnv(t *testing.T) {
	t.Helper()
	t.Setenv(constants.Namespace, "test-namespace")
	t.Setenv(constants.PodNamespace, "")
	t.Setenv(constants.StatefulSetName, "test-statefulset")
	t.Setenv(constants.LeaderElectionName, "test-leader-election")
	t.Setenv(constants.MetricsInterval, "5s")
	t.Setenv(constants.ScoreEmaAlpha, "0.5")
}

func TestRunWithLeaderElection(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		testCfg := scheduler.Config{
			Namespace:          "test-namespace",
			StatefulSetName:    "test-statefulset",
			MetricsInterval:    5 * time.Second,
			ScoreAlpha:         0.5,
			BatchTxParallelism: 1,
		}
		testRunnerId := "test-runner-id"

		t.Run("a single scheduler instance runs and becomes leader, which starts scheduling pending tasks", func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testCfg.StatefulSetName,
					Namespace: testCfg.Namespace,
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": testCfg.StatefulSetName},
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testRunnerId,
					Namespace: testCfg.Namespace,
					Labels:    map[string]string{"app": testCfg.StatefulSetName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "runner",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
							},
						},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{
						{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						},
					},
				},
			}
			podMetrics := &metricsv1beta1.PodMetrics{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testRunnerId,
					Namespace: testCfg.Namespace,
					Labels:    map[string]string{"app": testCfg.StatefulSetName},
				},
				Containers: []metricsv1beta1.ContainerMetrics{
					{
						Name: "runner",
						Usage: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("50m"),
							corev1.ResourceMemory: resource.MustParse("64Mi"),
						},
					},
				},
			}
			metricsClient := metricsfake.NewSimpleClientset()
			err := metricsClient.Tracker().Create(
				metricsv1beta1.SchemeGroupVersion.WithResource("pods"),
				podMetrics,
				testCfg.Namespace,
			)
			require.NoError(t, err)
			clients := &k8s.Clients{
				Core: fake.NewClientset(sts, pod),
				// ignore this deprecation, k8s.io/metrics v0.36.0 should fix this when it's released
				Metrics: metricsClient,
			}

			pendingSet, err := servicestate.CreateOrOpenReadySet(db, db)
			require.NoError(t, err)
			mockedRunnerTaskSet, err := pool.CreateOrOpenTaskSetForRunner(db, db, testRunnerId)
			require.NoError(t, err)
			tasksDir, err := task.CreateOrOpenTasksDirectory(db)
			require.NoError(t, err)
			activeRunners, err := pool.CreateOrOpenActiveRunners(db)
			require.NoError(t, err)
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				activeRunners.SetActive(tx, testRunnerId, true)
				return nil, nil
			})
			require.NoError(t, err)

			schedulerErr := make(chan error, 1)
			go func() {
				schedulerErr <- runWithLeaderElection(t.Context(), testCfg, "test-leader-election", db, clients)
			}()

			initial, changes, errCh, err := mockedRunnerTaskSet.Stream(t.Context())
			require.NoError(t, err)
			assert.Zero(t, initial.Cardinality())

			const testTaskId = "test-task-id"
			taskKey, err := tasksDir.Create(db, task.Id(testTaskId))
			require.NoError(t, err)

			// place a task in the pending set
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				taskKey.RunnerId().Set(tx, nil)
				taskKey.LifecycleStatus().Set(tx, task.LifecycleStatusPending)
				if err := pendingSet.Add(tx, []byte(testTaskId)); err != nil {
					return nil, err
				}
				return nil, nil
			})
			require.NoError(t, err)

			// wait for the scheduler to assign the task
			select {
			case err := <-schedulerErr:
				t.Fatalf("scheduler error: %v", err)
			case err := <-errCh:
				t.Fatalf("stream error: %v", err)
			case change := <-changes:
				assert.Equal(t, []reliableset.LogEntry{{
					Op:    reliableset.LogOperationAdd,
					Value: []byte(testTaskId),
				}}, change)
			case <-time.After(10 * time.Second):
				t.Fatalf("timed out waiting for change")
			}
		})
	})
}
