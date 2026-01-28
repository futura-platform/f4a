package k8s

import (
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

type Clients struct {
	Core    kubernetes.Interface
	Metrics metricsclient.Interface
}

func LoadConfig() (*rest.Config, error) {
	if env := os.Getenv("KUBECONFIG"); env != "" {
		return clientcmd.BuildConfigFromFlags("", env)
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load in-cluster config: %w", err)
	}
	return cfg, nil
}

func NewClients(cfg *rest.Config) (*Clients, error) {
	core, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	metrics, err := metricsclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Clients{Core: core, Metrics: metrics}, nil
}
