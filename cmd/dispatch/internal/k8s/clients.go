package k8s

import (
	"fmt"
	"os"

	"github.com/futura-platform/f4a/pkg/constants"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Clients struct {
	Core kubernetes.Interface
}

// LoadConfig loads the Kubernetes client configuration.
func LoadConfig() (*rest.Config, error) {
	if env := os.Getenv(constants.Kubeconfig); env != "" {
		return clientcmd.BuildConfigFromFlags("", env)
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load in-cluster config: %w", err)
	}
	return cfg, nil
}

// NewClients creates a Kubernetes client wrapper from the provided configuration.
func NewClients(cfg *rest.Config) (*Clients, error) {
	core, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Clients{Core: core}, nil
}
