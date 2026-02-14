# SerpMonitor Kubernetes Example

This example deploys a FoundationDB cluster using the FoundationDB operator and
installs the `f4a-runner` chart via a local umbrella chart.

## Quick Start with Tilt (Recommended)

[Tilt](https://tilt.dev) provides automatic rebuilding and redeployment on code changes.

```bash
# Install Tilt
brew install tilt

# Start minikube (if not running)
minikube start --memory=6144 --cpus=4 --disk-size=20g

# Run Tilt from the serpMonitor directory
cd examples/serpMonitor
tilt up
```

Open http://localhost:10350 to see the Tilt dashboard. Tilt will:

1. Install the FDB operator and CRDs
2. Build all Docker images
3. Deploy the Helm chart
4. Watch for code changes and auto-redeploy

Press `s` to stop, or run `tilt down` to tear everything down.

## Prebuilt Images (GHCR)

Prebuilt `gateway` and `dispatch` images are available in the GitHub Container Registry (GHCR)
on version tags (`v*`):

- `ghcr.io/futura-platform/f4a-gateway:<tag>`
- `ghcr.io/futura-platform/f4a-dispatch:<tag>`

## Manual Deployment

### Prerequisites

The umbrella chart includes:

- **f4a-runner**: Gateway, dispatch, and worker services
- **metrics-server**: Required for dispatch to collect pod resource metrics

The FDB operator must be installed separately (no published Helm repo).

### 1) Install the FoundationDB operator + CRDs

```bash
kubectl apply -f https://raw.githubusercontent.com/FoundationDB/fdb-kubernetes-operator/main/config/crd/bases/apps.foundationdb.org_foundationdbclusters.yaml
kubectl apply -f https://raw.githubusercontent.com/FoundationDB/fdb-kubernetes-operator/main/config/crd/bases/apps.foundationdb.org_foundationdbbackups.yaml
kubectl apply -f https://raw.githubusercontent.com/FoundationDB/fdb-kubernetes-operator/main/config/crd/bases/apps.foundationdb.org_foundationdbrestores.yaml
kubectl apply -f https://raw.githubusercontent.com/foundationdb/fdb-kubernetes-operator/main/config/samples/deployment.yaml
```

### 2) Build images

```bash
# Run from the f4a repo root.
minikube image build -t f4a-gateway:latest -f cmd/gateway/Dockerfile .
minikube image build -t f4a-dispatch:latest -f cmd/dispatch/Dockerfile .
minikube image build -t serpmonitor:latest -f examples/serpMonitor/Dockerfile .
```

For production, use the prebuilt GHCR images above (or push your own images) and
update `examples/serpMonitor/chart/values.yaml`.

### 3) Install the umbrella chart

```bash
helm dependency update examples/serpMonitor/chart
helm install serpmonitor examples/serpMonitor/chart
```

### 4) Verify deployment

```bash
# Check FDB cluster
kubectl get foundationdbcluster serpmonitor

# Check all pods are running
kubectl get pods

# Verify FDB is available
kubectl exec serpmonitor-log-0 -c foundationdb -- fdbcli --exec "status minimal"

# Check metrics are available
kubectl top pods
```

## Configuration

The umbrella chart configures:

- FDB cluster file passed to f4a services via ConfigMap
- metrics-server with `--kubelet-insecure-tls` for local clusters

To disable metrics-server (e.g., if already installed):

```yaml
metrics-server:
  enabled: false
```
