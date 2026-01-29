# SerpMonitor Kubernetes Example

This example deploys a FoundationDB cluster using the FoundationDB operator and
installs the `f4a-runner` chart via a local umbrella chart.

## 1) Install the FoundationDB operator + CRDs

Follow the official operator instructions:
https://github.com/FoundationDB/fdb-kubernetes-operator?tab=readme-ov-file#running-the-operator

```bash
kubectl apply -f https://raw.githubusercontent.com/FoundationDB/fdb-kubernetes-operator/main/config/crd/bases/apps.foundationdb.org_foundationdbclusters.yaml
kubectl apply -f https://raw.githubusercontent.com/FoundationDB/fdb-kubernetes-operator/main/config/crd/bases/apps.foundationdb.org_foundationdbbackups.yaml
kubectl apply -f https://raw.githubusercontent.com/FoundationDB/fdb-kubernetes-operator/main/config/crd/bases/apps.foundationdb.org_foundationdbrestores.yaml
kubectl apply -f https://raw.githubusercontent.com/foundationdb/fdb-kubernetes-operator/main/config/samples/deployment.yaml
```

## 2) Build and publish images

Build the SerpMonitor worker image:

```bash
docker build -t serpmonitor:latest -f examples/serpMonitor/Dockerfile .
```

The `f4a-runner` chart also needs images for the gateway and dispatch services.
Update `examples/serpMonitor/chart/values.yaml` with the image tags you publish.

All images must include the FoundationDB client library appropriate for the
FDB version you run.

## 3) Install the umbrella chart

From the repository root:

```bash
helm dependency update examples/serpMonitor/chart
helm install serpmonitor examples/serpMonitor/chart
```

The FoundationDB operator creates a ConfigMap named
`<clusterName>-config` containing the `cluster-file` key. The umbrella values
wire that into the `f4a-runner` chart and copy it into a writable `emptyDir`
before the services start.

Check cluster reconciliation:

```bash
kubectl get foundationdbcluster serpmonitor
```
