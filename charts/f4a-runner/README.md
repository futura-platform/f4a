# `f4a-runner` Helm Chart

The `f4a-runner` chart is published to GitHub Container Registry as an OCI artifact on tagged releases.

## Published Artifact

Release tags use a leading `v`, while the published chart version uses the same semantic version without the prefix.

- Git tag: `v0.1.0`
- OCI chart version: `0.1.0`
- OCI chart reference: `oci://ghcr.io/<owner>/f4a-runner`

Gateway and dispatch image tags are derived from the chart appVersion. In source, the chart appVersion defaults to `latest`. Published release charts override appVersion to the matching git tag, so those default image tags follow the release automatically.

## Authenticate

```bash
echo "${GITHUB_TOKEN}" | helm registry login ghcr.io \
  --username "${GITHUB_USER}" \
  --password-stdin
```

## Inspect a Release

```bash
helm show chart oci://ghcr.io/<owner>/f4a-runner --version 0.1.0
helm show values oci://ghcr.io/<owner>/f4a-runner --version 0.1.0
```

## Required Overrides

The published chart pins the default gateway and dispatch images to the matching release tag, but you still need to provide:

- `worker.image`
- `fdb.clusterFile.secret.name`

If you need to override the default gateway and dispatch tag, set `global.imageTag`.

## Startup Probe Tuning

The chart exposes a shared startup probe configuration at `global.startupProbe`, with optional per-workload overrides at:

- `gateway.startupProbe`
- `dispatch.startupProbe`
- `worker.startupProbe`

Example:

```bash
helm install f4a-runner oci://ghcr.io/<owner>/f4a-runner \
  --version 0.1.0 \
  --set worker.image=ghcr.io/<owner>/your-worker-image:v0.1.0 \
  --set fdb.clusterFile.secret.name=your-fdb-cluster-file-secret \
  --set global.startupProbe.failureThreshold=60 \
  --set worker.startupProbe.periodSeconds=10
```

## Additional Environment Variables

The chart lets you append extra Kubernetes `envFrom` and `env` entries to the gateway and dispatch containers. This is useful for OTEL configuration such as exporter endpoints, headers, and resource attributes.

Example values override:

```yaml
gateway:
  envFrom:
    - secretRef:
        name: otel-shared-env
  env:
    - name: OTEL_SERVICE_NAME
      value: f4a-gateway

dispatch:
  env:
    - name: OTEL_EXPORTER_OTLP_ENDPOINT
      value: http://otel-collector.observability.svc.cluster.local:4317
    - name: OTEL_RESOURCE_ATTRIBUTES
      value: service.name=f4a-dispatch,service.namespace=f4a
```

## Worker Autoscaling

Dispatch only assigns a task to a worker whose remaining declared CPU/memory can fit it; tasks that fit on no current worker wait in the pending set and are retried periodically. When configuring a KEDA scaler for the worker StatefulSet, include a trigger on the `pending_unschedulable_tasks` gauge (`> 0` means some task fits on no current worker) in addition to any utilization-based trigger: aggregate demand can sit below aggregate capacity while free resources are fragmented across workers, and only a fresh (empty) replica can host such a task. Scale-down removes the highest ordinals; dispatch deliberately fills the lowest ordinals first so that the pods being removed are empty and scale-down forces no task reschedules.

## Waiting For An Async Cluster File Secret

By default, the chart assumes `fdb.clusterFile.secret.name` already exists before pods start. If another controller or job creates that Secret later, enable `fdb.clusterFile.writable.wait.enabled` to make the init container poll the mounted source file until it exists and is non-empty.

Example:

```bash
helm install f4a-runner oci://ghcr.io/<owner>/f4a-runner \
  --version 0.1.0 \
  --set worker.image=ghcr.io/<owner>/your-worker-image:v0.1.0 \
  --set fdb.clusterFile.secret.name=your-fdb-cluster-file-secret \
  --set fdb.clusterFile.writable.wait.enabled=true \
  --set fdb.clusterFile.writable.wait.timeoutSeconds=600
```

## Install

```bash
helm install f4a-runner oci://ghcr.io/<owner>/f4a-runner \
  --version 0.1.0 \
  --set worker.image=ghcr.io/<owner>/your-worker-image:v0.1.0 \
  --set fdb.clusterFile.secret.name=your-fdb-cluster-file-secret
```

Override additional values as usual, for example:

```bash
helm install f4a-runner oci://ghcr.io/<owner>/f4a-runner \
  --version 0.1.0 \
  --set worker.image=ghcr.io/<owner>/your-worker-image:v0.1.0 \
  --set fdb.clusterFile.secret.name=your-fdb-cluster-file-secret \
  --set global.imageTag=v0.1.0 \
  --set gateway.replicas=2
```
