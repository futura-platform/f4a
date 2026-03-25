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
