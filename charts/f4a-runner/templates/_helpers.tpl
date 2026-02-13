{{- define "f4a-runner.gatewayName" -}}
{{- .Values.gateway.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "f4a-runner.dispatchName" -}}
{{- .Values.dispatch.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "f4a-runner.workerName" -}}
{{- .Values.worker.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "f4a-runner.dispatchServiceAccountName" -}}
{{- .Values.dispatch.serviceAccountName | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "f4a-runner.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "f4a-runner.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "f4a-runner.clusterFileKey" -}}
{{- .Values.fdb.clusterFile.secret.key | default "fdb.cluster" -}}
{{- end -}}

{{- define "f4a-runner.clusterFileSecretName" -}}
{{- .Values.fdb.clusterFile.secret.name -}}
{{- end -}}

{{- define "f4a-runner.clusterFileInitContainer" -}}
- name: fdb-cluster-init
  image: {{ .Values.fdb.clusterFile.writable.initImage | quote }}
  args:
    - --mode
    - init
    - --input-dir
    - {{ .Values.fdb.clusterFile.writable.sourceMountPath | quote }}
    - --output-dir
    - {{ .Values.fdb.clusterFile.writable.mountPath | quote }}
    - --copy-file
    - {{ .clusterFileKey | quote }}
    - --require-not-empty
    - {{ .clusterFileKey | quote }}
  volumeMounts:
    - name: fdb-cluster-source
      mountPath: {{ .Values.fdb.clusterFile.writable.sourceMountPath }}
      readOnly: true
    - name: fdb-cluster
      mountPath: {{ .Values.fdb.clusterFile.writable.mountPath }}
{{- end -}}

{{- define "f4a-runner.clusterFileVolumeMounts" -}}
{{- if .Values.fdb.clusterFile.writable.enabled }}
- name: fdb-cluster
  mountPath: {{ .Values.fdb.clusterFile.writable.mountPath }}
{{- else }}
- name: fdb-cluster-source
  mountPath: {{ .Values.fdb.clusterFile.writable.sourceMountPath }}
  readOnly: true
{{- end }}
{{- end -}}

{{- define "f4a-runner.clusterFileVolumes" -}}
- name: fdb-cluster-source
  secret:
    secretName: {{ .clusterFileSecretName }}
    items:
      - key: {{ .clusterFileKey | quote }}
        path: {{ .clusterFileKey | quote }}
{{- if .Values.fdb.clusterFile.writable.enabled }}
- name: fdb-cluster
  emptyDir: {}
{{- end }}
{{- end -}}
