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
{{- if .Values.fdb.clusterFile.configMap.name -}}
{{- .Values.fdb.clusterFile.configMap.key | default "cluster-file" -}}
{{- else -}}
{{- .Values.fdb.clusterFile.secret.key | default "fdb.cluster" -}}
{{- end -}}
{{- end -}}

{{- define "f4a-runner.clusterFileSourceName" -}}
{{- if .Values.fdb.clusterFile.configMap.name -}}
{{- .Values.fdb.clusterFile.configMap.name -}}
{{- else -}}
{{- .Values.fdb.clusterFile.secret.name -}}
{{- end -}}
{{- end -}}
