{{- define "rockstream.objectStoreSecretName" -}}
{{- if .Values.objectStore.existingSecret -}}
{{ .Values.objectStore.existingSecret }}
{{- else -}}
{{ .Release.Name }}-objectstore
{{- end -}}
{{- end -}}

{{- define "rockstream.objectStoreEnv" -}}
{{- if .Values.objectStore.enabled }}
- name: ROCKSTREAM_OBJECT_STORE_ENDPOINT
  value: {{ .Values.objectStore.endpoint | quote }}
- name: ROCKSTREAM_OBJECT_STORE_BUCKET
  value: {{ .Values.objectStore.bucket | quote }}
- name: ROCKSTREAM_OBJECT_STORE_REGION
  value: {{ .Values.objectStore.region | quote }}
- name: ROCKSTREAM_OBJECT_STORE_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "rockstream.objectStoreSecretName" . }}
      key: accessKey
- name: ROCKSTREAM_OBJECT_STORE_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "rockstream.objectStoreSecretName" . }}
      key: secretKey
{{- end }}
{{- end -}}
