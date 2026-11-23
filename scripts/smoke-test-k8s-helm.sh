#!/usr/bin/env bash
# Kubernetes & Helm S3 Kit Verification + k3d/MinIO Smoke Test (Issue #405 R5 + Verification #4)
# Usage:
#   ./scripts/smoke-test-k8s-helm.sh          # Helm lint + rendered manifest contract checks
#   ./scripts/smoke-test-k8s-helm.sh --k3d    # Full k3d + MinIO deploy, scale (gw 1->3, worker 2->4), worker kill, /ready & /metrics
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/deploy/helm/rockstream"
RUN_K3D=0
CLUSTER_NAME="rockstream-smoke-405"

for arg in "$@"; do
  case "$arg" in
    --k3d) RUN_K3D=1 ;;
    *)
      echo "Unknown option: $arg" >&2
      echo "Usage: $0 [--k3d]" >&2
      exit 2
      ;;
  esac
done

echo "==> Running helm lint on $CHART..."
helm lint "$CHART"
helm lint "$CHART" -f "$CHART/values-k3d-smoke.yaml"

echo "==> Rendering Helm chart with full S3, HPA, compactor, ServiceMonitor, Grafana, and MinIO enabled..."
RENDERED="$(mktemp)"
trap 'rm -f "$RENDERED"' EXIT
helm template rockstream "$CHART" \
  -f "$CHART/values-k3d-smoke.yaml" \
  --set monitoring.serviceMonitor.enabled=true \
  --set worker.autoscaling.enabled=true \
  > "$RENDERED"

for required in \
  "kind: StatefulSet" \
  "kind: Deployment" \
  "kind: HorizontalPodAutoscaler" \
  "kind: ServiceMonitor" \
  "kind: Secret" \
  "ROCKSTREAM_OBJECT_STORE_ENDPOINT" \
  "ROCKSTREAM_OBJECT_STORE_BUCKET" \
  "ROCKSTREAM_OBJECT_STORE_REGION" \
  "ROCKSTREAM_OBJECT_STORE_ACCESS_KEY" \
  "ROCKSTREAM_OBJECT_STORE_SECRET_KEY" \
  "rockstream-compactor" \
  "rockstream-grafana-dashboard" \
  "rockstream-minio" \
  "path: /ready" \
  "path: /metrics"; do
  if ! grep -Fq "$required" "$RENDERED"; then
    echo "FAIL: Rendered Helm manifest missing required element: $required" >&2
    exit 1
  fi
done
echo "OK: Helm lint and manifest contract checks passed."

if [ "$RUN_K3D" -eq 0 ]; then
  exit 0
fi

echo "==> Running live k3d + MinIO smoke test..."
cleanup_k3d() {
  rm -f "$RENDERED"
  k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
}
trap cleanup_k3d EXIT

k3d cluster create "$CLUSTER_NAME" --wait
docker build -t rockstream:local "$ROOT"
k3d image import rockstream:local -c "$CLUSTER_NAME"

helm upgrade --install rockstream "$CHART" \
  -f "$CHART/values-k3d-smoke.yaml" \
  --wait --timeout 5m

echo "==> Scaling stateless gateways (1 -> 3) and workers (2 -> 4)..."
kubectl scale deployment rockstream-gateway --replicas=3
kubectl scale statefulset rockstream-worker --replicas=4
kubectl rollout status deployment/rockstream-gateway --timeout=3m
kubectl rollout status statefulset/rockstream-worker --timeout=3m

echo "==> Killing a worker pod and verifying automatic recovery..."
kubectl delete pod rockstream-worker-1 --grace-period=0 --force
kubectl wait --for=condition=Ready pod/rockstream-worker-1 --timeout=3m

echo "==> Verifying /ready and /metrics on gateway..."
kubectl port-forward svc/rockstream-gateway 19090:9090 >/dev/null 2>&1 &
PF_PID=$!
sleep 2
curl -sf http://127.0.0.1:19090/ready >/dev/null
curl -sf http://127.0.0.1:19090/metrics | grep -q "rockstream"
kill "$PF_PID" 2>/dev/null || true

echo "OK: k3d + MinIO smoke test passed (scale-out, worker kill recovery, /ready, /metrics)."
