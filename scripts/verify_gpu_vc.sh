#!/usr/bin/env bash
# Verify that a CDE virtual cluster is genuinely GPU-enabled.
#
# Run this on the ECS master, where kubectl and the RKE2 kubeconfig live. SSH there by IP,
# not FQDN — the generated ssh config has a comma bug that disables ProxyJump for FQDNs.
#
#   ./verify_gpu_vc.sh <vcId>
#
# Override the kubectl invocation if yours differs:
#   KUBECTL="kubectl" ./verify_gpu_vc.sh <vcId>

set -uo pipefail

VC_ID="${1:-}"
if [[ -z "$VC_ID" ]]; then
  echo "usage: $0 <vcId>" >&2
  exit 2
fi

KUBECTL="${KUBECTL:-sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml}"

echo "==> 1. Worker GPU capacity and taints"
# Expect nvidia.com/gpu: 1 per worker and no taints. No taints means no podTemplateFile
# toleration work is needed.
$KUBECTL get nodes -o json \
  | jq -r '.items[] | "\(.metadata.name)  gpu=\(.status.allocatable["nvidia.com/gpu"] // "none")  taints=\(if .spec.taints then (.spec.taints | map(.key) | join(",")) else "NONE" end)"'

echo
echo "==> 2. gpuAccelerationEnabled in the VC's dex.yaml  (MUST print true)"
# This is the decisive check. A console-built VC reports GPU *quota* correctly while this
# flag is false, and then every job silently runs the CPU image.
if ! $KUBECTL get cm "${VC_ID}-api-cm" -n "$VC_ID" \
      -o jsonpath='{.data.dex\.yaml}' 2>/dev/null | grep gpuAccelerationEnabled; then
  echo "  !! could not read ${VC_ID}-api-cm in namespace ${VC_ID}" >&2
fi

echo
echo "==> 3. yunikorn queue GPU ceiling for this VC  (expect nvidia.com/gpu: \"3\" under max)"
# GPU quota does not inherit down the queue tree, so every level needs it named explicitly.
# `max` is a ceiling, not a reservation — which is why guaranteedGpuRequests must be 0.
$KUBECTL get cm -n yunikorn -o yaml 2>/dev/null \
  | grep -B 12 'nvidia.com/gpu' \
  | grep -E "name:|nvidia.com/gpu|max" \
  || echo "  (no GPU entry found in the yunikorn queue config)"

echo
echo "==> 4. Which image the VC's spark defaults name"
# A CPU image here is CORRECT, not a failure: gpuImage is a separate dex.yaml key that the
# runtime API server substitutes per job when a job requests GPUs.
$KUBECTL get cm "spark-defaults-conf-config-map-${VC_ID}" -n "$VC_ID" \
  -o yaml 2>/dev/null | grep -o 'dex-[a-z-]*runtime[^ "]*' | sort -u \
  || echo "  (spark defaults configmap not found)"
