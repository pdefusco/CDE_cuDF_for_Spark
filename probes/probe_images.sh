#!/usr/bin/env bash
# Differential image probe: does the CDE GPU runtime image exist in the registry this
# cluster can reach?
#
# The design matters. Each probe pod is scheduled with the SAME namespace, the SAME
# imagePullSecrets and (optionally) the SAME node as the others, and kubelet performs the
# registry auth — so no credential handling happens here and nothing about the probe can
# explain a difference between the pods. The CPU images are the CONTROL: if they pull and
# the GPU ones do not, the difference is the image, not the plumbing.
#
# Run on the ECS master:
#   ./probe_images.sh <vcId>                 # namespace == vcId
#   NODE=ds0928-ecs-worker-01 ./probe_images.sh <vcId>
#
# Then read probes/README.md for how to interpret the result.

set -uo pipefail

VC_ID="${1:-}"
if [[ -z "$VC_ID" ]]; then
  echo "usage: $0 <vcId>   (the VC namespace, which also holds the imagePullSecrets)" >&2
  exit 2
fi

KUBECTL="${KUBECTL:-sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml}"
# Read straight off the live cluster rather than guessed — the `cloudera/dex` segment is easy
# to miss, and omitting it makes every probe return NotFound for the wrong reason:
#   kubectl get cm <vcId>-api-cm -n <vcId> -o jsonpath='{.data.dex\.yaml}' | grep -E 'image:|Image:'
REGISTRY="${REGISTRY:-container.repository.cloudera.com/cdp-private/cloudera/dex}"
BUILD="${BUILD:-1.26.0-b557}"
SPARK="${SPARK:-3.5.4-7.3.2.0}"
NODE="${NODE:-}"

# CPU variants first — they are the control and must pass for the result to mean anything.
IMAGES=(
  "dex-spark-runtime-${SPARK}:${BUILD}"
  "dex-livy-runtime-${SPARK}:${BUILD}"
  "dex-spark-runtime-gpu-${SPARK}:${BUILD}"
  "dex-livy-runtime-gpu-${SPARK}:${BUILD}"
)

# Reuse whatever pull secrets the VC namespace already has, so the probe authenticates
# exactly the way a real CDE job would.
SECRETS=$($KUBECTL get secrets -n "$VC_ID" \
  -o jsonpath='{range .items[?(@.type=="kubernetes.io/dockerconfigjson")]}{.metadata.name}{"\n"}{end}' 2>/dev/null)

echo "namespace:      $VC_ID"
echo "registry:       $REGISTRY"
echo "pull secrets:   ${SECRETS:-<none found>}"
# No apostrophe in the default: bash 3.2 reads a ' inside ${...} as opening a quote.
echo "pinned node:    ${NODE:-<any, scheduler decides>}"
echo

for image in "${IMAGES[@]}"; do
  # Pod names must be DNS-safe: strip the tag, replace dots. Parameter expansion rather
  # than a `tr` pipeline because bash 3.2 (macOS) mis-parses the nested quoting.
  base="${image%%:*}"
  name="probe-${base//./-}"
  name="${name:0:50}"
  $KUBECTL delete pod "$name" -n "$VC_ID" --ignore-not-found >/dev/null 2>&1

  {
    echo "apiVersion: v1"
    echo "kind: Pod"
    echo "metadata:"
    echo "  name: $name"
    echo "  namespace: $VC_ID"
    echo "  labels: {app: cde-gpu-image-probe}"
    echo "spec:"
    echo "  restartPolicy: Never"
    [[ -n "$NODE" ]] && echo "  nodeName: $NODE"
    if [[ -n "$SECRETS" ]]; then
      echo "  imagePullSecrets:"
      while read -r s; do [[ -n "$s" ]] && echo "  - name: $s"; done <<<"$SECRETS"
    fi
    echo "  containers:"
    echo "  - name: probe"
    echo "    image: ${REGISTRY}/${image}"
    echo "    command: ['/bin/sh', '-c', 'echo pulled ok; exit 0']"
  } | $KUBECTL apply -f - >/dev/null

  printf '%-58s ' "$image"

  # Poll briefly — a NotFound resolves fast; a successful pull of a multi-GB image does not.
  for _ in $(seq 1 60); do
    phase=$($KUBECTL get pod "$name" -n "$VC_ID" -o jsonpath='{.status.phase}' 2>/dev/null)
    reason=$($KUBECTL get pod "$name" -n "$VC_ID" \
      -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null)
    case "$phase" in
      Succeeded|Running) echo "$phase  (image pulled)"; break ;;
    esac
    case "$reason" in
      ErrImagePull|ImagePullBackOff)
        msg=$($KUBECTL get pod "$name" -n "$VC_ID" \
          -o jsonpath='{.status.containerStatuses[0].state.waiting.message}' 2>/dev/null)
        # The distinction that matters: NotFound (tag absent) vs unauthorized (credentials).
        if echo "$msg" | grep -qi 'not found\|manifest unknown'; then
          echo "$reason -> NOT FOUND (tag absent in registry)"
        elif echo "$msg" | grep -qi 'unauthor\|denied\|forbidden'; then
          echo "$reason -> UNAUTHORIZED (credential problem, not a missing tag)"
        else
          echo "$reason -> $(echo "$msg" | head -c 120)"
        fi
        break ;;
    esac
    sleep 5
  done
done

echo
echo "Cleanup:  $KUBECTL delete pods -n $VC_ID -l app=cde-gpu-image-probe"
