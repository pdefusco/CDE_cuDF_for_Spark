#!/usr/bin/env bash
# Spark GPU resource discovery, baked into the image.
#
# Spark calls this on each executor and parses the single line of JSON on stdout to learn which
# GPU indices the container may use. It must print NOTHING else — any stray output makes Spark
# fail to parse the resource allocation, and the failure message does not mention this script.
#
# Pointed at by:
#   spark.executor.resource.gpu.discoveryScript=/opt/cde/gpu/getGpusResources.sh
#
# This lives inside the image rather than on a mounted path. The CAI version of this work used
# /home/cdsw/getGpusResources.sh, which does not exist in a CDE executor.

set -uo pipefail

# `nvidia-smi` must exist in the container, which requires the NVIDIA container runtime to be
# wired into containerd on the host. If it is missing, print an empty address list rather than
# garbage: Spark then reports "0 GPUs" plainly instead of a JSON parse error, which is a far
# easier failure to diagnose.
if ! command -v nvidia-smi >/dev/null 2>&1; then
  echo '{"name": "gpu", "addresses": []}'
  exit 0
fi

# Join the index column into a quoted, comma-separated list. `paste -sd,` is used instead of the
# usual sed hold-space one-liner because it is legible and behaves identically for one GPU (the
# case here: 1 A10G per worker, no MIG, no sharing).
ADDRS=$(nvidia-smi --query-gpu=index --format=csv,noheader 2>/dev/null \
        | tr -d ' ' | grep -E '^[0-9]+$' | paste -sd, -)

if [[ -z "$ADDRS" ]]; then
  echo '{"name": "gpu", "addresses": []}'
  exit 0
fi

# Quote each index: 0,1 -> "0","1"
QUOTED=$(echo "$ADDRS" | sed 's/[^,][^,]*/"&"/g')
echo "{\"name\": \"gpu\", \"addresses\": [$QUOTED]}"
