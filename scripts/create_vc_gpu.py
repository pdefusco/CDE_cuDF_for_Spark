#!/usr/bin/env python3
"""Create a CDE virtual cluster with Spark GPU acceleration enabled.

GPU acceleration is a *chart value*, reachable only through `chartValueOverrides` on
`createVc`. `UpdateVcRequest` carries no resource fields and no chart overrides at all, so
everything set here is frozen for the life of the virtual cluster — a wrong value means
delete + recreate. Get it right the first time.

Verified against CDE 1.26.0-b557 on Private Cloud CE 1.5.5-h3000.

    python scripts/create_vc_gpu.py --cluster-id cluster-j6th9czl --name cuDFforSpark
"""

import argparse
import sys

# Space-separated "key:value" pairs, NOT a dict. The server appends its own entries to
# whatever is passed (it adds `airflow.enabled:true`), so this is a floor, not the final set.
GPU_CHART_OVERRIDES = " ".join(
    [
        "dexapp.api.gpuAcceleration.enabled:true",
        "safari.enabled:false",
        "pipelines.enabled:true",
        # --- The escape hatch for a missing/unreachable GPU runtime image -----------------
        # CDE 1.26.0-b557 references GPU runtime images that are not published to the
        # registry a CE cluster can reach, so the flag above permits a substitution that
        # then fails to pull. The chart templates the reference as
        #   {{ .Values.dexapp.api.sparkRuntime.gpuImage.override | default (printf ...) }}
        # so these two point it at an arbitrary image instead. Both are chart values, i.e.
        # CREATE-TIME ONLY — uncomment them in *this* call rather than discovering later
        # that they cost another recreate. See README section 7.
        # "dexapp.api.sparkRuntime.gpuImage.override:<registry>/<repo>:<tag>",
        # "dexapp.api.livyRuntime.gpuImage.override:<registry>/<repo>:<tag>",
    ]
)


def build_request(cluster_id: str, name: str) -> dict:
    return {
        "clusterId": cluster_id,
        "name": name,
        "vcTier": "ALLP",  # reads back as "tier-2" in describeVc — same thing
        "cpuRequests": "24",
        "memoryRequests": "64Gi",
        "gpuRequests": "3",  # cluster total: 1 A10G per worker, no sharing, no MIG
        "guaranteedCpuRequests": "12",
        "guaranteedMemoryRequests": "32Gi",
        # MUST be 0. The parent yunikorn queue has a `max` block and no `guaranteed` block
        # at all, so a child GPU guarantee has nothing to draw from. `max: 3` is what
        # actually permits GPU scheduling.
        "guaranteedGpuRequests": "0",
        "chartValueOverrides": [
            {"chartName": "dex-app", "overrides": GPU_CHART_OVERRIDES}
        ],
        # NOTE: `sparkVersion` is deliberately absent. The bundled cdpcli enum offers
        # SPARK3_5 / SPARK3_5_4 but this control plane rejects both with
        # 400 INVALID_ARGUMENT (client enum is newer than the server). Omitting it lets the
        # server default correctly — describeVc then reports sparkVersion: 3.5.4.
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cluster-id", required=True, help="CDE service cluster ID, e.g. cluster-j6th9czl"
    )
    parser.add_argument("--name", required=True, help="Virtual cluster name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the request without sending it",
    )
    args = parser.parse_args()

    request = build_request(args.cluster_id, args.name)

    if args.dry_run:
        import json

        print(json.dumps(request, indent=2))
        return 0

    try:
        from cdpy.common import CdpcliWrapper
    except ImportError:
        print(
            "cdpy is not installed. Either `pip install cdpy`, or send the same request "
            "with:\n  python scripts/create_vc_gpu.py ... --dry-run | "
            "cdp de create-vc --cli-input-json file:///dev/stdin",
            file=sys.stderr,
        )
        return 1

    result = CdpcliWrapper().call(svc="de", func="create_vc", **request)
    print(result)

    # Expect roughly 3 minutes of:
    #   AppInstallationInitiated -> AppFSPolicyCreated -> AppInstalling -> AppInstalled
    # Then confirm the flag actually landed:
    #   scripts/verify_gpu_vc.sh <vcId>
    print(
        f"\nSubmitted. Poll with: cdp de describe-vc --cluster-id {args.cluster_id} "
        "--vc-id <vcId>\nThen verify: scripts/verify_gpu_vc.sh <vcId>"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
