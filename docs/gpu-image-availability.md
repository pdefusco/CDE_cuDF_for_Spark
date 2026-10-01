# Why CDE 1.26's own GPU runtime images could not be used

Background detail for README §6. The short version is in the README; this file holds the evidence, so
that the claim stays **checkable** and the probe stays **re-runnable** after any registry, entitlement,
or CDE version change.

Turning on `gpuAccelerationEnabled` is necessary but **not sufficient — the images it selects do not
exist on any registry this cluster can reach.** With the flag on and no custom runtime, a GPU job gets
`ImagePullBackOff` rather than a GPU.

## What CDE actually tries to pull

Read it off the cluster rather than reconstructing it. The VC's own `dex.yaml` is the authoritative
answer:

```bash
kubectl get cm <vcId>-api-cm -n <vcId> -o jsonpath='{.data.dex\.yaml}' | grep -E 'image:|Image:'
```

On this VC (`dex-app-qkckw9t9`, checked 2026-09-30):

```
image:    container.repository.cloudera.com/cdp-private/cloudera/dex/dex-livy-runtime-3.5.4-7.3.2.0:1.26.0-b557
gpuImage: container.repository.cloudera.com/cdp-private/cloudera/dex/dex-livy-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557
gpuImage: container.repository.cloudera.com/cdp-private/cloudera/dex/dex-spark-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557
```

Note the **`cloudera/dex/`** segment. Every image actually running in the VC namespace carries it
(`dex-runtime-api-server`, `dex-livy-server-3.5.4-…`, `dex-spark-history-server-3.5.4-…`), so
`cdp-private/` alone is not where these live. An earlier draft of this analysis dropped that segment —
a wrong prefix returns `NotFound` for *every* image, GPU or not, and therefore tells you nothing.

## The differential probe

`probes/probe_images.sh` runs one pod per image in the VC namespace: same node, same
`imagePullSecrets`. **Kubelet does the authentication**, so the probe handles no credentials itself.
All four were probed under the `cdp-private/cloudera/dex/` prefix above.

| image | result |
|---|---|
| `dex-spark-runtime-3.5.4-7.3.2.0:1.26.0-b557` (CPU) | **pulls, runs, `Completed`** |
| `dex-livy-runtime-3.5.4-7.3.2.0:1.26.0-b557` (CPU) | **pulls, `Running`** |
| `dex-spark-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557` | `ErrImagePull` → **`NotFound`** |
| `dex-livy-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557` | `ErrImagePull` → **`NotFound`** |

Why that is conclusive rather than a credentials, mirror, or wrong-path problem — the CPU rows are in
the list precisely to rule those out:

- The error is **`NotFound`, not `unauthorized`.** The registry resolved the reference and reported the
  tag absent. That is an availability answer, not a credentials one.
- The **CPU variants pull from the same prefix.** That rules out credentials *and* a mistyped path in
  one step.
- There is **no `registries.yaml`** on the ECS nodes, so nothing is rewriting the reference.

**Conclusion:** CDE 1.26.0-b557 references GPU runtime images in its chart that are not published to a
repository this cluster can reach — plausibly a paid entitlement that Private Cloud **Community
Edition** does not carry.

## Ruled out first: GPU scheduling is fine

The `nvidia` Ansible role deliberately does not run `nvidia-ctk runtime configure` and does not install
a device plugin, yet the workers advertise `nvidia.com/gpu: 1` — ECS supplies the device plugin itself.
So the blocker really is only image availability, not scheduling or device exposure.

## No local registry to push to either

The control plane runs `ContainerInfo.Mode: public` with `CopyDocker: false`, and there is no
Harbor / Nexus / Artifactory anywhere in the deployment. There is also no pull-secret plumbing
(`docker_user` / `docker_password` are commented out on the ECS service definition).

This shaped the workaround: prefer a registry needing **no auth from the cluster** — the nodes egress
via NAT. A public Docker Hub repo is what we used; ECR in the same region is the other candidate. A
private registry would mean wiring up the pull secret by hand.

## The chart escape hatch, if a custom runtime were ever not an option

The chart templates the GPU image reference as

```
{{ .Values.dexapp.api.sparkRuntime.gpuImage.override | default (printf "%s/%s-%s-%s:%s" ...) }}
```

so `dexapp.api.sparkRuntime.gpuImage.override` (and the `livyRuntime` twin) can point at an arbitrary
image. Being a chart value it is **create-time only** — it must go in the *same* `createVc` as the
acceleration flag. The commented-out block in `scripts/create_vc_gpu.py` is there for exactly this.

A CDE **custom runtime** is strictly better where it works: it needs no VC recreate, and it overrides
the `gpuImage` selection outright. See
[`custom-gpu-runtime.md`](custom-gpu-runtime.md).

## Re-probing

```bash
probes/probe_images.sh <vcId>
```

Cheapest way to tell whether a registry or entitlement change has made the supported path available.
**If Cloudera's GPU runtime appears — or just its Cloudera-built RAPIDS jar — delete the patch in
`runtime/` and use it.**
