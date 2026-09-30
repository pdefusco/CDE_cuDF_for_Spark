# CDE cuDF for Spark

Enabling **Spark GPU acceleration on Cloudera Data Engineering (CDE)** running on Cloudera Private
Cloud, as a step toward running a Spark RAPIDS (cuDF) workload on-prem.

**Outcome so far:** the GPU acceleration switch works and is verified on the cluster — but the GPU
Spark runtime images that switch selects are **not published to the registry this cluster can reach**,
so a GPU job gets `ImagePullBackOff` rather than a GPU. The procedure below is correct and reusable;
the blocker in section 6 is where it stops. Internal technical prototype, not a supported
configuration.

## 1. Environment

| | |
|---|---|
| Private Cloud | CE 1.5.5-h3000 (ECS / RKE2 v1.33.12) |
| Cloudera Manager | 7.13.2.6 |
| Runtime | 7.3.2.0 |
| CDE | 1.26.0-b557 |
| Spark | 3.5.4 |
| GPU workers | 3 × `g5.8xlarge` — 32 vCPU / 128 GiB / 1× NVIDIA A10G 24 GB each |
| Worker OS / arch | RHEL 9.6, **amd64** |
| NVIDIA driver | 615.71.09 (CUDA 13.4) |
| CDE service | `ds0928-de` / `cluster-j6th9czl` |
| Virtual cluster | `cuDFforSpark` |

**GPU budget is 3, total.** Each worker advertises `nvidia.com/gpu: 1` with
`gpu.sharing-strategy=none` and `mig.capable=false`, so 3 is both the cluster total and the
concurrent-GPU ceiling. Size `spark.executor.instances` against that, not against vCPU.

## 2. Cluster access

- **Management Console:** `https://console-cdp.apps.ecs.<gateway-ip>.nip.io`
  On the login page choose **local administrator**, then log in as `admin` with the password held in
  the `common_password` key of the deployment repo's `config.yml`. The default LDAP
  connector returns `temporarily_unavailable` for *every* identity, including ones that bind against
  FreeIPA cleanly — the control plane's `cm-ldap` provider is the broken part, not the directory.
- **Cloudera Manager:** `https://cm.<gateway-ip>.nip.io`
- **`kubectl`** lives on the ECS master, not locally:
  ```bash
  sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml get nodes
  ```
- **SSH to ECS nodes by IP, not FQDN.** The generated `<prefix>-ssh.config` contains
  `Host *.cldr.internal, 10.10.*`. A comma is not an OpenSSH pattern separator, so the first pattern
  parses as the literal string `*.cldr.internal,` — no FQDN matches it, and the whole block including
  `ProxyJump` silently never applies. The `10.10.*` token does parse, so IP-addressed connections
  work. The comma is baked into `tf_cluster_aws/hosts_common.tf` in the deployment repo, so it
  regenerates on every `terraform apply`.

## 3. Steps

### 3.1 Confirm the workers can schedule GPUs

```bash
sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml \
  get nodes -o json | jq -r '.items[] | "\(.metadata.name) gpu=\(.status.allocatable["nvidia.com/gpu"]) taints=\(.spec.taints // "NONE")"'
```

Expect `gpu=1` on each worker and `taints=NONE`. Because there are no taints, **no `podTemplateFile`
toleration work is needed** — this is often assumed necessary and is not.

### 3.2 Set the service-level GPU quota

```bash
cdp de update-service --cluster-id cluster-j6th9czl --gpu-requests 3
```

The **service does not need recreating** for this.

### 3.3 Create the virtual cluster with GPU acceleration enabled

This is the step that actually matters, and it is **create-only** — see section 5.

```bash
python scripts/create_vc_gpu.py --cluster-id cluster-j6th9czl --name cuDFforSpark
```

Install runs about 3 minutes: `AppInstallationInitiated` → `AppFSPolicyCreated` → `AppInstalling` →
`AppInstalled`.

### 3.4 Verify the flag actually landed

```bash
scripts/verify_gpu_vc.sh <vcId>
```

The decisive check is that the VC's `dex.yaml` carries `gpuAccelerationEnabled: true`:

```bash
kubectl get cm <vcId>-api-cm -n <vcId> -o jsonpath='{.data.dex\.yaml}' | grep gpuAccelerationEnabled
```

The script also confirms the yunikorn leaf queue exposes `nvidia.com/gpu: "3"` under `max`.

## 4. How a job would request a GPU

Not yet validated on this cluster — blocked by section 6. The configuration that is known to work for
**Spark 3.5.4** (carried over from prior Spark RAPIDS work on Cloudera AI) is in
`docs/rapids-spark-conf.md`.

## 5. Gotchas

- **GPU settings are frozen at VC creation.** `UpdateVcRequest` has no resource fields and no
  `chartValueOverrides` — its entire property set is `aclUsers, clusterId, discardSparkConfigs,
  enableComputeOverride, fullAccessGroups, fullAccessUsers, sparkConfigs, vcId, viewOnlyGroups,
  viewOnlyUsers`. A wrong value costs a delete + recreate, so get them right the first time.
- **A console-built VC gets `gpuAccelerationEnabled: false`.** The UI exposes GPU *quota* but not the
  acceleration flag, so a UI-built VC looks GPU-capable — `gpuRequests: 3` in `describeVc`,
  `nvidia.com/gpu: "3"` on its yunikorn queue — while every job silently runs the CPU image. Build the
  VC through the API.
- **`guaranteedGpuRequests` must be `0`, not 3.** The parent yunikorn queue has a `max` block and *no
  `guaranteed` block at all*, so a child GPU guarantee has nothing to draw from. `max: 3` is what
  permits GPU scheduling. yunikorn `max` is a ceiling, not a reservation, and GPU quota does **not**
  inherit down the tree — every level needs it named explicitly.
- **Omit `sparkVersion`.** The bundled `cdpcli` enum offers `SPARK3_5` and `SPARK3_5_4`, but this
  control plane rejects both with `400 INVALID_ARGUMENT`. Omit it and the server defaults correctly —
  `describeVc` then reports `sparkVersion: 3.5.4`. (Also: `vcTier: ALLP` on write reads back as
  `tier-2`. Same thing, different vocabulary.)
- **`AppDeleted` is terminal — do not poll for absence.** `deleteVc` reaches `AppDeleted` in ~15s, then
  the record soft-lingers in `listVcs` forever. "Poll until the vcId disappears" never terminates. Gate
  a rebuild on *"no VC of this name whose status lacks `Deleted`"*.
- **`cloudera.cloud.de_virtual_cluster` cannot do this.** It supports `chart_value_overrides` and
  `spark_version` but has no `gpu_requests` and no `guaranteed_*` parameters at all — it would create a
  VC with acceleration on and `nvidia.com/gpu: 0` on its leaf queue, i.e. nothing to schedule against.
  Use the direct API.
- **There is no VC automation in the deployment repo.** `playbooks/cde-service.yml` creates only the
  *service*, with `gpu_requests: null` and `chart_value_overrides: null`. The `cloudera.cloud.de`
  module does accept both, so the service-level quota (3.2) could move into the playbook later.
- **`spark-defaults-conf-config-map-<vcId>` naming the CPU image is correct, not a failure.** `gpuImage`
  is a separate key in `dex.yaml` that the runtime API server substitutes per job when a job requests
  GPUs. The acceleration flag is what *permits* that substitution.

## 6. Blocker: the GPU runtime images are not published for this build

Turning the flag on is necessary but **not sufficient — the images it selects do not exist.**

This cluster resolves images against `container.repository.cloudera.com/cdp-private/`
(`docker_registry` + `image_basepath` on the ECS service, with `external_registry_enabled: true`), so
the image CDE wants is:

```
container.repository.cloudera.com/cdp-private/dex-spark-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557
```

Verified with differential probe pods in the VC namespace — same node, same `imagePullSecrets`,
kubelet does the auth, so no credential handling is involved (see `probes/`):

| image | result |
|---|---|
| `dex-spark-runtime-3.5.4-7.3.2.0:1.26.0-b557` (CPU) | **pulls, runs, `Completed`** |
| `dex-livy-runtime-3.5.4-7.3.2.0:1.26.0-b557` (CPU) | **pulls, `Running`** |
| `dex-spark-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557` | `ErrImagePull` → **`NotFound`** |
| `dex-livy-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557` | `ErrImagePull` → **`NotFound`** |

Why this is conclusive rather than a credentials or mirror problem:

- The error is **`NotFound`, not `unauthorized`** — the registry resolved the reference and reported
  the tag absent.
- The **CPU variants pull**, which proves the credentials and the `cdp-private` path are fine.
- There is **no `registries.yaml`** on the ECS nodes, so nothing is rewriting the reference.

Conclusion: CDE 1.26.0-b557 references GPU runtime images in its chart that are not published to a
repository this cluster can reach — plausibly a paid entitlement that Private Cloud **Community
Edition** does not carry.

Also relevant to any workaround: the control plane runs `ContainerInfo.Mode: public` with
`CopyDocker: false`, and there is no Harbor/Nexus/Artifactory anywhere in the deployment. **There is no
local registry to push a replacement image to.**

Worth ruling out first: GPU *scheduling* is fine. The `nvidia` Ansible role deliberately does not run
`nvidia-ctk runtime configure` or install a device plugin, yet the workers advertise
`nvidia.com/gpu: 1` — ECS supplies the device plugin itself. The blocker really is only image
availability.

## 7. Next steps

**The escape hatch.** The chart templates the GPU image reference as

```
{{ .Values.dexapp.api.sparkRuntime.gpuImage.override | default (printf "%s/%s-%s-%s:%s" ...) }}
```

so `dexapp.api.sparkRuntime.gpuImage.override` (and the `livyRuntime` twin) can point at an arbitrary
image. Being a chart value it is **create-time only** — set it in the *same* `createVc` as the
acceleration flag rather than discovering that afterwards. The commented-out block in
`scripts/create_vc_gpu.py` is there for exactly this.

**Where to host such an image.** No local registry exists, and no pull-secret plumbing exists either
(`docker_user` / `docker_password` are commented out on the ECS service definition). Prefer a registry
that needs no auth from the cluster — the nodes egress via NAT. ECR in the same region or a public repo
are the candidates; a private registry means wiring up the pull secret by hand.

**Build architecture is the sharp edge.** Workers are amd64 RHEL 9.6; the build host is arm64 Apple
Silicon. Any image build needs an explicit `--platform linux/amd64` or it produces an image the cluster
cannot run.

**The RAPIDS version lock.** The jar, the shim override, the shuffle-manager class and the Spark
**patch** version are a four-way lock. See `docs/rapids-spark-conf.md` — for Spark 3.5.4 the proven
combination is `rapids-4-spark_2.12:26.02.0` with the `spark354` shim.

**RAPIDS falls back to CPU silently.** A green job proves nothing, and neither does a wall-clock
number on its own. Verify with `spark.rapids.sql.explain=NOT_ON_GPU` in the executor logs plus live
`nvidia-smi`.

## Repo layout

```
scripts/create_vc_gpu.py    the verified createVc, incl. the commented gpuImage.override block
scripts/verify_gpu_vc.sh    confirm gpuAccelerationEnabled, queue quota, node GPU capacity
probes/                     the differential image probe that established the blocker
docs/rapids-spark-conf.md   Spark RAPIDS configuration and the version-lock matrix
```
