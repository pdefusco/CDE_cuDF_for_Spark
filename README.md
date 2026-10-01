# CDE cuDF for Spark

Enabling **Spark GPU acceleration on Cloudera Data Engineering (CDE)** running on Cloudera Private
Cloud, as a step toward running a Spark RAPIDS (cuDF) workload on-prem.

**Outcome: working.** A Spark RAPIDS query runs end-to-end on an NVIDIA A10G on CDE 1.26, verified by
an `isFinalPlan=true` plan containing 8 `Gpu*` operators and zero CPU operators below
`GpuColumnarToRow`.

Getting there meant routing around **two** separate blockers:

1. CDE 1.26's own GPU runtime images are not published to any registry this cluster can reach
   (section 6 — still true, and re-probeable).
2. The OSS RAPIDS jar **cannot link** against Cloudera's Spark `3.5.4.1.26.732.0-45` — Cloudera added a
   defaulted 6th parameter to `MapPartitionsRDD`, which is source-compatible but binary-incompatible,
   so every GPU plan died at the first action with `NoSuchMethodError`.

The second is fixed by recompiling **two unmodified upstream classes** (of 17,833) against the
cluster's own `spark-core`, using the CDE base image as its own toolchain, and shipping the result as a
**CDE custom runtime** — which needs no VC recreate.

➡ **Runbook, exact version strings, Docker + CDE CLI steps, and verified output:
[`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md)**
➡ Why the patch is legitimate and what it does *not* cover:
[`runtime/README-patch.md`](runtime/README-patch.md)

Internal technical prototype, not a supported configuration. The runtime image contains a modified
third-party artifact.

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

## 4. How a job requests a GPU

**Validated on this cluster** — job run 6, `Succeeded`, 8 `Gpu*` operators in the final plan. The full
procedure (build, push, `cde resource create`, `cde spark submit`) and the verified driver output are in
[`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md). The shortest form:

```bash
cde --config-profile <profile> resource create \
  --name cde-rapids-runtime-p1 --type custom-runtime-image --image-engine spark3 \
  --image pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-p1

RUNTIME_RESOURCE=cde-rapids-runtime-p1 ./scripts/submit_rapids_smoke_test.sh
```

The version-lock matrix (jar ↔ shim ↔ shuffle manager ↔ Spark patch version) is in
`docs/rapids-spark-conf.md`.

**Sizing must go through CDE's native flags, not `--conf`** — CDE applies its job spec after merging
`--conf`, so `--conf spark.executor.memory=8g` is silently replaced by the spec default of 1g. And a 1g
driver is cgroup-OOM-killed with no Java stack trace: the log just stops.

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
- **Do not look for the runtime image in the VC's spark defaults.** `<vcId>-spark-defaults` contains no
  image reference at all (checked on the live VC). The images live in `dex.yaml` as the sibling keys
  `image` (CPU) and `gpuImage`, and the runtime API server picks between them per job. So `image`
  pointing at a CPU runtime is correct, not a failure — the acceleration flag is what *permits* the
  `gpuImage` substitution.

## 6. Blocker 1: the GPU runtime images are not published for this build

**Still true** — but routed around with a custom runtime (section 4), which overrides the VC's
`gpuImage` outright. Keep this section because the probe is the cheapest way to tell whether a registry
or entitlement change has made the supported path available.

Turning the flag on is necessary but **not sufficient — the images it selects do not exist.**

The VC's own `dex.yaml` names exactly what CDE will try to pull. Read it off the cluster rather than
reconstructing it — that is the authoritative answer:

```bash
kubectl get cm <vcId>-api-cm -n <vcId> -o jsonpath='{.data.dex\.yaml}' | grep -E 'image:|Image:'
```

On this VC (`dex-app-qkckw9t9`, checked 2026-09-30) that yields:

```
image:    container.repository.cloudera.com/cdp-private/cloudera/dex/dex-livy-runtime-3.5.4-7.3.2.0:1.26.0-b557
gpuImage: container.repository.cloudera.com/cdp-private/cloudera/dex/dex-livy-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557
gpuImage: container.repository.cloudera.com/cdp-private/cloudera/dex/dex-spark-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557
```

Note the **`cloudera/dex/`** segment. Every image actually running in the VC namespace carries it
(`dex-runtime-api-server`, `dex-livy-server-3.5.4-...`, `dex-spark-history-server-3.5.4-...`), so
`cdp-private/` alone is not where these live.

Verified with differential probe pods in the VC namespace — same node, same `imagePullSecrets`,
kubelet does the auth, so no credential handling is involved (see `probes/`). All four were probed
under the `cdp-private/cloudera/dex/` prefix above:

| image | result |
|---|---|
| `dex-spark-runtime-3.5.4-7.3.2.0:1.26.0-b557` (CPU) | **pulls, runs, `Completed`** |
| `dex-livy-runtime-3.5.4-7.3.2.0:1.26.0-b557` (CPU) | **pulls, `Running`** |
| `dex-spark-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557` | `ErrImagePull` → **`NotFound`** |
| `dex-livy-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557` | `ErrImagePull` → **`NotFound`** |

Why this is conclusive rather than a credentials, mirror, or wrong-path problem:

- The error is **`NotFound`, not `unauthorized`** — the registry resolved the reference and reported
  the tag absent. That is an availability answer, not a credentials one.
- The **CPU variants pull from the same prefix**, which is the whole reason they are in the probe list.
  They rule out credentials *and* rule out a mistyped repository path.
- There is **no `registries.yaml`** on the ECS nodes, so nothing is rewriting the reference.

Conclusion: CDE 1.26.0-b557 references GPU runtime images in its chart that are not published to a
repository this cluster can reach — plausibly a paid entitlement that Private Cloud **Community
Edition** does not carry. With the flag on, a GPU job gets `ImagePullBackOff` rather than a GPU.

Keep the prefix in mind if you re-probe: an earlier draft of this file dropped the `cloudera/dex/`
segment. A wrong prefix returns `NotFound` for *every* image, GPU or not, and tells you nothing.

Also relevant to any workaround: the control plane runs `ContainerInfo.Mode: public` with
`CopyDocker: false`, and there is no Harbor/Nexus/Artifactory anywhere in the deployment. **There is no
local registry to push a replacement image to.**

Worth ruling out first: GPU *scheduling* is fine. The `nvidia` Ansible role deliberately does not run
`nvidia-ctk runtime configure` or install a device plugin, yet the workers advertise
`nvidia.com/gpu: 1` — ECS supplies the device plugin itself. The blocker really is only image
availability.

## 7. Blocker 2: the OSS RAPIDS jar cannot link against Cloudera's Spark

With a pullable image in hand, the next failure was a **linkage** error, which no amount of
configuration reaches:

```
java.lang.NoSuchMethodError: 'void org.apache.spark.rdd.MapPartitionsRDD.<init>(
    org.apache.spark.rdd.RDD, scala.Function3, boolean, boolean, boolean,
    scala.reflect.ClassTag, scala.reflect.ClassTag)'
  at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar(GpuExec.scala:341)
```

Cloudera's `3.5.4.1.26.732.0-45` added a **defaulted** 6th constructor parameter
(`isDeterministic: Option[Boolean]`). Defaulted means *source*-compatible but **binary**-incompatible:
only pre-compiled callers break. The call site is the base method for every GPU operator, so it fires
on any GPU plan.

**Fixed by recompiling, not by patching logic.** Because the parameter is defaulted, the unmodified
upstream source emits the 8-arg call when compiled against Cloudera's `spark-core` — and the CDE base
image already ships both `scala-compiler-2.12.19.jar` and the exact `spark-core` jar, so it is its own
toolchain. Only 2 classes of 17,833 call that constructor.

This is a **build window**, not a fork-wide difference: Cloudera 7.2.18 links fine, as does every Apache
release 3.5.4–4.0.1. Evidence table in
[`docs/oss-rapids-vs-cloudera-spark.md`](docs/oss-rapids-vs-cloudera-spark.md); the patch itself in
[`runtime/README-patch.md`](runtime/README-patch.md).

## 8. Next steps and alternatives

**Re-run the probe after any registry, entitlement, or CDE version change** —
`probes/probe_images.sh` is the cheapest way to tell whether section 6 still holds. If Cloudera's own
GPU runtime (or just its Cloudera-built RAPIDS jar) becomes available, **delete the patch and use it.**

**Port the real workload.** The smoke test proves linkage and plan conversion; it does not prove a
250M-row Hive ETL. That is where any remaining signature change would surface.

**The chart escape hatch**, if a custom runtime were ever not an option. The chart templates the GPU
image reference as

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

**RAPIDS falls back to CPU silently — and the obvious check reports false CPU verdicts.** A green job
proves nothing, and neither does a wall-clock number. But with AQE on, reading the plan the naive way
reports *pre-conversion CPU operator names* on a run that was fully GPU-accelerated; that produced a
false `FAIL: ran on the CPU` twice here. Read the plan **after** the action and **off the same DataFrame
the action ran on**, then assert `isFinalPlan=true` — details in
[`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md) §6.

## Repo layout

```
docs/custom-gpu-runtime.md  ** the end-to-end runbook: versions, Docker + CDE CLI, verified output
runtime/Dockerfile          the custom runtime: two-stage, recompiles 2 RAPIDS classes in-image
runtime/README-patch.md     why the patch is legitimate, its provenance, and its limits
runtime/patch/*.scala       the two unmodified upstream NVIDIA sources that get recompiled
jobs/rapids_smoke_test.py   proves plugin load, GPU visibility, and Gpu* operators in the final plan
scripts/submit_rapids_smoke_test.sh   the submit, with every non-obvious flag explained inline
scripts/create_vc_gpu.py    the verified createVc, incl. the commented gpuImage.override block
scripts/verify_gpu_vc.sh    confirm gpuAccelerationEnabled, queue quota, node GPU capacity
probes/                     the differential image probe that established blocker 1
docs/oss-rapids-vs-cloudera-spark.md  the build-range evidence for blocker 2
docs/rapids-spark-conf.md   Spark RAPIDS configuration and the version-lock matrix
docs/cde-pvc-prerequisites.md         DE role + keytab onboarding, both easy to miss
```
