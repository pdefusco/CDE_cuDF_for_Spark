# CDE cuDF for Spark

GPU-accelerated Spark on Cloudera Private Cloud, using the open-source [NVIDIA RAPIDS Accelerator for
Apache Spark](https://nvidia.github.io/spark-rapids/) (cuDF). This repo records what it took to get
RAPIDS running on four Cloudera setups, and how to repeat it:

- **Cloudera Data Engineering (CDE) on Private Cloud Data Services**, on two Runtime versions. Here
  the deliverable is a **CDE custom runtime**: CDE's own Spark runtime image with the RAPIDS jar
  added, registered as a `custom-runtime-image` resource and selected per job.
- **Private Cloud Base, with Spark 3 on YARN and no Data Services**, on two Runtime versions. There is
  no image: each job ships the jar with `--jars`.

A fifth setup, **Data Hub on CDP Public Cloud (Runtime 7.3.2)**, is a work in progress. It is
blocked on Cloudera Manager permissions; see
[Work in progress: Data Hub 7.3.2](#work-in-progress-data-hub-732-cdp-public-cloud).

On each of the four completed setups, RAPIDS queries ran end to end on an NVIDIA A10G. Each run was checked from the final
query plan, not from the job status, because RAPIDS falls back to the CPU without failing the job.

Internal technical prototype, not a supported configuration.

## Results by version

| | **Runtime 7.3.2.0 · Data Services 1.5.5-h3000 (Community Edition) · CDE 1.26** | **Runtime 7.1.9 SP2 CHF4 · Data Services 1.5.5 SP4 · CDE 1.27** | **Runtime 7.1.9 SP1 + PATCH-1069 · Base only · Spark 3 on YARN** | **Runtime 7.3.2 · Base only · Spark 3 on YARN** |
|---|---|---|---|---|
| Guide | ➡ [`README-732.md`](README-732.md) (write-up and steps) | ➡ [`README-719.md`](README-719.md) (install guide) · [engineering notes](.internal/README-719.md) | ➡ [`README-719-base.md`](README-719-base.md) (install guide) · [engineering notes](.internal/README-719-base.md) | ➡ [`README-732-base.md`](README-732-base.md) (install guide) · [engineering notes](.internal/README-732-base.md) |
| Platform | Data Services 1.5.5-h3000, Community Edition, on ECS (RKE2 v1.33.12) | Data Services 1.5.5 SP4 on ECS | Base cluster only, no Data Services | Base cluster only, no Data Services |
| Cloudera Manager | 7.13.2.6 | 7.13.2 SP2 | 7.13.2 SP2 | 7.13.2.6 |
| Cloudera Runtime | 7.3.2.0 | 7.1.9 SP2 CHF4 (`7.1.9.20400`) | 7.1.9 SP1 + PATCH-1069 (`7.1.9-1.cdh7.1.9.p1069.74330271`) | 7.3.2 (`7.3.2-1.cdh7.3.2.p0.77083870`) |
| Where Spark runs | CDE 1.26.0-b557 | CDE 1.27.0-b693 | YARN, from the SPARK3 parcel (CDS 3.5.4); no CDE | YARN, from the CDH parcel (Spark 3 is part of it on 7.3.x); no CDE |
| Cloudera Spark build | `3.5.4.1.26.732.0-45` | `3.5.4.127.354.7191000.0-30` | `3.5.4.3.5.7191000.0-30` | `3.5.4.7.3.2.0-957` |
| RAPIDS jar | 26.02.0, **patched**: 2 classes recompiled against Cloudera's Spark | 26.02.0, unmodified | 26.10.0, unmodified. 26.02.0 fails DataSource V2 writes here | 26.10.0, **patched** on the gateway by `base/patch_rapids_jar.sh`: 2 classes recompiled against Cloudera's Spark, from the 26.10.0 source |
| Does the stock jar link? | No. Cloudera's Spark adds a parameter (`isDeterministic`) to the `MapPartitionsRDD` constructor, which then takes 8 arguments in bytecode instead of 7 | Yes. Upstream 7-argument constructor | Yes. 7-argument constructor, checked with `javap` | No. 8-argument constructor, as on CDE 7.3.2 |
| How the jar gets to the job | Custom runtime image `pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-p2`, from `runtime/Dockerfile`. The tested runs used the older `-p1`, which breaks the ColumnarRdd path (see `runtime/README-patch.md`) | Custom runtime image `pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-7.1.9`, from `runtime/Dockerfile.719` | `--jars` on each job; no image | `--jars` on each job; no image |
| Iceberg | No Iceberg jars in the CDE 1.26 image | Not checked | Works, on the CPU, with `suppressPlanningFailure=true`; otherwise the session must not load Iceberg | Iceberg 1.5.2. Works, on the CPU, with `suppressPlanningFailure=true`; otherwise the session must not load Iceberg |
| What was verified | Smoke test (CDE job run 6): 8 `Gpu*` operators in the final plan | Smoke test (CDE job run 2): 8 `Gpu*` operators in the final plan | 5-query benchmark: every query fully on the GPU, 4.5× faster than the CPU | 5-query benchmark: every query fully on the GPU, 4.2× faster than the CPU |
| GPU workers | 3 × `g5.8xlarge` (32 vCPU, 128 GiB), 1× NVIDIA A10G 24 GB each, RHEL 9.6 | 3 × `g5.8xlarge` (32 vCPU, 128 GiB), 1× NVIDIA A10G 24 GB each, RHEL 9.6 | 3 × `g5.4xlarge` (16 vCPU, 64 GiB), 1× NVIDIA A10G 24 GB each, RHEL 9.6 | 3 × `g5.4xlarge` (16 vCPU, 64 GiB), 1× NVIDIA A10G 24 GB each, RHEL 9.6 |
| NVIDIA driver | 615.71.09 (CUDA 13.4) | not recorded | 620.70.06 | 620.70.06 |

**In short:** none of the four needs a patch to Cloudera's Spark. On CDE you build a custom runtime
image. That is a small job: CDE's CPU image plus one jar and a short script. On Base there is no image,
and the work is in the job settings, mostly around how the session handles Iceberg. On Runtime 7.3.2,
on CDE and on Base alike, two RAPIDS classes must also be recompiled against Cloudera's Spark; on 7.1.9
the published jar works as it is. On Base that is one script, run once on the gateway host.

## Work in progress: Data Hub 7.3.2 (CDP Public Cloud)

> **Status: work in progress, blocked.** The Data Hub is up and the GPU driver recipe is attached,
> but YARN GPU scheduling can't be turned on with the Cloudera Manager role an environment owner
> gets. No GPU node has been started yet, and no RAPIDS query has run.

The goal is the same as for Base: Spark 3 on YARN, with GPU executors on a Data Hub compute group.
Guide, with the commands and which steps have run: ➡ [`README-732-datahub.md`](README-732-datahub.md).

| | **Data Hub, Runtime 7.3.2 · Data Engineering template · CDP Public Cloud (AWS)** |
|---|---|
| Cluster template | "7.3.2 - Data Engineering: Apache Spark3, Apache Hive, Apache Oozie" |
| Image | The default 7.3.2 image of the `cdp-default` catalog: RHEL 9.6, no NVIDIA driver, no CUDA |
| Cloudera Manager | 7.13.2 |
| Cloudera Runtime | 7.3.2 (`7.3.2-1.cdh7.3.2.p20000.83281140`) |
| Where Spark runs | YARN, from the CDH parcel. The cluster is a compute cluster attached to the environment's Data Lake |
| Cloudera Spark build | `3.5.8.7.3.2.20000-258`, Scala 2.12.19, JDK 17. This is Spark 3.5.8, not the 3.5.4 of the other four setups |
| Does the stock jar link? | No. 8-argument `MapPartitionsRDD` constructor, as on Base 7.3.2 |
| RAPIDS jar | 26.10.0 (planned). It lists Spark 3.5.8 as supported (shim `spark358`) |
| Iceberg | 1.10.1. RAPIDS 26.10 supports Iceberg 1.6 and later, so Iceberg may run on the GPU here. Not tested yet |
| Hosts | master `m5.4xlarge` × 1, CPU workers `r5d.2xlarge` × 3, GPU compute group `p4d.24xlarge` (8 × NVIDIA A100 40 GB) at 0 nodes until GPU scheduling works |

### What we found so far

1. **Data Hub accepts only the p4d family for GPUs** (`p4d.24xlarge`, `p4de.24xlarge`). A `g5`
   instance type (A10G, used in the four other setups) fails validation. A request to support
   g5 is open with the Data Hub team. A p4d.24xlarge costs about $22 an hour on demand, so the compute
   group stays at 0 nodes until the rest is ready.
2. **Data Hub sets up nothing for GPUs.** The runtime image has no NVIDIA driver or CUDA. The 7.3.2
   Data Engineering template doesn't turn on YARN GPU scheduling (`yarn.io/gpu`), and provisioning
   installs nothing GPU-related. You install the driver yourself, with a recipe, and turn on GPU
   scheduling in Cloudera Manager.
3. **The driver has to come from NVIDIA's `.run` installer.** The image has no package repositories,
   so the NVIDIA RPMs can't resolve their dependencies. The recipe, run on the compute group before
   services start, installs:
   - driver 615.71.10 (open kernel modules), built against the kernel-devel the image already has;
   - Fabric Manager 615.71.10 from NVIDIA's tarball. A p4d's 8 A100s are connected through
     NVSwitch, and CUDA won't start without a running Fabric Manager of the same version;
   - kernel arguments for cgroup v1, because YARN GPU isolation doesn't support cgroup v2. They take
     effect after a reboot, once the node has been added.

   The recipe has been written and attached, but it hasn't run yet.
4. **An environment owner can't turn on YARN GPU scheduling in Cloudera Manager.** This is the
   blocker.
   - GPU isolation needs YARN cgroups ("Use CGroups for Resource Management"). Cloudera Manager
     refuses to restart YARN with cgroups unless every NodeManager host has "Enable Cgroup-based
     Resource Management" turned on: *"Yarn is configured to use cgroups, but 3 hosts do not have
     'Enable Cgroup-based Resource Management' enabled"*.
   - That check has no "Suppress Configuration Validator" switch. Only the GPU validators have one.
   - "Enable Cgroup-based Resource Management" is a host setting. The environment's admin group
     (`_c_cm_admins_*`) maps in Cloudera Manager to **Limited Cluster Administrator**. That role can
     change service and role settings, but gets HTTP 403 on host settings. No group maps to Full
     Administrator, and changing the user's CDP role doesn't change the mapping.
   - We've asked Cloudera's product and engineering teams for a supported way to get Full
     Administrator on a Data Hub, or for the host setting to be turned on for this cluster.
5. **RAPIDS will need the `spark358` shim, and the patch script will need an update.** Set
   `spark.rapids.shims-provider-override=com.nvidia.spark.rapids.shims.spark358.SparkShimServiceProvider`.
   `base/patch_rapids_jar.sh` currently patches only the `spark354/` classes, so it has to be
   generalised before it can patch for this Spark build.

### Remaining steps

| Step | State |
|---|---|
| Create the Data Hub with the default 7.3.2 image and a p4d compute group at 0 nodes | Done |
| Write the driver and Fabric Manager recipe, and attach it to the compute group | Done; not run yet |
| Turn on YARN cgroups and "Enable Cgroup-based Resource Management" on every NodeManager host | **Blocked**: needs Full Administrator in Cloudera Manager |
| Scale the compute group to 1, reboot it into cgroup v1, check `nvidia-smi` (8 GPUs) and Fabric Manager | To do |
| Turn on GPU usage for the compute group's NodeManagers; check YARN reports `yarn.io/gpu` = 8 | To do |
| Generalise `base/patch_rapids_jar.sh` to the `spark358` shim and patch the 26.10.0 jar | To do |
| Run `base/benchmark.sh` and `base/iceberg_check.sh`, GPU against CPU | To do |
| Write up the result as a fifth column in [Results by version](#results-by-version) | To do |

## What gets in the way, and where

Four problems came up. Which ones you hit depends on where Spark runs:

| Problem | 7.3.2 on CDE | 7.1.9 on CDE | 7.1.9 SP1 on Base | 7.3.2 on Base |
|---|---|---|---|---|
| 1. RAPIDS doesn't recognise Cloudera's Spark version | override set on every run | override set on every run | yes, seen without the override | yes, seen without the override |
| 2. No CDE GPU runtime image to use | yes | not checked (custom runtime used anyway) | does not apply | does not apply |
| 3. The RAPIDS jar doesn't link against Cloudera's Spark | yes | no | no | yes |
| 4. An Iceberg older than 1.6 in the default Spark config | no (no Iceberg in the image) | not checked | yes (1.3) | yes (1.5.2) |

**1. RAPIDS doesn't recognise Cloudera's Spark version.** RAPIDS reads the Spark version string to
choose which of its builds to use, and Cloudera's strings (such as `3.5.4.3.5.7191000.0-30`) aren't
on its list. One setting tells it to use its Apache Spark 3.5.4 build:
`spark.rapids.shims-provider-override=com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider`.
Jobs also add RAPIDS's Kryo registrator, `spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator`.
Cloudera's Spark config selects the Kryo serializer (checked on Base), and RAPIDS refuses to run under
Kryo without its registrator.

**2. CDE has no GPU runtime image you can use.** CDE is meant to provide a GPU version of its Spark
image. On 7.3.2, that image couldn't be pulled from any registry we could reach. The way around it
is a custom runtime, which is less work than it sounds. The RAPIDS jar already bundles cuDF and the
CUDA libraries, so the image is CDE's normal CPU image plus that jar and a GPU discovery script.
There is no CUDA base image and no toolkit install, and the virtual cluster doesn't have to be
recreated. On 7.1.9 the stock GPU image wasn't tried; the custom runtime was used from the start.
Details: [`docs/gpu-image-availability.md`](docs/gpu-image-availability.md).

**3. On Runtime 7.3.2, the RAPIDS jar doesn't link against Cloudera's Spark.** Cloudera's 7.3.2
Spark, both CDE's and the one in the Base CDH parcel, adds an extra parameter, with a default value, to the constructor of an internal class,
`MapPartitionsRDD`. Code compiled against Cloudera's Spark is unaffected. The RAPIDS jar, however,
was compiled against Apache Spark and calls the old constructor, so every GPU query fails on its first
action with `NoSuchMethodError`. The fix is to recompile two NVIDIA source files, unchanged, against
the cluster's own `spark-core`, and put the results in the jar. On CDE that happens while the runtime
image is built; on Base, [`base/patch_rapids_jar.sh`](base/patch_rapids_jar.sh) does it on the
gateway host. The Spark 3.5.4 builds tested on Runtime 7.1.9 don't have the extra parameter, so there
the jar works as it is, on CDE and on Base. A scan of every Spark method and class the jar uses,
against 7.3.2 Base's Spark, found nothing else broken on a default code path
([`docs/linkage-scan.md`](docs/linkage-scan.md#runtime-732-base-spark-3-on-yarn)).
Details: [`docs/oss-rapids-vs-cloudera-spark.md`](docs/oss-rapids-vs-cloudera-spark.md),
[`runtime/README-patch.md`](runtime/README-patch.md).

This depends on the Spark build, not the Runtime version. The extra parameter isn't limited to 7.3.2:
the Spark 3.3.2 that the 7.1.9 CDH parcel bundles (`3.3.2.3.3.7191000.1-126`) has it too, while CDS
3.5.4 on the same cluster doesn't. The builds also don't differ in their version string (both 7.3.2's
and 7.1.9's report 3.5.4). So check the `spark-core` jar your jobs actually run:
[`scripts/check_spark_core.sh`](scripts/check_spark_core.sh) reads it from a local jar, a CDE runtime
image, a cluster host or a parcel URL, and says whether the stock jar links. On CDE, each Dockerfile
also checks the constructor while it builds, so the wrong choice fails the build instead of your first
GPU job. See [`runtime/README.md`](runtime/README.md).

**4. On Base, the default Spark config loads an Iceberg that RAPIDS can't handle.** Spark 3 on Base
turns Iceberg on in every session: 1.3 with CDS 3.5.4 on 7.1.9, 1.5.2 on 7.3.2. RAPIDS 26.10 supports
only Iceberg 1.6 and later. When it sees the older Iceberg, it fails
any query that reads or writes through Spark's DataSource V2 API, even if no Iceberg table is
involved. That API covers Iceberg tables, `writeTo(...)` and the `noop` sink. Parquet files and Hive
tables go through the older V1 API, and they run on the GPU regardless. There are two ways to handle
this:
- Set `spark.rapids.sql.suppressPlanningFailure=true` (recommended). The Iceberg parts of a query
  then run on the CPU, and the rest stays on the GPU.
- Give RAPIDS jobs a copy of the Spark config with Iceberg removed.

RAPIDS 26.02.0 is worse here: it fails every DataSource V2 write with `IllegalAccessError`, with or
without that setting, so use 26.10.0 on Base. Details:
[`README-719-base.md` §5](README-719-base.md#5-choose-how-rapids-sessions-handle-iceberg).

## How to set it up

### On CDE (Data Services)

The per-version guides have the exact commands and expected output:
[`README-732.md`](README-732.md) and [`README-719.md`](README-719.md).

Start with what every CDE job needs, GPU or not. The submitting user needs a DE resource role and a
Kerberos keytab, which they onboard themselves. Some deployments also need a reverse-proxy SNI fix.
All three are in [`docs/cde-pvc-prerequisites.md`](docs/cde-pvc-prerequisites.md).

1. **Check that Kubernetes sees the GPUs.** Each ECS GPU worker must advertise `nvidia.com/gpu`.
   Then give the CDE service a GPU quota: `cdp de update-service --gpu-requests 3`.
2. **Create a virtual cluster with GPU acceleration, through the API.** Run
   `scripts/create_vc_gpu.py`, then check the result with `scripts/verify_gpu_vc.sh`. GPU acceleration
   can only be set when the virtual cluster is created. A virtual cluster created in the CDE console
   always has it off (`gpuAccelerationEnabled: false`), and nothing tells you.
3. **Build the custom runtime image** with the Dockerfile for your Spark build (see problem 3). Build
   for `linux/amd64`, and keep CDE's base image and its user (uid 1345). Push the image to a registry
   the cluster can pull from, then register it with `cde resource create --type custom-runtime-image`.
4. **Run the smoke test**, `scripts/submit_rapids_smoke_test.sh`, with `RUNTIME_RESOURCE=<resource>`.
   The verdict is in the job's `driver/stdout`. Don't go by the job status: a job that fell back to
   the CPU also succeeds. The test checks for `Gpu*` operators in the final query plan.

### On Base (Spark 3 on YARN)

On a Data Hub, steps 1 and 4 work differently and aren't finished. See
[`README-732-datahub.md`](README-732-datahub.md) (work in progress).

The full guides are [`README-719-base.md`](README-719-base.md) (7.1.9) and
[`README-732-base.md`](README-732-base.md) (7.3.2, which adds step 4).

1. **Turn on YARN GPU scheduling.** On RHEL 9, every NodeManager host must boot with cgroup v1. In
   Cloudera Manager, turn on cgroups, the Linux Container Executor and GPU usage for the NodeManagers
   (§2 lists the settings). `base/check_cluster.sh` then confirms that each NodeManager reports
   `yarn.io/gpu`.
2. **Set up the user.** They need an HDFS home directory, `/user/<name>`, and a Kerberos ticket on a
   Spark 3 gateway host.
3. **Download the RAPIDS jar and the GPU discovery script** to the gateway host, with
   `base/plugin_load.sh`.
4. **On 7.3.2, patch the jar** with `base/patch_rapids_jar.sh`, and use the copy it writes to
   `/tmp/rapids-patched/`. `base/check_cluster.sh` shows whether your Spark needs it: an
   8-argument `MapPartitionsRDD` constructor.
5. **Submit jobs with the jar and the RAPIDS settings.** Pass the jar with `--jars`, and set the shim
   override, the Kryo registrator and `suppressPlanningFailure=true`. §6 has the full command.
6. **Check the final query plan**, not the job status, to see what ran on the GPU (§7).
   `base/benchmark.sh` does this for a five-query benchmark, and `base/iceberg_check.sh` for Iceberg
   and Hive tables.

## Repo layout

```
README-732.md               Runtime 7.3.2 / CDE 1.26: the missing GPU image, the linkage patch, limits, next steps
README-719.md               Runtime 7.1.9 / CDE 1.27 install guide: tested versions, steps, troubleshooting
.internal/README-719.md     Runtime 7.1.9 engineering notes: why no patch is needed, comparison with 7.3.2
README-719-base.md          Base 7.1.9 SP1, Spark 3 on YARN (no CDE) guide: YARN GPU setup, settings, benchmark
.internal/README-719-base.md  Base engineering notes: the requirements, Iceberg and DataSource V2, 26.02.0, CDE comparison
README-732-base.md          Base 7.3.2, Spark 3 on YARN guide: the 7.1.9 setup plus patching the jar on the gateway
.internal/README-732-base.md  Base 7.3.2 engineering notes: the patch, the linkage scan, comparison with the other three
base/                       Base scripts, run on a Spark 3 gateway: check_cluster, plugin_load, patch_rapids_jar,
                            benchmark, iceberg_check, linkage_scan, summary, diagnose
README-732-datahub.md       Data Hub 7.3.2 on CDP Public Cloud (work in progress): p4d, driver recipe, the CM role blocker
datahub/                    Data Hub scripts, run from a workstation: create-datahub, nvidia-gpu-recipe (untested),
                            cm-yarn-gpu (Cloudera Manager API through Knox)
runtime/Dockerfile          7.3.2 custom runtime: two-stage, recompiles 2 RAPIDS classes in-image
runtime/Dockerfile.719      7.1.9 custom runtime: unpatched OSS jar, no patcher stage
runtime/README.md           which Dockerfile, fetching the jar, building
runtime/README-patch.md     why the 7.3.2 patch is legitimate, its provenance, and its limits
runtime/patch/*.scala       the two unmodified upstream NVIDIA sources that get recompiled
jobs/rapids_smoke_test.py   proves plugin load, GPU visibility, and Gpu* operators in the final plan
scripts/submit_rapids_smoke_test.sh   the submit, with every non-obvious flag explained inline
scripts/create_vc_gpu.py    the verified createVc, incl. the commented gpuImage.override block
scripts/verify_gpu_vc.sh    confirm gpuAccelerationEnabled, queue quota, node GPU capacity
scripts/check_spark_core.sh does a Spark build need the patch? javap on spark-core from a jar, image, host or parcel
probes/                     the differential image probe behind problem 2 (no usable GPU image)
docs/custom-gpu-runtime.md  the 7.3.2 end-to-end runbook: versions, Docker + CDE CLI, verified output
docs/gpu-image-availability.md        problem 2 in full: the probe, its reasoning, the chart hatch
docs/oss-rapids-vs-cloudera-spark.md  the build-range evidence for problem 3 (the 7.3.2 linkage break)
docs/linkage-scan.md        how much else is broken on 7.3.2: the CDE 1.26 hand scan, and base/linkage_scan.sh on Base
docs/rapids-spark-conf.md   Spark RAPIDS configuration and the version-lock matrix
docs/cde-pvc-prerequisites.md         DE role, keytab onboarding, proxy 421: all block every job
docs/slides.md              two leadership slides: highlights, and the proposed productized patch
```
