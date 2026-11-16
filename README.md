# CDE cuDF for Spark

GPU-accelerated Spark on Cloudera Private Cloud, using the open-source [NVIDIA RAPIDS Accelerator for
Apache Spark](https://nvidia.github.io/spark-rapids/) (cuDF). This repo records what it took to get
RAPIDS running on three Cloudera setups, and how to repeat it:

- **Cloudera Data Engineering (CDE) on Private Cloud Data Services**, on two Runtime versions. Here
  the deliverable is a **CDE custom runtime**: CDE's own Spark runtime image with the RAPIDS jar
  added, registered as a `custom-runtime-image` resource and selected per job.
- **Private Cloud Base, with Spark 3 on YARN and no Data Services.** There is nothing to build: each
  job ships the jar with `--jars`.

On each setup, RAPIDS queries ran end to end on an NVIDIA A10G. Each run was checked from the final
query plan, not from the job status, because RAPIDS falls back to the CPU without failing the job.

Internal technical prototype, not a supported configuration.

## Results by version

| | **Runtime 7.3.2.0 · Data Services 1.5.5-h3000 (Community Edition) · CDE 1.26** | **Runtime 7.1.9 SP2 CHF4 · Data Services 1.5.5 SP4 · CDE 1.27** | **Runtime 7.1.9 SP1 + PATCH-1069 · Base only · Spark 3 on YARN** |
|---|---|---|---|
| Guide | ➡ [`README-732.md`](README-732.md) (write-up and steps) | ➡ [`README-719.md`](README-719.md) (install guide) · [engineering notes](.internal/README-719.md) | ➡ [`README-719-base.md`](README-719-base.md) (install guide) · [engineering notes](.internal/README-719-base.md) |
| Platform | Data Services 1.5.5-h3000, Community Edition, on ECS (RKE2 v1.33.12) | Data Services 1.5.5 SP4 on ECS | Base cluster only, no Data Services |
| Cloudera Manager | 7.13.2.6 | 7.13.2 SP2 | 7.13.2 SP2 |
| Cloudera Runtime | 7.3.2.0 | 7.1.9 SP2 CHF4 (`7.1.9.20400`) | 7.1.9 SP1 + PATCH-1069 (`7.1.9-1.cdh7.1.9.p1069.74330271`) |
| Where Spark runs | CDE 1.26.0-b557 | CDE 1.27.0-b693 | YARN, from the SPARK3 parcel (CDS 3.5.4); no CDE |
| Cloudera Spark build | `3.5.4.1.26.732.0-45` | `3.5.4.127.354.7191000.0-30` | `3.5.4.3.5.7191000.0-30` |
| RAPIDS jar | 26.02.0, **patched**: 2 classes recompiled against Cloudera's Spark | 26.02.0, unmodified | 26.10.0, unmodified. 26.02.0 fails DataSource V2 writes here |
| Does the stock jar link? | No. Cloudera's Spark adds a parameter (`isDeterministic`) to the `MapPartitionsRDD` constructor, which then takes 8 arguments in bytecode instead of 7 | Yes. Upstream 7-argument constructor | Yes. 7-argument constructor, inferred from the GPU plans running (not checked with `javap`) |
| How the jar gets to the job | Custom runtime image `pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-p1`, from `runtime/Dockerfile` | Custom runtime image `pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-7.1.9`, from `runtime/Dockerfile.719` | `--jars` on each job; no image |
| Iceberg | No Iceberg jars in the CDE 1.26 image | Not checked | Works, on the CPU, with `suppressPlanningFailure=true`; otherwise the session must not load Iceberg |
| What was verified | Smoke test (CDE job run 6): 8 `Gpu*` operators in the final plan | Smoke test (CDE job run 2): 8 `Gpu*` operators in the final plan | 5-query benchmark: every query fully on the GPU, 4.5× faster than the CPU |
| GPU workers | 3 × `g5.8xlarge` (32 vCPU, 128 GiB), 1× NVIDIA A10G 24 GB each, RHEL 9.6 | 3 × `g5.8xlarge` (32 vCPU, 128 GiB), 1× NVIDIA A10G 24 GB each, RHEL 9.6 | 3 × `g5.4xlarge` (16 vCPU, 64 GiB), 1× NVIDIA A10G 24 GB each, RHEL 9.6 |
| NVIDIA driver | 615.71.09 (CUDA 13.4) | not recorded | 620.70.06 |

**In short:** none of the three needs a patch to Cloudera's Spark. On CDE you build a custom runtime
image. That is a small job: CDE's CPU image plus one jar and a short script. Runtime 7.3.2 also needs
two RAPIDS classes recompiled; 7.1.9 does not. On Base there is nothing to build, and the work is in
the job settings, mostly around how the session handles Iceberg.

## What gets in the way, and where

Four problems came up. Which ones you hit depends on where Spark runs:

| Problem | 7.3.2 on CDE | 7.1.9 on CDE | 7.1.9 SP1 on Base |
|---|---|---|---|
| 1. RAPIDS doesn't recognise Cloudera's Spark version | override set on every run | override set on every run | yes, seen without the override |
| 2. No CDE GPU runtime image to use | yes | not checked (custom runtime used anyway) | does not apply |
| 3. The RAPIDS jar doesn't link against Cloudera's Spark | yes | no | no |
| 4. Iceberg 1.3 in the default Spark config | no (no Iceberg in the image) | not checked | yes |

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
Spark adds an extra parameter, with a default value, to the constructor of an internal class,
`MapPartitionsRDD`. Code compiled against Cloudera's Spark is unaffected. The RAPIDS jar, however,
was compiled against Apache Spark and calls the old constructor, so every GPU query fails on its first
action with `NoSuchMethodError`. The fix is to recompile two NVIDIA source files, unchanged, against
the cluster's own `spark-core`, and put the results in the jar. Runtime 7.1.9's Spark doesn't have the
extra parameter, so on 7.1.9 the jar works as it is, on CDE and on Base. Details:
[`docs/oss-rapids-vs-cloudera-spark.md`](docs/oss-rapids-vs-cloudera-spark.md),
[`runtime/README-patch.md`](runtime/README-patch.md).

Both Spark builds report themselves as 3.5.4. So on CDE, choose the Dockerfile by checking the
`spark-core` jar in your base image, not by the version number. Each Dockerfile checks the constructor
while it builds, so the wrong choice fails the build instead of your first GPU job. See
[`runtime/README.md`](runtime/README.md).

**4. On Base, the default Spark config loads Iceberg 1.3.** CDS 3.5.4 turns Iceberg on in every
session, and RAPIDS 26.10 supports only Iceberg 1.6 and later. When it sees the older Iceberg, it fails
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

The full guide is [`README-719-base.md`](README-719-base.md).

1. **Turn on YARN GPU scheduling.** On RHEL 9, every NodeManager host must boot with cgroup v1. In
   Cloudera Manager, turn on cgroups, the Linux Container Executor and GPU usage for the NodeManagers
   (§2 lists the settings). `base/check_cluster.sh` then confirms that each NodeManager reports
   `yarn.io/gpu`.
2. **Set up the user.** They need an HDFS home directory, `/user/<name>`, and a Kerberos ticket on a
   Spark 3 gateway host.
3. **Download the RAPIDS jar and the GPU discovery script** to the gateway host, with
   `base/plugin_load.sh`.
4. **Submit jobs with the jar and the RAPIDS settings.** Pass the jar with `--jars`, and set the shim
   override, the Kryo registrator and `suppressPlanningFailure=true`. §6 has the full command.
5. **Check the final query plan**, not the job status, to see what ran on the GPU (§7).
   `base/benchmark.sh` does this for a five-query benchmark, and `base/iceberg_check.sh` for Iceberg
   and Hive tables.

## Repo layout

```
README-732.md               Runtime 7.3.2 / CDE 1.26: the missing GPU image, the linkage patch, limits, next steps
README-719.md               Runtime 7.1.9 / CDE 1.27 install guide: tested versions, steps, troubleshooting
.internal/README-719.md     Runtime 7.1.9 engineering notes: why no patch is needed, comparison with 7.3.2
README-719-base.md          Base 7.1.9 SP1, Spark 3 on YARN (no CDE) guide: YARN GPU setup, settings, benchmark
.internal/README-719-base.md  Base engineering notes: the requirements, Iceberg and DataSource V2, 26.02.0, CDE comparison
base/                       Base scripts, run on a Spark 3 gateway: check_cluster, plugin_load, benchmark, iceberg_check, summary, diagnose
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
docs/linkage-scan.md        how much else is broken on 7.3.2: 4,473 refs scanned, 16 misses, 1 reachable
docs/rapids-spark-conf.md   Spark RAPIDS configuration and the version-lock matrix
docs/cde-pvc-prerequisites.md         DE role, keytab onboarding, proxy 421: all block every job
docs/slides.md              two leadership slides: highlights, and the proposed productized patch
```
