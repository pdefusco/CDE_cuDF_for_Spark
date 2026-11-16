# CDE cuDF for Spark

Enabling **Spark GPU acceleration on Cloudera Data Engineering (CDE)** running on Cloudera Private
Cloud, as a step toward running a Spark RAPIDS (cuDF) workload on-prem.

The deliverable is a **CDE custom runtime**: CDE's own CPU Spark runtime image with the NVIDIA Spark
RAPIDS plugin added, registered as a `custom-runtime-image` resource and selected per job. A RAPIDS
query has run end-to-end on an NVIDIA A10G on two CDP versions so far. Each one is verified by an
`isFinalPlan=true` plan containing 8 `Gpu*` operators and no CPU operator below `GpuColumnarToRow`.

The same plugin also runs on a **Private Cloud Base cluster without CDE**, with Spark 3 on YARN and
the jar passed per job with `--jars`. There it ran five benchmark queries entirely on the GPU, 4.5×
faster than the CPU. It keeps the stock Spark 3 config if `spark.rapids.sql.suppressPlanningFailure=true`
is set, in which case Iceberg tables work but run on the CPU; otherwise the session must not load
Iceberg. See
[`README-719-base.md`](README-719-base.md).

Internal technical prototype, not a supported configuration.

## Results by version

| | **Runtime 7.3.2** | **Runtime 7.1.9** | **Base 7.1.9 SP1, Spark on YARN** |
|---|---|---|---|
| Write-up | ➡ [`README-732.md`](README-732.md) | ➡ [`README-719.md`](README-719.md) (install guide) · [engineering notes](.internal/README-719.md) | ➡ [`README-719-base.md`](README-719-base.md) (install guide) · [engineering notes](.internal/README-719-base.md) |
| Private Cloud | CE 1.5.5-h3000 | Data Services 1.5.5 SP4 | Base only, no Data Services |
| Cloudera Manager | 7.13.2.6 | 7.13.2 SP2 | 7.13.2 SP2 |
| Runtime | 7.3.2.0 | 7.1.9 SP2 CHF4 (`7.1.9.20400`) | 7.1.9 SP1 + PATCH-1069 (`p1069`) |
| CDE | 1.26.0-b557 | 1.27.0-b693 | none: SPARK3 parcel on YARN |
| Cloudera Spark build | `3.5.4.1.26.732.0-45` | `3.5.4.127.354.7191000.0-30` | `3.5.4.3.5.7191000.0-30` |
| `MapPartitionsRDD` ctor | 8 args (Cloudera-added `isDeterministic`) | 7 args (upstream) | 7 args, inferred from the GPU plans running; not checked with `javap` |
| OSS RAPIDS jar | 26.02.0 **does not link**: patched (2 classes recompiled) | 26.02.0 links **unmodified** | 26.10.0 links **unmodified**; Iceberg runs on the CPU (`suppressPlanningFailure=true`) or is removed. 26.02.0 fails every DataSource V2 write |
| Dockerfile | `runtime/Dockerfile` | `runtime/Dockerfile.719` | none: `--jars` per job |
| Image tag | `26.02.0-p1` | `26.02.0-7.1.9` | n/a |
| Verified | job run 6, 8 `Gpu*` operators | job run 2, 8 `Gpu*` operators | 5-query benchmark, all `Gpu*`, 4.5× vs CPU |
| GPU workers | 3 × `g5.8xlarge`, 1× A10G each | same | 3 × `g5.4xlarge`, 1× A10G each |

The short version: **the custom runtime is the approach on both CDE versions. The jar patch is a
7.3.2 artifact only.** On Base there is no image at all; the catch there is CDS's Iceberg 1.3 and DataSource V2
writes, not linkage.

## The two blockers, and which version has them

1. **CDE's own GPU runtime image could not be pulled** (7.3.2: `NotFound` on every reachable registry).
   So a custom runtime has to supply the RAPIDS pieces itself. That turns out to be small: the RAPIDS
   jar bundles cuDF and the CUDA runtime, so the image is the CPU base plus one jar and a GPU discovery
   script. There is no CUDA base image, no toolkit install, and no VC recreate. On 7.1.9 the stock GPU
   image was not probed; the custom runtime was used regardless.
   Evidence: [`docs/gpu-image-availability.md`](docs/gpu-image-availability.md).
2. **The OSS RAPIDS jar cannot link against Cloudera's 7.3.2 Spark.** Cloudera's 7.3.2 Spark adds a
   defaulted parameter to `MapPartitionsRDD`. That is source-compatible but binary-incompatible, so
   every GPU plan dies at its first action with `NoSuchMethodError`. The fix recompiles two unmodified
   upstream NVIDIA classes against the cluster's own `spark-core`. **7.1.9's Spark does not have the
   extra parameter**, so this blocker does not exist there.
   Evidence: [`docs/oss-rapids-vs-cloudera-spark.md`](docs/oss-rapids-vs-cloudera-spark.md),
   [`runtime/README-patch.md`](runtime/README-patch.md).

**Pick the Dockerfile by the `spark-core` jar in the CDE base image, not by the Spark version.** Both
builds call themselves Spark 3.5.4. Each Dockerfile asserts its constructor shape at build time, so
the wrong choice fails the build rather than the first GPU job. See
[`runtime/README.md`](runtime/README.md).

## The common workflow

The same on both versions; the per-version READMEs have the exact commands and output.

1. **Confirm the ECS workers advertise `nvidia.com/gpu`**, and set the CDE service GPU quota
   (`cdp de update-service --gpu-requests 3`).
2. **Create the virtual cluster through the API** with GPU acceleration on:
   `scripts/create_vc_gpu.py`, then `scripts/verify_gpu_vc.sh`. This is create-only, and a VC built in
   the console silently gets `gpuAccelerationEnabled: false`.
3. **Build the custom runtime** for your Spark build (`--platform linux/amd64`, staying on the CDE base
   image and uid 1345). Push it and register it with `cde resource create --type custom-runtime-image`.
4. **Submit the smoke test**, `scripts/submit_rapids_smoke_test.sh`, with
   `RUNTIME_RESOURCE=<resource>`. Read the verdict from `driver/stdout`. A green job proves nothing on
   its own, because RAPIDS falls back to the CPU silently; the test asserts `Gpu*` operators in the
   *final* plan.

Before any of this works, CDE Private Cloud needs a DE resource role and a self-onboarded Kerberos
keytab for the submitting user. Without either one, **every** job fails, GPU or not. Depending on the
deployment, the reverse proxy may also need an SNI fix. All three are in
[`docs/cde-pvc-prerequisites.md`](docs/cde-pvc-prerequisites.md).

## Repo layout

```
README-732.md               Runtime 7.3.2 / CDE 1.26: both blockers, the patch, limits, next steps
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
probes/                     the differential image probe that established blocker 1
docs/custom-gpu-runtime.md  the 7.3.2 end-to-end runbook: versions, Docker + CDE CLI, verified output
docs/gpu-image-availability.md        blocker 1 in full: the probe, its reasoning, the chart hatch
docs/oss-rapids-vs-cloudera-spark.md  the build-range evidence for blocker 2
docs/linkage-scan.md        how much else is broken on 7.3.2: 4,473 refs scanned, 16 misses, 1 reachable
docs/rapids-spark-conf.md   Spark RAPIDS configuration and the version-lock matrix
docs/cde-pvc-prerequisites.md         DE role, keytab onboarding, proxy 421: all block every job
docs/slides.md              two leadership slides: highlights, and the proposed productized patch
```
