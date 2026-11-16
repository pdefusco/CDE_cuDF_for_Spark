# GPU-accelerated Spark on CDE: Cloudera Private Cloud with Runtime 7.1.9

← [Overview and other versions](README.md)

This guide shows how to set up a Cloudera Data Engineering (CDE) virtual cluster that runs Spark SQL
and DataFrame jobs on NVIDIA GPUs, using the open-source [NVIDIA RAPIDS Accelerator for Apache
Spark](https://nvidia.github.io/spark-rapids/). The accelerator comes in a **CDE custom runtime
image**: CDE's own Spark runtime with the RAPIDS jar added. A job uses the GPU by asking for that
image. Jobs that don't ask for it run exactly as before.

> **Support status.** This is a tested reference configuration, not a Cloudera-supported one. Test it
> with your own workloads before you rely on it.

## 1. Tested versions

| Component | Version |
|---|---|
| Cloudera Private Cloud Data Services | 1.5.5 SP4 (Embedded Container Service) |
| Cloudera Manager | 7.13.2 SP2 |
| Cloudera Runtime | 7.1.9 SP2 CHF4 (`7.1.9.20400`) |
| Cloudera Data Engineering | 1.27.0-b693 |
| CDE Spark runtime base image | `dex-spark-runtime-3.5.4-7.1.9.1078:1.27.0-b693` |
| Spark | 3.5.4 (`spark-core_2.12-3.5.4.127.354.7191000.0-30`), Scala 2.12.18, JDK 17 |
| NVIDIA RAPIDS Accelerator | `rapids-4-spark_2.12-26.02.0-cuda12.jar`, unmodified, `spark354` shim |
| GPU hosts | 3 × AWS `g5.8xlarge`, 1 × NVIDIA A10G (24 GB) each, RHEL 9.6, x86-64 |

**Use these versions together.** Each RAPIDS release is built for specific Spark versions, and the
`spark354` shim used here has to match the Spark build inside the CDE image. On a different CDE or
Runtime version, check the `spark-core` jar in your base image first; see
[Other versions](#8-other-versions).

## 2. Prerequisites

**Cluster**

- ECS GPU nodes with the NVIDIA driver installed, and each worker advertising `nvidia.com/gpu` to
  Kubernetes. Step 3.1 checks this.
- A CDE service on the Private Cloud environment.
- Shell access to the ECS master host, with `jq` installed. Steps 3.1 and 3.4 run there.

**User**

Without both of these, **every** CDE job fails, GPU or not:

- A **DE resource role** (for example `DEAdmin`) on the environment, granted in Cloudera IAM. A
  virtual-cluster ACL is not enough. Role changes can take up to 5 minutes to take effect.
- A **Kerberos keytab uploaded to CDE**: virtual cluster → **Actions** → **Upload User Keytab**.

Symptoms and fixes for both: [`docs/cde-pvc-prerequisites.md`](docs/cde-pvc-prerequisites.md).

**Workstation**

- Docker, able to build `linux/amd64` images. On Apple Silicon this is a cross-build; no emulation is
  needed, because the image build only copies files.
- Login access to the registry that hosts the CDE base image. For a connected install this is
  `container.repository.cloudera.com`, with your Cloudera license credentials.
- A container registry that the ECS cluster can pull from, to host the custom image.
- The CDP CLI, configured for your Private Cloud control plane, and `pip install cdpy`.
- The CDE CLI, configured against the virtual cluster's **Jobs API URL** (available after step 3.3).
- This repository, cloned locally.

## 3. Create a GPU-enabled virtual cluster

### 3.1 Check that the workers expose GPUs

On the ECS master host (ECS's `kubectl` is
`sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml`):

```bash
kubectl get nodes -o json | jq -r '.items[] |
  "\(.metadata.name) gpu=\(.status.allocatable["nvidia.com/gpu"] // "none") taints=\(.spec.taints // "NONE")"'
```

Each GPU worker should show `gpu=1` (or more). If the GPU workers have no taints, jobs don't need
tolerations to be scheduled on them.

### 3.2 Give the CDE service a GPU quota

```bash
cdp de list-services                       # note the clusterId, e.g. cluster-xxxxxxxx
cdp de update-service --cluster-id <clusterId> --gpu-requests <totalGpus>
```

This changes the existing service; you don't have to recreate it.

### 3.3 Create the virtual cluster with the API, not the console

> **GPU acceleration can only be turned on when a virtual cluster is created.** A virtual cluster
> created in the console always has it off (`gpuAccelerationEnabled: false`), and you can't change it
> later. GPU jobs on such a cluster quietly run on the CPU, with no error. Create the virtual cluster
> with the script below instead.

```bash
python scripts/create_vc_gpu.py --cluster-id <clusterId> --name <vcName>
```

The script turns on `dexapp.api.gpuAcceleration.enabled`. It sets the virtual cluster's GPU limit to
3 and its **guaranteed** GPU requests to `0`; the guaranteed value must be `0`. Before you run it,
edit `gpuRequests` and the CPU and memory values in `build_request()` to fit your cluster. Add
`--dry-run` to print the request without sending it. The virtual cluster takes about 3 minutes to
install; check on it with `cdp de describe-vc --cluster-id <clusterId> --vc-id <vcId>`.

### 3.4 Verify the virtual cluster

On the ECS master host:

```bash
scripts/verify_gpu_vc.sh <vcId>
```

By default the script runs ECS's RKE2 `kubectl` through `sudo`; set `KUBECTL=...` to use a different
one.

The output must include `gpuAccelerationEnabled: true`. It also shows the GPU limit of the virtual
cluster's queue and the runtime images the virtual cluster uses. Write down the `image:` line: step
4.2 builds on that image.

## 4. Build and register the custom runtime

### 4.1 Download the RAPIDS jar

```bash
cd runtime
curl -fL --retry 3 -O \
  https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.0/rapids-4-spark_2.12-26.02.0-cuda12.jar
```

The `cuda12` build already contains cuDF and the CUDA runtime libraries, so the image doesn't need
the CUDA toolkit or an NVIDIA base image. The hosts still need an NVIDIA driver that supports CUDA 12.

### 4.2 Build and push

Set `BASE` to the `image:` value from step 3.4. That is the normal (CPU) Spark runtime your virtual
cluster already uses:

```bash
BASE=container.repository.cloudera.com/cdp-private/cloudera/dex/dex-spark-runtime-3.5.4-7.1.9.1078:1.27.0-b693
IMAGE=<your-registry>/cde-rapids-runtime-3.5.4:26.02.0-7.1.9

docker login "${BASE%%/*}"   # the registry that hosts BASE
docker build --platform linux/amd64 -f Dockerfile.719 --build-arg BASE="$BASE" -t "$IMAGE" .
docker push "$IMAGE"
cd ..
```

The build prints `upstream MapPartitionsRDD ctor OK`. If it stops with `FATAL: base MapPartitionsRDD
lacks the upstream 7-arg ctor`, your base image has a different Spark build from the one this guide
covers; see [Other versions](#8-other-versions).

`Dockerfile.719` adds three things to the base image:

- the RAPIDS jar, in `/opt/spark/jars/`;
- the GPU discovery script, at `/opt/cde/gpu/getGpusResources.sh`;
- ownership changes for both files.

It keeps CDE's base image and its non-root user (uid 1345), because CDE requires both in a custom
runtime.

### 4.3 Register the image with CDE

```bash
cde resource create --name cde-rapids-runtime-719 \
  --type custom-runtime-image --image-engine spark3 --image "$IMAGE"
```

We tested with a public registry. For a private registry, first create a Docker credential with
`cde credential create --type docker-basic`, then add `--image-credential <name>` to the command
above. We haven't tested that.

## 5. Run the smoke test

A job that succeeds hasn't necessarily used the GPU: when RAPIDS can't handle part of a query, it
runs that part on the CPU without any error. So the smoke test doesn't stop at checking that the job
finished. It fails unless the query's **final** physical plan contains `Gpu*` operators.

```bash
CDE_PROFILE=<cdeCliProfile> RUNTIME_RESOURCE=cde-rapids-runtime-719 \
  ./scripts/submit_rapids_smoke_test.sh

cde --config-profile <cdeCliProfile> run logs --id <runId> --type driver/stdout --follow=false
```

Always set `CDE_PROFILE`. The script's default profile name only exists in our test environment.

Expected output (trimmed):

```
  host=rapids-smoke-test-…-exec-1  gpu=0, NVIDIA A10G

AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   GpuColumnarToRow false
   +- GpuTopN(limit=5, …)
      +- GpuHashAggregate (keys=[k#2], functions=[gpucount(1, false), gpubasicsum(…), avg(…)])
         +- GpuShuffleCoalesce 1073741824
            +- GpuCustomShuffleReader coalesced
               +- ShuffleQueryStage 0
                  +- GpuColumnarExchange gpuhashpartitioning(k#2, 200, Murmur3Mode), ENSURE_REQUIREMENTS
                     +- GpuHashAggregate (keys=[k#2], functions=[partial_gpucount(1, false), …])
                        +- GpuProject [cast((id#0L % 1000) as int) AS k#2, …]
                           +- GpuRange (0, 20000000, step=1, splits=24)

VERDICT
  plugin configured : True
  plan finalized    : True
  Gpu* operators    : 8  ['GpuColumnarExchange', 'GpuColumnarToRow', 'GpuCustomShuffleReader',
                          'GpuHashAggregate', 'GpuProject', 'GpuRange', 'GpuShuffleCoalesce', 'GpuTopN']
  PASS: RAPIDS executed operators on the GPU.
```

## 6. Run your own jobs on the GPU

Start from [`scripts/submit_rapids_smoke_test.sh`](scripts/submit_rapids_smoke_test.sh). Every GPU
job needs the custom runtime (`--runtime-image-resource-name cde-rapids-runtime-719`) and these
settings:

| Setting | Value |
|---|---|
| `spark.plugins` | `com.nvidia.spark.SQLPlugin` |
| `spark.rapids.sql.enabled` | `true` |
| `spark.rapids.shims-provider-override` | `com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider` |
| `spark.shuffle.manager` | `com.nvidia.spark.rapids.spark354.RapidsShuffleManager` |
| `spark.kryo.registrator` | `com.nvidia.spark.rapids.GpuKryoRegistrator` |
| `spark.driver.extraClassPath`, `spark.executor.extraClassPath` | `/opt/spark/jars/rapids-4-spark_2.12-26.02.0-cuda12.jar` |
| `spark.executor.resource.gpu.amount` | `1` |
| `spark.executor.resource.gpu.vendor` | `nvidia.com` |
| `spark.executor.resource.gpu.discoveryScript` | `/opt/cde/gpu/getGpusResources.sh` |
| `spark.task.resource.gpu.amount` | `1 / executor cores`, e.g. `0.25` for 4 cores |
| `spark.executor.memoryOverhead` | generous, e.g. `6g`; RAPIDS host memory is off-heap |

**Size the driver and executors with CDE's own flags** (`--driver-memory`, `--executor-memory`,
`--executor-cores`), not with `--conf spark.executor.memory=…`. CDE applies its job defaults after
your `--conf` values, so those get replaced with 1g without any warning.

While tuning, add `--conf spark.rapids.sql.explain=NOT_ON_GPU`, and RAPIDS will log every operator it
left on the CPU, with the reason. For sizing advice for production jobs, see
[`docs/rapids-spark-conf.md`](docs/rapids-spark-conf.md).

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `authorization policy denies` on every call | No DE resource role | Grant one in IAM, then wait up to 5 minutes ([prerequisites §1](docs/cde-pvc-prerequisites.md)) |
| Runs fail within seconds: `keytab is not present for user …` | No keytab uploaded | Virtual cluster → Actions → Upload User Keytab ([prerequisites §2](docs/cde-pvc-prerequisites.md)) |
| Job succeeds, but the plan has no `Gpu*` operators | `gpuAccelerationEnabled: false`, or a setting from §6 is missing | Run `verify_gpu_vc.sh`. If the flag is false, recreate the virtual cluster with `create_vc_gpu.py` |
| Executor pod stays `Pending` | The GPU quota is used up or was never set | Check the service quota (§3.2), the virtual cluster's GPU limit, and other running GPU jobs |
| `NoSuchMethodError: …MapPartitionsRDD.<init>` | The image was built on a different Spark build | Rebuild on the 7.1.9 base (§4.2); see §8 |
| `docker build` fails at the ctor check | `BASE` is not the 7.1.9 Spark build | Use the `image:` value from `verify_gpu_vc.sh` |

## 8. Other versions

The published RAPIDS jar doesn't work with the Spark build in Runtime 7.3.2 / CDE 1.26. That version
needs `runtime/Dockerfile` instead, which patches the jar; see [`README-732.md`](README-732.md).
`Dockerfile.719` checks the Spark build while the image is being built, so a mismatch fails the build
rather than a running job. To check any other base image yourself:

```bash
docker run --rm --platform linux/amd64 --entrypoint bash "$BASE" -c \
  'javap -cp "$(ls /opt/spark/jars/spark-core_*.jar)" -p org.apache.spark.rdd.MapPartitionsRDD | grep "MapPartitionsRDD("'
```

If the constructor ends `boolean, boolean, boolean, scala.reflect.ClassTag<U>, scala.reflect.ClassTag<T>)`,
`Dockerfile.719` applies.

## 9. Known limitations

- **Only the smoke test was run.** It shows that RAPIDS loads, the executors see the GPU, and
  aggregation, shuffle and projection run on it. Nothing has been benchmarked on this version, and
  no production workload has been tried.
- **Only one compatibility point was checked.** On 7.3.2, RAPIDS broke on one Spark internal, and
  that one is fine here. RAPIDS uses other Spark internals for operators the smoke test doesn't
  run, and one of those could still fail. If a job fails with `NoSuchMethodError` or
  `NoSuchFieldError`, report the stack trace.
- **Iceberg tables and other DataSource V2 writes were not tested.** The smoke test writes nothing.
  On a Base cluster, the same RAPIDS 26.02.0 jar failed every DataSource V2 write (Iceberg tables,
  `df.writeTo(...)`, the `noop` sink) with `IllegalAccessError`, when the session had Iceberg
  loaded; see [`README-719-base.md` §11](README-719-base.md#11-known-limitations). Whether this CDE
  image loads Iceberg wasn't checked, so test such writes before relying on them.
- **Cloudera's own GPU runtime image wasn't tried.** The virtual cluster's configuration names
  `dex-spark-runtime-gpu-3.5.4-7.1.9.1078:1.27.0-b693`. If your entitlement gives you access to it,
  it may be an alternative to a custom image.
- **x86-64 only.** The image is built for `linux/amd64`.
