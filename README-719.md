# GPU-accelerated Spark on CDE: Cloudera Private Cloud with Runtime 7.1.9

← [Overview and other versions](README.md)

This guide sets up a Cloudera Data Engineering (CDE) virtual cluster that runs Spark SQL and DataFrame
jobs on NVIDIA GPUs, using the open-source [NVIDIA RAPIDS Accelerator for Apache
Spark](https://nvidia.github.io/spark-rapids/). The accelerator ships in a **CDE custom runtime
image**: CDE's own Spark runtime with the RAPIDS jar added. Each job opts in by naming that image, and
jobs that don't name it are unaffected.

> **Support status.** This is a tested reference configuration, not a Cloudera-supported one. Validate
> it against your own workloads before relying on it.

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

**Use these exact versions together.** The RAPIDS jar version, its shim (`spark354`) and the CDE
Spark build are locked to one another. On another CDE or Runtime version, check the `spark-core` jar
in your base image first; see [Other versions](#8-other-versions).

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

Expect `gpu=1` (or more) on each GPU worker. If the GPU workers have no taints, no pod tolerations are
needed.

### 3.2 Give the CDE service a GPU quota

```bash
cdp de list-services                       # note the clusterId, e.g. cluster-xxxxxxxx
cdp de update-service --cluster-id <clusterId> --gpu-requests <totalGpus>
```

This updates the service in place; it does not need recreating.

### 3.3 Create the virtual cluster with the API, not the console

> **GPU acceleration can only be set when a virtual cluster is created.** A virtual cluster created
> in the console gets `gpuAccelerationEnabled: false`, and the setting cannot be changed afterwards.
> In that state GPU jobs run on the CPU without any error. Create the virtual cluster with the script
> below.

```bash
python scripts/create_vc_gpu.py --cluster-id <clusterId> --name <vcName>
```

The script enables `dexapp.api.gpuAcceleration.enabled`. It sets the virtual cluster's GPU ceiling to
3 and its **guaranteed** GPU requests to `0`, which is required. Before creating, edit `gpuRequests`
and the CPU and memory values in `build_request()` to fit your cluster. Add `--dry-run` to print the
request without sending it. Installation takes about 3 minutes; poll with
`cdp de describe-vc --cluster-id <clusterId> --vc-id <vcId>`.

### 3.4 Verify the virtual cluster

On the ECS master host:

```bash
scripts/verify_gpu_vc.sh <vcId>
```

The script uses ECS's RKE2 `kubectl` through `sudo` by default; set `KUBECTL=...` to override that.

It must print `gpuAccelerationEnabled: true`. It also shows the GPU limit of the virtual cluster's
queue, and the runtime images the virtual cluster uses. Note the `image:` line, because step 4.2
builds on that image.

## 4. Build and register the custom runtime

### 4.1 Download the RAPIDS jar

```bash
cd runtime
curl -fL --retry 3 -O \
  https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.0/rapids-4-spark_2.12-26.02.0-cuda12.jar
```

The `cuda12` build includes cuDF and the CUDA runtime libraries, so the image needs no CUDA toolkit
and no NVIDIA base image. It needs an NVIDIA driver on the hosts that supports CUDA 12.

### 4.2 Build and push

Set `BASE` to the `image:` value from step 3.4, which is the CPU Spark runtime your virtual cluster
already uses:

```bash
BASE=container.repository.cloudera.com/cdp-private/cloudera/dex/dex-spark-runtime-3.5.4-7.1.9.1078:1.27.0-b693
IMAGE=<your-registry>/cde-rapids-runtime-3.5.4:26.02.0-7.1.9

docker login "${BASE%%/*}"   # the registry that hosts BASE
docker build --platform linux/amd64 -f Dockerfile.719 --build-arg BASE="$BASE" -t "$IMAGE" .
docker push "$IMAGE"
cd ..
```

The build prints `upstream MapPartitionsRDD ctor OK`. If it fails with `FATAL: base MapPartitionsRDD
lacks the upstream 7-arg ctor`, the base image is not the Spark build this guide covers; see
[Other versions](#8-other-versions).

`Dockerfile.719` adds three things to the base image:

- the RAPIDS jar, in `/opt/spark/jars/`;
- the GPU discovery script, at `/opt/cde/gpu/getGpusResources.sh`;
- ownership changes for both files.

It keeps the CDE base image and its non-root user (uid 1345), which CDE requires of a custom runtime.

### 4.3 Register the image with CDE

```bash
cde resource create --name cde-rapids-runtime-719 \
  --type custom-runtime-image --image-engine spark3 --image "$IMAGE"
```

We tested with a public registry. For a private registry, first create a Docker credential with
`cde credential create --type docker-basic`, then pass it with `--image-credential <name>`. This path
has not been tested here.

## 5. Run the smoke test

The smoke test checks that each query is planned on the GPU, not just that the job finishes. RAPIDS
falls back to the CPU silently, so a green job proves nothing by itself. The test fails unless the
**final** physical plan contains `Gpu*` operators.

```bash
CDE_PROFILE=<cdeCliProfile> RUNTIME_RESOURCE=cde-rapids-runtime-719 \
  ./scripts/submit_rapids_smoke_test.sh

cde --config-profile <cdeCliProfile> run logs --id <runId> --type driver/stdout --follow=false
```

Always set `CDE_PROFILE`, because the script's default profile name is specific to our test
environment.

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

Use [`scripts/submit_rapids_smoke_test.sh`](scripts/submit_rapids_smoke_test.sh) as the template.
Every job needs the custom runtime (`--runtime-image-resource-name cde-rapids-runtime-719`) and these
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
`--conf`, so those values are silently replaced with 1g.

While tuning, add `--conf spark.rapids.sql.explain=NOT_ON_GPU`. RAPIDS then logs every operator it
left on the CPU, with the reason. For production sizing guidance, see
[`docs/rapids-spark-conf.md`](docs/rapids-spark-conf.md).

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `authorization policy denies` on every call | No DE resource role | Grant one in IAM, then wait up to 5 minutes ([prerequisites §1](docs/cde-pvc-prerequisites.md)) |
| Runs fail within seconds: `keytab is not present for user …` | No keytab uploaded | Virtual cluster → Actions → Upload User Keytab ([prerequisites §2](docs/cde-pvc-prerequisites.md)) |
| Job succeeds, but the plan has no `Gpu*` operators | `gpuAccelerationEnabled: false`, or a missing setting from §6 | Run `verify_gpu_vc.sh`. If the flag is false, recreate the virtual cluster with `create_vc_gpu.py`. |
| Executor pod stays `Pending` | GPU quota exhausted or not set | Check the service quota (§3.2), the virtual cluster ceiling, and other running GPU jobs |
| `NoSuchMethodError: …MapPartitionsRDD.<init>` | The image was built on a different Spark build | Rebuild on the 7.1.9 base (§4.2); see §8 |
| `docker build` fails at the ctor check | `BASE` is not the 7.1.9 Spark build | Use the `image:` value from `verify_gpu_vc.sh` |

## 8. Other versions

Runtime 7.3.2 / CDE 1.26 ships a Spark build that the published RAPIDS jar cannot link against. That
version needs `runtime/Dockerfile` instead, which patches the jar; see [`README-732.md`](README-732.md).
`Dockerfile.719` checks for the correct build when the image is built, so a mismatch fails the build
and does not reach a running job. To check any other base image yourself:

```bash
docker run --rm --platform linux/amd64 --entrypoint bash "$BASE" -c \
  'javap -cp "$(ls /opt/spark/jars/spark-core_*.jar)" -p org.apache.spark.rdd.MapPartitionsRDD | grep "MapPartitionsRDD("'
```

If the constructor ends `boolean, boolean, boolean, scala.reflect.ClassTag<U>, scala.reflect.ClassTag<T>)`,
`Dockerfile.719` applies.

## 9. Known limitations

- **Validated with the smoke test only.** The test shows that RAPIDS loads, executors see the GPU, and
  aggregation, shuffle and projection run on it. It has not been benchmarked against a production
  workload on this version.
- **Only the RAPIDS ↔ Spark link point that failed on 7.3.2 has been checked here.** A different Spark
  internal used by an operator the smoke test does not exercise could still fail at runtime. If a job
  fails with `NoSuchMethodError` or `NoSuchFieldError`, report the stack trace.
- **Cloudera's built-in GPU runtime image was not evaluated.** The virtual cluster's configuration names
  `dex-spark-runtime-gpu-3.5.4-7.1.9.1078:1.27.0-b693`. If it is available to your entitlement, it may
  be an alternative to a custom image.
- **x86-64 only.** The image is built for `linux/amd64`.
