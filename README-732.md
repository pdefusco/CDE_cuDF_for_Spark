# CDE cuDF for Spark on Runtime 7.3.2 / CDE 1.26

← [Overview and other versions](README.md) · Runtime 7.1.9: [`README-719.md`](README-719.md)

This is the write-up of getting Spark RAPIDS (cuDF) GPU acceleration working on CDE 1.26, on a
Cloudera Private Cloud running Runtime 7.3.2.

**Outcome: it works.** A Spark RAPIDS query runs end to end on an NVIDIA A10G on CDE 1.26. The proof
is the query's final plan (`isFinalPlan=true`): it contains 8 `Gpu*` operators, and no CPU operator
below `GpuColumnarToRow`.

Two problems had to be worked around. The overview lists them as problems 2 and 3:

1. **CDE 1.26 has no GPU runtime image this cluster can pull.** CDE's own GPU images aren't
   published to any registry the cluster can reach. So a **custom runtime** image has to bring the
   NVIDIA pieces itself (section 6).
2. **The open-source RAPIDS jar doesn't link against Cloudera's Spark `3.5.4.1.26.732.0-45`.**
   Cloudera added a sixth parameter, with a default value, to the constructor of Spark's internal
   `MapPartitionsRDD` class. Code compiled against Cloudera's Spark is fine. But the RAPIDS jar was
   compiled against Apache Spark, so every GPU query failed on its first action with
   `NoSuchMethodError` (section 7).

The fix for the second problem is to recompile **two unmodified NVIDIA classes**, out of 17,833 in the
jar, against the cluster's own `spark-core`. The CDE base image already contains the compiler and the
right `spark-core`, so it serves as the build environment. The result ships in the same **CDE custom
runtime**, and the virtual cluster doesn't have to be recreated.

➡ **Runbook, exact version strings, Docker and CDE CLI steps, and verified output:
[`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md)**
➡ **Section 8 says what this does and doesn't make possible.** Read it before relying on any of this.
➡ Why the patch is legitimate, and what it doesn't cover:
[`runtime/README-patch.md`](runtime/README-patch.md)

The linkage problem is specific to this Spark build. On Runtime 7.1.9 the open-source jar works
unmodified: see [`README-719.md`](README-719.md) for the install guide, and
[`.internal/README-719.md`](.internal/README-719.md) for how that was established.

This is an internal technical prototype, not a supported configuration. The runtime image contains a
modified third-party artifact.

## 1. Environment

| | |
|---|---|
| Private Cloud | Data Services 1.5.5-h3000, Community Edition (ECS / RKE2 v1.33.12) |
| Cloudera Manager | 7.13.2.6 |
| Runtime | 7.3.2.0 |
| CDE | 1.26.0-b557 |
| Spark | 3.5.4 (Cloudera build `3.5.4.1.26.732.0-45`) |
| GPU workers | 3 × `g5.8xlarge`: 32 vCPU, 128 GiB and 1× NVIDIA A10G 24 GB each |
| Worker OS / arch | RHEL 9.6, **amd64** |
| NVIDIA driver | 615.71.09 (CUDA 13.4) |
| CDE service | `ds0928-de` / `cluster-j6th9czl` |
| Virtual cluster | `cuDFforSpark` |

**There are 3 GPUs in total.** Each worker advertises `nvidia.com/gpu: 1`, with
`gpu.sharing-strategy=none` and `mig.capable=false`. So at most 3 executors can hold a GPU at once,
across all jobs. Size `spark.executor.instances` by GPUs, not by vCPUs.

## 2. Cluster access

- **Management Console:** `https://console-cdp.apps.ecs.<gateway-ip>.nip.io`
  On the login page, choose **local administrator** and log in as `admin`. The password is the
  `common_password` value in the deployment repo's `config.yml`. Don't use the default LDAP
  connector: it returns `temporarily_unavailable` for *every* user, including users that bind to
  FreeIPA without trouble. The fault is in the control plane's `cm-ldap` provider, not in the
  directory.
- **Cloudera Manager:** `https://cm.<gateway-ip>.nip.io`
- **`kubectl`** runs on the ECS master, not on your workstation:
  ```bash
  sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml get nodes
  ```
- **SSH to ECS nodes by IP address, not by hostname.** The generated `<prefix>-ssh.config` has the
  line `Host *.cldr.internal, 10.10.*`. OpenSSH doesn't treat a comma as a separator there, so the
  first pattern is read as the literal text `*.cldr.internal,`. No hostname matches it, so the whole
  block, including its `ProxyJump`, is skipped without any message. The `10.10.*` pattern is read
  correctly, so connecting by IP works. The comma comes from `tf_cluster_aws/hosts_common.tf` in the
  deployment repo, so every `terraform apply` writes it again.

## 3. Steps

### 3.1 Check that the workers can schedule GPUs

```bash
sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml \
  get nodes -o json | jq -r '.items[] | "\(.metadata.name) gpu=\(.status.allocatable["nvidia.com/gpu"]) taints=\(.spec.taints // "NONE")"'
```

Each worker should show `gpu=1` and `taints=NONE`. Because the workers have no taints, **you don't
need a `podTemplateFile` with tolerations**. That's often assumed to be necessary, and here it isn't.

### 3.2 Set the GPU quota on the CDE service

```bash
cdp de update-service --cluster-id cluster-j6th9czl --gpu-requests 3
```

This changes the existing service; you don't have to recreate it.

### 3.3 Create the virtual cluster with GPU acceleration turned on

This is the step that matters most, and it only works at creation time; see section 5.

```bash
python scripts/create_vc_gpu.py --cluster-id cluster-j6th9czl --name cuDFforSpark
```

Installation takes about 3 minutes. The status goes `AppInstallationInitiated` → `AppFSPolicyCreated`
→ `AppInstalling` → `AppInstalled`.

### 3.4 Check that the setting took effect

```bash
scripts/verify_gpu_vc.sh <vcId>
```

What decides it is whether the virtual cluster's `dex.yaml` contains `gpuAccelerationEnabled: true`:

```bash
kubectl get cm <vcId>-api-cm -n <vcId> -o jsonpath='{.data.dex\.yaml}' | grep gpuAccelerationEnabled
```

The script also checks that the virtual cluster's YuniKorn leaf queue has `nvidia.com/gpu: "3"` under
`max`.

## 4. How a job requests a GPU

**This was tested on this cluster:** job run 6 succeeded, with 8 `Gpu*` operators in its final plan.
The full procedure (build, push, `cde resource create`, `cde spark submit`) and the driver output are
in [`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md). In short:

```bash
cde --config-profile <profile> resource create \
  --name cde-rapids-runtime-p1 --type custom-runtime-image --image-engine spark3 \
  --image pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-p1

RUNTIME_RESOURCE=cde-rapids-runtime-p1 ./scripts/submit_rapids_smoke_test.sh
```

The submit script sets every RAPIDS setting the job needs, including the shim override and the Kryo
registrator (problem 1 in the [overview](README.md)), with a comment on each. Which jar, shim and
shuffle manager go together is in `docs/rapids-spark-conf.md`.

**Size the driver and executors with CDE's own flags, not with `--conf`.** CDE applies its job spec
after your `--conf` values, so `--conf spark.executor.memory=8g` is replaced by the spec default of
1g, without any warning. The logs show it: run 3's `ResourceProfile` recorded executor `memory=1024`
despite the setting, and run 4 recorded `8192` once the same sizing moved to CDE's flags. Don't leave
the driver at the 1g default either. If the container's memory limit kills the driver, there's no
Java stack trace: the log just stops, which makes it very hard to diagnose. (No run here actually hit
this; it's a risk, not something we observed.)

## 5. Gotchas

- **GPU settings are fixed when the virtual cluster is created.** `UpdateVcRequest` has no resource
  fields and no `chartValueOverrides`. These are all of its properties: `aclUsers, clusterId,
  discardSparkConfigs, enableComputeOverride, fullAccessGroups, fullAccessUsers, sparkConfigs, vcId,
  viewOnlyGroups, viewOnlyUsers`. A wrong value means deleting and recreating the virtual cluster,
  so get them right the first time.
- **A virtual cluster created in the console has `gpuAccelerationEnabled: false`.** The console lets
  you set a GPU *quota* but not the acceleration flag. So a console-built virtual cluster looks ready
  for GPUs: `describeVc` shows `gpuRequests: 3`, and its YuniKorn queue has `nvidia.com/gpu: "3"`. But
  every job quietly runs the CPU image. Create the virtual cluster through the API.
- **Set `guaranteedGpuRequests` to `0`, not 3.** The parent YuniKorn queue has a `max` block and no
  `guaranteed` block at all, so a guaranteed GPU amount on the child has nothing to draw from. What
  allows GPU scheduling is `max: 3`. In YuniKorn, `max` is a limit, not a reservation. GPU quota is
  also **not** inherited down the queue tree, so every level has to name it.
- **Leave out `sparkVersion`.** The bundled `cdpcli` offers `SPARK3_5` and `SPARK3_5_4`, but this
  control plane rejects both with `400 INVALID_ARGUMENT`. If you leave it out, the server picks the
  right default, and `describeVc` then reports `sparkVersion: 3.5.4`. (Also, `vcTier: ALLP` reads back
  as `tier-2`. It's the same tier under a different name.)
- **`AppDeleted` is final, so don't wait for the virtual cluster to disappear.** `deleteVc` reaches
  `AppDeleted` in about 15 seconds, but the record stays in `listVcs` indefinitely. A loop that waits
  until the vcId is gone never ends. Before recreating, check instead that no virtual cluster of that
  name has a status without `Deleted` in it.
- **The `cloudera.cloud.de_virtual_cluster` Ansible module can't create this virtual cluster.** It
  takes `chart_value_overrides` and `spark_version`, but it has no `gpu_requests` and no `guaranteed_*`
  parameters at all. It would create a virtual cluster with acceleration on and `nvidia.com/gpu: 0` on
  its leaf queue, which leaves nothing to schedule against. Use the API directly.
- **The deployment repo doesn't automate virtual clusters.** `playbooks/cde-service.yml` creates only
  the CDE *service*, with `gpu_requests: null` and `chart_value_overrides: null`. The
  `cloudera.cloud.de` module does accept both, so the service quota in 3.2 could move into the
  playbook later.
- **The runtime image isn't in the virtual cluster's Spark defaults.** `<vcId>-spark-defaults` has no
  image reference at all (checked on the live virtual cluster). The images are in `dex.yaml`, as two
  keys side by side: `image` (CPU) and `gpuImage`. CDE's runtime API server picks one of them for each
  job. So an `image` that points at a CPU runtime is correct, not a fault. The acceleration flag is
  what *allows* CDE to use `gpuImage` instead.

## 6. Problem: no GPU runtime image, so a custom runtime supplies the NVIDIA pieces

With GPU acceleration on, CDE picks the **GPU version** of its runtime image. On this build those
images aren't available from any registry the cluster can reach. Pulls fail with `NotFound`, not
`unauthorized`, while the CPU images pull fine from the same path. One likely explanation is a paid
entitlement that Community Edition doesn't include. If you turn the flag on and do nothing else, a GPU
job gets `ImagePullBackOff` instead of a GPU.

The way around it is a **CDE custom runtime**: our own image, built on CDE's *CPU* Spark runtime, with
the GPU parts added. A custom runtime replaces the virtual cluster's `gpuImage` choice outright, and
switching to it doesn't require recreating the virtual cluster. That makes it a better option than
the chart override (section 9), even where both would work.

Only two things need to go into the image:

| Added to the base image | Why |
|---|---|
| `rapids-4-spark_2.12-26.02.0-cuda12.jar` → `/opt/spark/jars/` | It contains the RAPIDS plugin **and** the native GPU libraries: the jar bundles cuDF and the CUDA runtime it needs. So **you don't need to install the CUDA toolkit or use an NVIDIA base image**, and the Dockerfile just copies the jar in. It has to go in `jars/`, not `optional-lib/`, because `RapidsShuffleManager` must load while the executor is still starting up. |
| `getGpusResources.sh` → `/opt/cde/gpu/` | The discovery script Spark runs to find the GPUs and assign them to executors. Ours replaces the stock one: when `nvidia-smi` is missing, it returns a valid, empty list. A misconfiguration then shows up as "0 GPUs", instead of a JSON parse error that doesn't say what's wrong. |

Things that **don't** go into the image, so you don't go looking for them:

- **The NVIDIA driver** (615.71.09, CUDA 13.4, here) is on the host, not in the container. A CUDA 13.4
  driver runs CUDA 12 code, which is why the `cuda12` jar is the right one.
- **The Kubernetes device plugin** comes with ECS. The `nvidia` Ansible role deliberately skips
  `nvidia-ctk runtime configure`, and the workers still advertise `nvidia.com/gpu: 1`. Scheduling GPUs
  was never the problem.
- **Anything that makes the base image itself GPU-aware.** Keep CDE's runtime image as the base. That's
  what makes the result a valid `custom-runtime-image` resource. It must also still run as uid 1345,
  because CDE rejects a runtime image that would run as root.

The build and registration commands are in [`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md).
The evidence that the images are unavailable, the probe that re-checks it, and the chart override are
in [`docs/gpu-image-availability.md`](docs/gpu-image-availability.md). Run the probe again after any
change to the registry, the entitlement or the CDE version. **If Cloudera's GPU runtime becomes
available, delete all of this and use that instead.**

## 7. Problem: the open-source RAPIDS jar doesn't link against Cloudera's Spark

Once the image could be pulled, the next failure was a **linkage error**. That's a kind of failure no
configuration setting can fix:

```
java.lang.NoSuchMethodError: 'void org.apache.spark.rdd.MapPartitionsRDD.<init>(
    org.apache.spark.rdd.RDD, scala.Function3, boolean, boolean, boolean,
    scala.reflect.ClassTag, scala.reflect.ClassTag)'
  at org.apache.spark.rapids.LocationPreservingMapPartitionsRDD.<init>(…:44)
  at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar(GpuExec.scala:341)
```

**Where this came from:** job runs **3 and 4**, both on the unpatched `cde-rapids-runtime` image. The
stack trace is in **`driver/stdout`**, not `driver/stderr`. It arrives as a `Py4JJavaError` from
`collectToPython`, so the Java frames come after the Python traceback on stdout. To see it:

```bash
cde --config-profile <profile> run logs --id 4 --type driver/stdout --follow=false
```

Run 4 is the better one to look at. It has the same trace, but its sizing was already fixed: 8 GiB of
executor memory instead of run 3's 1 GiB, so there's nothing else going on in the log.

### What Cloudera changed

Cloudera's `3.5.4.1.26.732.0-45` **modifies Apache Spark's own `org.apache.spark.rdd.MapPartitionsRDD`**.
The constructor has a new sixth parameter, `isDeterministic: Option[Boolean]`. It feeds a new private
method, `defaultMapOutputDeterministicLevel()`, which `getOutputDeterministicLevel()` reads. In
bytecode the constructor now takes 8 arguments instead of 7, because Scala adds the two `ClassTag`
arguments at the end. This was checked with `javap` against the cluster's own
`spark-core_2.12-3.5.4.1.26.732.0-45.jar`. There's **no 7-argument version** left.

It's normal for a vendor's distribution to carry its own patches. What causes trouble here is the
*kind* of change. Because the new parameter has a default value, the change is:

- **compatible at the source level.** Code that compiles against Apache Spark still compiles against
  Cloudera's, because the Scala compiler fills in the default. So all of Cloudera's own code
  recompiles, and their CI passes.
- **incompatible at the binary level.** A class that was compiled *before*, against Apache Spark, has
  the 7-argument call already built into its bytecode.

The JVM looks up a method by its exact signature, and it has no fallback at link time. So only
**third-party code that ships already compiled** breaks. That's exactly what the open-source RAPIDS
jar is, and it's why nothing in Cloudera's own tests would have caught this.

It also can't be avoided by steering clear of particular operators. The failing call is reached from
`GpuExec.doExecuteColumnar`, the method that **every** GPU operator inherits. So any GPU plan hits it
on its first action.

### Why the fix is a recompile, not a code change

Here's NVIDIA's source, unmodified. It passes **five** arguments and never mentions `isDeterministic`:

```scala
class LocationPreservingMapPartitionsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T], f: (TaskContext, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false, isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
    extends MapPartitionsRDD[U, T](prev, f,
      preservesPartitioning = preservesPartitioning,
      isFromBarrier = isFromBarrier,
      isOrderSensitive = isOrderSensitive) {
```

Because the new parameter has a default, **this exact source is already valid against Cloudera's
constructor.** Compile it against Cloudera's `spark-core`, and the compiler produces the 8-argument
call by itself. It fills in the missing value from Cloudera's `$lessinit$greater$default$6()`, which
means it uses **Cloudera's own default**, not a value we chose.

So the patch changes **no RAPIDS logic at all**. The only difference between our classes and NVIDIA's
is the method signature in the compiled bytecode. There's no change in behaviour to review, no
decision about what `isDeterministic` should be, and nothing that can drift from upstream. That's what
makes it reasonable to ship a patched third-party jar here.

The alternative would be to change the code. You'd rewrite the subclass to call its parent's
constructor differently, or edit the signature with a bytecode tool. Either way, the result is no
longer NVIDIA's code, it needs its own correctness review, and it's tied to Cloudera's 8-argument
constructor, so it would break on Apache Spark. A recompile has none of those problems.

**The CDE base image has everything needed to build the patch.** That makes it cheap, and it removes
any doubt about what we compiled against. The image already contains `scala-compiler-2.12.19.jar`
*and* the exact `spark-core` jar. Compile with `-target:jvm-1.8`, since every class in the RAPIDS jar
is Java 8 bytecode (major version 52). Then write the two classes back to `spark354/`, where a 3.5.4
session loads them from. Their `$` companion objects in `spark-shared/` stay as published: they don't
call `MapPartitionsRDD`, and one of them has a method (`mapPartitions`, for the ColumnarRdd export
path) that the compiled patch source doesn't.

### Why only two classes

A byte-level scan of all 17,833 classes in the jar, looking for the 7-argument call, found just **two**
real classes, plus one compiler-generated companion:
`org.apache.spark.rapids.LocationPreservingMapPartitionsRDD` and
`org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD`. Both are thin subclasses of
`MapPartitionsRDD` that do little more than pass their arguments to the parent constructor. One also
overrides `getPreferredLocations`; the other adds nothing. Counting the callers *before* estimating the
work is what showed this could be patched rather than needing a fork.

The build checks with `javap -c` that both classes, as read back out of the patched jar, now make the
8-argument call. If a future base image drops the parameter, the compiler would quietly produce the
7-argument call again and the patch would do nothing. It's better for the build to fail than to ship
that. It also checks that the companion still has `mapPartitions`.

The published `26.02.0-p1` image predates that last check. Its build overwrote the companions too,
which breaks the ColumnarRdd export path (XGBoost and similar) with a `NoSuchMethodError`. The smoke
test and benchmark don't use that path. See [`runtime/README-patch.md`](runtime/README-patch.md).

### It's one range of builds, not all of Cloudera's Spark

| Spark build | Runtime / CDP | Constructor |
|---|---|---|
| Cloudera `3.5.1.1.23.7218.0-28` | 7.2.18 / 1.23 | 7 args: links |
| Cloudera `3.5.4.1.25.731.0-41` | 7.3.1 / 1.25 (what CAI runs) | 7 args: inferred¹ |
| Cloudera `3.5.4.1.26.732.0-45` | **7.3.2 / 1.26 (this cluster)** | **8 args: fails** |
| Cloudera `3.5.4.127.354.7191000.0-30` | 7.1.9 SP2 CHF4 / 1.27 | 7 args: links, measured² |
| Apache 3.5.4–3.5.7, 4.0.0, 4.0.1 | — | 7 args |

So the parameter was added in the Cloudera 7.3.2 / 1.26 line. This also explains something that
looked like a contradiction. The CAI demos use the *same* dependencies (RAPIDS 26.02.0, the `spark354`
shim, cuda12) and they work, on `3.5.4.1.25.731.0-41`. The dependencies are the same; the Spark build
is different.

¹ We never read 1.25's jars directly, but the argument count follows from what we know. CAI runs
RAPIDS successfully on 1.25, and the failing call is in `GpuExec.doExecuteColumnar`, which **every**
GPU operator inherits. An 8-argument constructor there would make every GPU query fail on 1.25 too.
This is the *only* measured difference between the 1.25 and 1.26 Spark builds: `FileScan` and
`InSubqueryExec`, checked on both, are identical.

² Read with `javap` from the CDE 1.27 base image, and confirmed when the unpatched open-source jar
passed the smoke test on that cluster ([`.internal/README-719.md`](.internal/README-719.md)). **Don't**
use the 7.3.2 `Dockerfile` there. Its patched classes would compile to the 7-argument call, and its
own check would fail the build. `runtime/Dockerfile.719` checks for the opposite.

Two other options were checked and ruled out:
- **No open-source RAPIDS release can fix this.** The 8-argument constructor exists in no Apache Spark
  release, so NVIDIA has never had a Spark to compile against it.
- **`shims-provider-override` can't fix it either.** `javap -c` shows that the `spark354`, `spark355`,
  `spark356` and `spark357` shims all make the 7-argument call.

The full evidence is in [`docs/oss-rapids-vs-cloudera-spark.md`](docs/oss-rapids-vs-cloudera-spark.md).
The patch, where it comes from, and how to check it against upstream are in
[`runtime/README-patch.md`](runtime/README-patch.md).

## 8. What this does and doesn't make possible

First, the point most likely to be misread: **Spark itself is unchanged.** We didn't modify, rebuild
or override any part of Cloudera's Spark; it runs exactly as shipped. The only changed artifact is
the **RAPIDS plugin jar**, and the change is a recompile of two classes from the same source, not an
edit. None of this changes how Spark behaves for any other job, GPU or not.

### Verified working

Run 6 worked end to end on an A10G. RAPIDS loaded, picked its shim, found the GPU and was assigned it,
converted the plan, and ran these 8 operators with **no CPU operator below `GpuColumnarToRow`**:

```
GpuRange → GpuProject → GpuHashAggregate → GpuColumnarExchange
  → GpuCustomShuffleReader → GpuShuffleCoalesce → GpuHashAggregate → GpuTopN → GpuColumnarToRow
```

That covers projection, hash aggregation, a **real GPU shuffle**, top-N sorting, and the conversion
from columns back to rows. This says more than a short operator list suggests. The broken
constructor is reached from `GpuExec.doExecuteColumnar`, which **every** GPU operator inherits, so the
fix applies to all of them, not just the operators above.

### What about everything else RAPIDS calls?

The patch fixes **one** `NoSuchMethodError`. We measured whether it's the only one rather than guess.
Comparing Cloudera's Spark with Apache's would turn up thousands of irrelevant differences. Instead,
the scan asks the question the JVM asks at link time: *for every Spark class member the RAPIDS jar
refers to, is there a member with the same signature in this image's jars?*

| | |
|---|---|
| Classes in scope (only the shims that load on 3.5.4) | 5,397 of 17,833 |
| Distinct references to Spark | 4,473 |
| Found | 4,457, or **99.6%** |
| Not found | **16** |
| Still reachable with default settings, after checking each one | **1** |

A list of missing symbols isn't a list of bugs, so each of the 16 was traced to the bytecode that
calls it:

| Missing symbol | Verdict |
|---|---|
| `InSubqueryExec.copy` + 6 defaults | **Never called.** The caller is an orphaned anonymous function in `spark-shared/`. `spark354/` ships its own `FileSourceScanExecMeta`, which takes precedence and never refers to `InSubqueryExec`. We first ranked this the biggest risk; it isn't a risk at all. |
| `FileScan.…$$normalizedPartitionFilters$` / `…DataFilters$` | **A real mismatch, but never reached.** `FileScan` is the DataSource **V2** read path. `spark.sql.sources.useV1SourceList` keeps Parquet, ORC and Avro on V1, so `GpuParquetScan`, `GpuOrcScan` and `GpuAvroScan` never load. It's also missing on **1.25**, so it isn't new in 1.26. |
| `CreateHiveTableAsSelectCommand.outputColumns` | **Wrongly attributed.** RAPIDS calls `DataWritingCommand.outputColumns$`, which 1.26 has. |
| Push-based shuffle merge (2), storage-partitioned join (3) | Only used when features that are off by default are turned on. |
| **`CreateHiveTableAsSelectCommand.getWritingCommand`** | **Real, and reachable.** This is the one that remains. |

The method, the evidence for each finding, and the scan's limits are in
[`docs/linkage-scan.md`](docs/linkage-scan.md).

### The one real gap left: CTAS into Hive-format tables

```
RAPIDS calls:  CreateHiveTableAsSelectCommand.getWritingCommand(SessionCatalog, CatalogTable, boolean)
1.26 declares: private                        getWritingCommand(CatalogTable, boolean)
```

Apache Spark 3.5.9 declares it the same way Cloudera does. So this is a change Apache made in a later
3.5 maintenance release, not a Cloudera difference, and it would break on Apache 3.5.9 too.

It's only reached by `CREATE TABLE … AS SELECT`, or `saveAsTable`, into a **Hive-format** table
(`USING hive`, or `STORED AS …`). Other writes take other paths:
- `saveAsTable` with the default format goes to `GpuCreateDataSourceTableAsSelectCommand`.
- Writes into an existing table go to `GpuInsertIntoHiveTable`.
- Hive **reads** never touch it.

We deliberately haven't patched it in advance. It may never be hit, and unlike `MapPartitionsRDD` it
would need a real source change, not a recompile, because the argument list changed.

### What could still break, and how you'd know

The scan rules out **one** kind of failure: linking against Spark. It doesn't cover *behaviour*
behind a matching signature, reflection, or service loaders. It also doesn't cover RAPIDS against
Hadoop, Hive or Parquet, or the cuDF native layer. Treat the 16 as predictions to test. **Porting the
250M-row Hive ETL is that test.** A `spark.range()` smoke test exercises far less of Spark.

A linkage failure is easy to recognise. It's a `NoSuchMethodError`, `NoSuchFieldError` or
`AbstractMethodError` that **names a Spark class** (not a RAPIDS one), on the first action of a query.
The fix is quick, and the steps are always the same:
1. Read the error.
2. `javap` the named Spark class out of the runtime image.
3. Put the RAPIDS source that calls it into `runtime/patch/`.
4. Rebuild.

That takes minutes, not days.

One question about behaviour, which the scan can't answer, is worth naming. With the patch, RAPIDS
gets Cloudera's default `isDeterministic`. That value feeds `getOutputDeterministicLevel()`, which
decides whether a partition can be recomputed after a shuffle fetch fails. Nobody has checked whether
that default suits a GPU columnar RDD. It would only matter when tasks are retried or a node is lost.

### What this approach won't fix

- **An API that was actually removed or renamed.** The recompile works *only* because the new
  parameter has a default, which keeps NVIDIA's source valid. If Cloudera removes a method RAPIDS
  calls, or changes a signature in a way the source can't absorb, the source won't compile any more.
  Then it's a real port, which is a different and much bigger job. `getWritingCommand` above is
  exactly this case: its argument list got shorter, so a recompile can't fix it, and the source would
  have to change.
- **Client version mismatches that have nothing to do with RAPIDS**, such as Hive, Ozone or Ranger.
  This mostly matters as the reason we dropped the idea of using a 7.3.1 image. It would put 7.3.1
  clients against 7.3.2 services, and a `spark.range()` test would never have caught that.
- **The number of GPUs.** There are 3 GPUs, 1 per worker, with `sharing-strategy=none`. At most 3
  executors can use a GPU at once, whatever else you do.
- **Support.** Neither Cloudera nor NVIDIA supports this configuration, and the patched jar gets no
  security or bug fixes. It's an internal prototype.

### The patch is tied to exact versions

The patch is valid only for **RAPIDS 26.02.0** with **Spark `3.5.4.1.26.732.0-45`**.
- **A CDE upgrade** changes `spark-core`, so the image has to be rebuilt. The build's check will tell
  you whether the patch is still needed.
- **A RAPIDS upgrade** means taking the two source files again, from the matching NVIDIA release tag.

Neither is hard, but both are required.

Runtime 7.1.9 shows that this works in both directions. Its Spark, `3.5.4.127.354.7191000.0-30`, needs
**no** patch, and the patched image is wrong there. Choose the Dockerfile by the `spark-core` jar in
the base image, not by the Spark version number, because both builds report 3.5.4.

## 9. Next steps and alternatives

**Re-run the probe after any change to the registry, the entitlement or the CDE version.**
`probes/probe_images.sh <vcId>` is the cheapest way to find out whether the GPU images are still
missing. If Cloudera's own GPU runtime, or just its Cloudera-built RAPIDS jar, becomes available,
**drop the patch and use that.**

**Port the real workload.** The smoke test shows that the jar links and the plan converts. It doesn't
show that a 250M-row Hive ETL works. That's where any remaining signature change would surface; see
section 8.

**The chart override, if a custom runtime were ever not an option.**
`dexapp.api.sparkRuntime.gpuImage.override`, and its `livyRuntime` equivalent, can point at any image.
But they're chart values, so they can only be set when the virtual cluster is created. The details,
and where such an image could be hosted given that there's no local registry or pull-secret setup,
are in [`docs/gpu-image-availability.md`](docs/gpu-image-availability.md).

**Build for the right CPU architecture.** The workers are amd64 RHEL 9.6, and the build host was an
arm64 Apple Silicon Mac. Every image build needs `--platform linux/amd64`, or it produces an image the
cluster can't run.

**Use matching RAPIDS versions.** The jar, the shim override, the shuffle manager class and Spark's
maintenance version (the `4` in 3.5.4) all have to match. See `docs/rapids-spark-conf.md`. For Spark
3.5.4, the combination that works is `rapids-4-spark_2.12:26.02.0` with the `spark354` shim.

**Check the final plan, and check it the right way.** RAPIDS falls back to the CPU without failing the
job, so neither a successful job nor a wall-clock time proves anything. And with AQE on, the obvious
way of reading the plan shows the *CPU operator names from before conversion*, even on a run that was
fully on the GPU. That gave a false `FAIL: ran on the CPU` twice here. Read the plan **after** the
action, **from the same DataFrame the action ran on**, and check that `isFinalPlan=true`. The details
are in [`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md) §6.

Full repo layout: [`README.md`](README.md#repo-layout).
