# CDE cuDF for Spark

Enabling **Spark GPU acceleration on Cloudera Data Engineering (CDE)** running on Cloudera Private
Cloud, as a step toward running a Spark RAPIDS (cuDF) workload on-prem.

**Outcome: working.** A Spark RAPIDS query runs end-to-end on an NVIDIA A10G on CDE 1.26, verified by
an `isFinalPlan=true` plan containing 8 `Gpu*` operators and zero CPU operators below
`GpuColumnarToRow`.

Getting there meant routing around **two** separate blockers:

1. CDE 1.26's own GPU runtime images are not published to any registry this cluster can reach, so a
   **custom runtime** has to supply the NVIDIA pieces itself (section 6).
2. The OSS RAPIDS jar **cannot link** against Cloudera's Spark `3.5.4.1.26.732.0-45` — Cloudera added a
   defaulted 6th parameter to `MapPartitionsRDD`, which is source-compatible but binary-incompatible,
   so every GPU plan died at the first action with `NoSuchMethodError`.

The second is fixed by recompiling **two unmodified upstream classes** (of 17,833) against the
cluster's own `spark-core`, using the CDE base image as its own toolchain, and shipping the result as a
**CDE custom runtime** — which needs no VC recreate.

➡ **Runbook, exact version strings, Docker + CDE CLI steps, and verified output:
[`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md)**
➡ **What this does and does not make possible: section 8** — read this before relying on it.
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

## 6. Blocker 1: a custom runtime image has to supply the NVIDIA pieces

Enabling GPU acceleration makes CDE select a **GPU variant** of its runtime image — and on this build
those images are not published to any registry the cluster can reach. They return `NotFound` (not
`unauthorized`; the CPU variants pull fine from the same prefix), plausibly a paid entitlement that
Community Edition does not carry. With the flag on and nothing else done, a GPU job gets
`ImagePullBackOff` rather than a GPU.

The fix is a **CDE custom runtime**: build our own image on CDE's *CPU* Spark runtime base and add the
GPU parts ourselves. A custom runtime overrides the VC's `gpuImage` selection outright and needs **no
VC recreate**, which makes it better than the chart override even where both are possible.

What actually has to go into that image is small — only two things:

| Added to the base image | Why |
|---|---|
| `rapids-4-spark_2.12-26.02.0-cuda12.jar` → `/opt/spark/jars/` | The RAPIDS plugin **and** the native GPU libraries. The jar bundles cuDF and the CUDA runtime it needs, so **no CUDA toolkit install and no NVIDIA base image are required** — that is why this is a `COPY`, not a package install. It must land in `jars/`, not `optional-lib/`: `RapidsShuffleManager` has to resolve while the executor is still starting. |
| `getGpusResources.sh` → `/opt/cde/gpu/` | The discovery script Spark calls to enumerate GPUs and assign them to executors. Ours replaces the stock one so that a missing `nvidia-smi` yields a valid empty address list — a misconfiguration then surfaces as "0 GPUs" instead of a JSON parse error that names nothing. |

What does **not** go in, and is worth knowing so you don't look for it:

- **The NVIDIA driver** (615.71.09 / CUDA 13.4 here) lives on the host, not in the container. A 13.4
  driver runs CUDA 12 binaries fine, which is why the `cuda12` jar is correct.
- **The Kubernetes device plugin** — ECS supplies it. The `nvidia` Ansible role deliberately skips
  `nvidia-ctk runtime configure`, yet workers still advertise `nvidia.com/gpu: 1`. GPU *scheduling* was
  never the problem.
- **Anything to make the base image GPU-aware.** Keep the CDE runtime base: that is what makes the
  result a legal `custom-runtime-image` resource, and it must still run as uid 1345 (CDE rejects a
  runtime image that would run as root).

Build and registration commands are in
[`docs/custom-gpu-runtime.md`](docs/custom-gpu-runtime.md). The image-availability evidence, the
re-runnable probe, and the chart escape hatch are in
[`docs/gpu-image-availability.md`](docs/gpu-image-availability.md) — re-probe after any registry,
entitlement, or CDE version change, because **if Cloudera's GPU runtime becomes available, the right
move is to delete all of this and use it.**

## 7. Blocker 2: the OSS RAPIDS jar cannot link against Cloudera's Spark

With a pullable image in hand, the next failure was a **linkage** error — a category no amount of
configuration reaches:

```
java.lang.NoSuchMethodError: 'void org.apache.spark.rdd.MapPartitionsRDD.<init>(
    org.apache.spark.rdd.RDD, scala.Function3, boolean, boolean, boolean,
    scala.reflect.ClassTag, scala.reflect.ClassTag)'
  at org.apache.spark.rapids.LocationPreservingMapPartitionsRDD.<init>(…:44)
  at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar(GpuExec.scala:341)
```

### What Cloudera changed

Cloudera's `3.5.4.1.26.732.0-45` carries a **modification to Apache Spark's own
`org.apache.spark.rdd.MapPartitionsRDD`**: a sixth constructor parameter,
`isDeterministic: Option[Boolean]`, feeding a new private `defaultMapOutputDeterministicLevel()` that
`getOutputDeterministicLevel()` reads. Verified with `javap` against the cluster's own
`spark-core_2.12-3.5.4.1.26.732.0-45.jar`. There is **no 7-arg overload** left.

Vendor distributions carrying their own patches is normal and expected. What makes this one bite is the
*shape* of the change: a **defaulted** parameter is

- **source-compatible** — anything that compiles against Apache Spark still compiles against
  Cloudera's, because the Scala compiler fills the parameter in; so all of Cloudera's own code, and
  their entire CI, recompiles and passes cleanly, and
- **binary-incompatible** — a class compiled *earlier*, against Apache Spark, has the 7-arg descriptor
  already baked into its bytecode.

The JVM resolves a method call by **exact descriptor match**, with no overload fallback at link time.
So only **pre-compiled third-party consumers** break — which is exactly what the OSS RAPIDS jar is, and
why nothing in Cloudera's own test matrix would have caught it.

It is also unavoidable rather than operator-specific: the call site is reached from
`GpuExec.doExecuteColumnar`, the base method **every** GPU operator inherits. Any GPU plan at all hits
it on its first action.

### Why the fix is a recompile and not a code change

Here is NVIDIA's source, unmodified — note it passes **five** arguments and never mentions
`isDeterministic`:

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

Because the new parameter is defaulted, **that same text is already legal against Cloudera's
constructor.** Compile it against Cloudera's `spark-core` and scalac emits the 8-arg call on its own,
filling the gap from Cloudera's `$lessinit$greater$default$6()` — i.e. with **Cloudera's own default
value**, not a guess of ours.

So the patch alters **no RAPIDS logic whatsoever**. The only difference between our classes and
NVIDIA's is the method descriptor in the emitted bytecode. There is no behavioural change to review, no
judgement call about what `isDeterministic` ought to be, and nothing that can drift from upstream
semantics. That is what makes a patched third-party jar defensible here at all.

The alternative — *patching logic* — would mean rewriting the subclass to construct its parent
differently, or rewriting the descriptor with a bytecode tool. Either produces code that is no longer
NVIDIA's, needs its own correctness review, and is then hard-wired to Cloudera's 8-arg shape so it
breaks on Apache Spark. Recompiling has none of those properties.

**The CDE base image is its own toolchain**, which is what makes this cheap and removes all doubt about
what we compiled against: it already ships `scala-compiler-2.12.19.jar` *and* the exact `spark-core`
jar. Compile `-target:jvm-1.8` (every class in the jar is major version 52), and write the output back
to both prefixes RAPIDS loads from — `spark354/` for the classes, `spark-shared/` for the `$`
default-arg companions.

### Why only two classes

A byte-exact scan of all 17,833 classes in the jar for the 7-arg descriptor found just **two** real
classes (plus one synthetic companion):
`org.apache.spark.rapids.LocationPreservingMapPartitionsRDD` and
`org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD`. Both are thin
`MapPartitionsRDD` subclasses whose whole body is a constructor forward — one adds a
`getPreferredLocations` override, the other adds nothing. Counting the callers *before* estimating the
work is what separated "patch it" from "fork it."

The build asserts via `javap -c` that both emit the 8-arg call, and that exactly 4 patched entries land
in the jar. If a future base image drops the parameter, scalac would silently emit the 7-arg call again
and the patch would become a no-op — better to fail the build than ship that.

### It is a build window, not a fork-wide difference

| Spark build | Runtime / CDP | ctor |
|---|---|---|
| Cloudera `3.5.1.1.23.7218.0-28` | 7.2.18 / 1.23 | 7 args — links |
| Cloudera `3.5.4.1.25.731.0-41` | 7.3.1 / 1.25 (what CAI runs) | 7 args — inferred¹ |
| Cloudera `3.5.4.1.26.732.0-45` | **7.3.2 / 1.26 (here)** | **8 args — fails** |
| Apache 3.5.4–3.5.7, 4.0.0, 4.0.1 | — | 7 args |

So the parameter entered in the Cloudera 7.3.2/1.26 line. This reconciles a real contradiction worth
recording: the CAI demos run the *same* dependency set — RAPIDS 26.02.0, `spark354` shim, cuda12 —
successfully, on `3.5.4.1.25.731.0-41`. Same dependencies, different Spark build.

¹ 1.25's jars were never read directly, but the arity follows: CAI runs RAPIDS green on 1.25, and the
broken call sits in `GpuExec.doExecuteColumnar`, which **every** GPU operator inherits. An 8-arg ctor
there would fail every GPU query on 1.25 too. Note this is the *only* measured difference between the
1.25 and 1.26 Spark builds — `FileScan` and `InSubqueryExec`, checked on both, are identical.

Also ruled out rather than assumed: **no OSS RAPIDS release can fix this**, because the 8-arg
constructor exists in no Apache Spark release, so NVIDIA has never had a Spark to compile it against.
Nor can `shims-provider-override` — `javap -c` shows all of `spark354/355/356/357` emit the 7-arg call.

Full evidence in [`docs/oss-rapids-vs-cloudera-spark.md`](docs/oss-rapids-vs-cloudera-spark.md); the
patch, its provenance and how to verify it against upstream in
[`runtime/README-patch.md`](runtime/README-patch.md).

## 8. What this does and does not make possible

First, the thing most likely to be misread: **Spark itself is untouched.** We did not modify, rebuild,
or override any part of Cloudera's Spark distribution — it runs exactly as shipped. The only changed
artifact is the **RAPIDS plugin jar**, and the change to it is a recompile of two classes with identical
source, not an edit. Nothing about this alters Spark's behaviour for any other job, GPU or not.

### Verified working

Run 6, end to end on an A10G: plugin load, shim selection, GPU discovery and assignment, RAPIDS plan
conversion, and these 8 operators with **no CPU operator below `GpuColumnarToRow`**:

```
GpuRange → GpuProject → GpuHashAggregate → GpuColumnarExchange
  → GpuCustomShuffleReader → GpuShuffleCoalesce → GpuHashAggregate → GpuTopN → GpuColumnarToRow
```

That covers projection, hash aggregation, a **real GPU shuffle**, top-N/sort, and the columnar↔row
boundary. The reason this generalises further than a 9-operator list suggests: the constructor that was
broken is reached from `GpuExec.doExecuteColumnar`, which **every** GPU operator inherits. The fix is at
the base of all of them, not in the particular operators above.

### What about everything else RAPIDS calls?

The patch fixes **one** `NoSuchMethodError`. Whether it is the only one was measured rather than
guessed — not by diffing Cloudera's Spark against Apache's (thousands of irrelevant differences), but by
asking the one question the JVM asks at link time: *for every Spark symbol the RAPIDS jar references,
does a member with a matching descriptor exist in the jars in this image?*

| | |
|---|---|
| Classes in scope (after filtering to the shims that load on 3.5.4) | 5,397 of 17,833 |
| Unique external Spark references | 4,473 |
| Resolve | 4,457 — **99.6%** |
| Do not resolve | **16** |
| Still reachable in a default configuration after triage | **1** |

The 16 triage down as follows — a raw miss list is not a defect list, so each was traced to its calling
bytecode:

| missing symbol | verdict |
|---|---|
| `InSubqueryExec.copy` + 6 defaults | **Dead code.** The caller is an orphan anonfun in `spark-shared/`; `spark354/` ships its own `FileSourceScanExecMeta` that shadows it and never references `InSubqueryExec`. Initially ranked the top risk — it is not a risk. |
| `FileScan.…$$normalizedPartitionFilters$` / `…DataFilters$` | **Real mismatch, unreachable.** `FileScan` is the DataSource **v2** path; `spark.sql.sources.useV1SourceList` keeps parquet/orc/avro on v1, so `GpuParquetScan`/`GpuOrcScan`/`GpuAvroScan` never load. Also present on **1.25**, so not a 1.26 regression. |
| `CreateHiveTableAsSelectCommand.outputColumns` | **Mis-attributed.** RAPIDS calls `DataWritingCommand.outputColumns$`, which 1.26 declares. |
| push-based shuffle merge (2), storage-partitioned join (3) | Gated behind flags that default to off. |
| **`CreateHiveTableAsSelectCommand.getWritingCommand`** | **Real and reachable.** The one that survives. |

Full method, per-finding evidence, and the scan's limits: [`docs/linkage-scan.md`](docs/linkage-scan.md).

### The one real remaining gap: Hive-serde CTAS

```
RAPIDS calls:  CreateHiveTableAsSelectCommand.getWritingCommand(SessionCatalog, CatalogTable, boolean)
1.26 declares: private                        getWritingCommand(CatalogTable, boolean)
```

Apache 3.5.9 declares it the same way Cloudera does, so this is **Apache maintenance drift after 3.5.4**
rather than a fork difference — it would break on Apache 3.5.9 too.

It is reached only by `CREATE TABLE … AS SELECT` or `saveAsTable` against a **Hive-serde** table
(`USING hive`, `STORED AS …`). Default-provider `saveAsTable` routes to
`GpuCreateDataSourceTableAsSelectCommand`; writes into an existing table go to `GpuInsertIntoHiveTable`;
Hive **reads** never touch it. It is deliberately **not** patched pre-emptively — it may never fire, and
unlike `MapPartitionsRDD` it would need a real source edit, not a recompile, because the argument list
changed.

### Could still break — and how you would know

The scan bounds **one** class of failure: linkage against Spark. It says nothing about *behaviour* behind
a matching signature, nothing about reflection or service loaders, and nothing about RAPIDS against
Hadoop/Hive/Parquet or the cuDF native layer. Treat the 16 as predictions to test. **Porting the 250M-row
Hive ETL is the test**; a `spark.range()` smoke test touches far less Spark surface.

Linkage failures announce themselves unmistakably: a `NoSuchMethodError`, `NoSuchFieldError` or
`AbstractMethodError` **naming a Spark class** (not a RAPIDS one) in the first action of a query. The loop
is short and always the same — read the error, `javap` the named Spark class out of the runtime image,
drop the offending RAPIDS source into `runtime/patch/`, rebuild. Minutes, not days.

One behavioural question the scan cannot see is worth naming: the patch makes RAPIDS inherit Cloudera's
default `isDeterministic`, which feeds `getOutputDeterministicLevel()` and decides whether a partition may
be recomputed after a fetch failure. Whether that default suits a GPU columnar RDD is uninvestigated, and
would surface only under task retry or node loss.

### Will not be fixed by this approach

- **A genuinely removed or renamed API.** The recompile trick works *only* because the added parameter
  is defaulted, leaving upstream source still valid. If Cloudera removes a method RAPIDS calls, or
  changes a signature incompatibly at the source level, the source would no longer compile and you
  would be doing a real port — a different and much larger job. `getWritingCommand` above is exactly
  this case: its argument list shrank, so a recompile cannot fix it and a source edit would be needed.
- **Client-version skew unrelated to RAPIDS** — Hive, Ozone, Ranger. (Relevant mainly as the reason the
  "use a 7.3.1 image" route was abandoned: it would put 7.3.1 clients against 7.3.2 services, which
  `spark.range()` would never have caught.)
- **The GPU budget.** 3 GPUs, 1 per worker, `sharing-strategy=none`. Concurrency is capped at 3
  regardless of anything here.
- **Support.** This is neither a Cloudera- nor an NVIDIA-supported configuration, and the patched jar
  gets no security or bugfix stream. It is an internal prototype.

### Version lock — this is pinned harder than it looks

The patch is valid for exactly **RAPIDS 26.02.0** against **Spark `3.5.4.1.26.732.0-45`**. A CDE
upgrade changes `spark-core`, so the image must be rebuilt — and the build assertion will tell you
whether the patch is still needed at all. A RAPIDS upgrade means re-extracting the two sources at the
matching upstream tag. Neither is hard; both are mandatory.

## 9. Next steps and alternatives

**Re-run the probe after any registry, entitlement, or CDE version change** —
`probes/probe_images.sh <vcId>` is the cheapest way to tell whether blocker 1 still holds. If Cloudera's
own GPU runtime (or just its Cloudera-built RAPIDS jar) becomes available, **delete the patch and use
it.**

**Port the real workload.** The smoke test proves linkage and plan conversion; it does not prove a
250M-row Hive ETL. That is where any remaining signature change would surface — see section 8.

**The chart escape hatch**, if a custom runtime were ever not an option:
`dexapp.api.sparkRuntime.gpuImage.override` and its `livyRuntime` twin can point at an arbitrary image,
but being chart values they are **create-time only**. Details, and where to host such an image given
that no local registry or pull-secret plumbing exists, are in
[`docs/gpu-image-availability.md`](docs/gpu-image-availability.md).

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
docs/gpu-image-availability.md        blocker 1 in full: the probe, its reasoning, the chart hatch
docs/oss-rapids-vs-cloudera-spark.md  the build-range evidence for blocker 2
docs/linkage-scan.md        how much else is broken: 4,473 refs scanned, 16 misses, 1 reachable
docs/rapids-spark-conf.md   Spark RAPIDS configuration and the version-lock matrix
docs/cde-pvc-prerequisites.md         DE role + keytab onboarding, both easy to miss
```
