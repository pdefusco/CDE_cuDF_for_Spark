# Spark RAPIDS on Cloudera Base, Runtime 7.1.9 SP1 / CDS 3.5.4 on YARN

← [Overview](../README.md) · Customer-facing version: [`README-719-base.md`](../README-719-base.md) · CDE on 7.1.9: [`README-719.md`](README-719.md)

> **Internal engineering write-up.** How the Base result was reached, why each setting is needed, and
> how it compares to CDE on 7.1.9. For the customer-facing guide, see
> [`../README-719-base.md`](../README-719-base.md).

The question was different from the CDE work: **does the upstream RAPIDS jar run directly on
Cloudera's Spark 3.5.4 build on a Base cluster, or does Spark need a patch?** There is no CDE, no
ECS and no custom image here; Spark 3 runs on YARN and each job ships the jar with `--jars`.

**Outcome: no Spark patch, but not as-is either.** `rapids-4-spark_2.12-26.10.0-cuda12` ran every
operator of five benchmark queries on the GPU, 4.5× faster than the CPU overall, with no
`NoSuchMethodError`, `ClassCastException` or CPU fallback. It needs three settings and an HDFS home
directory, plus a way around the Iceberg 1.3 that CDS 3.5.4 configures by default. With that Iceberg
loadable, RAPIDS fails every DataSource V2 operation. There are two ways around it:
`spark.rapids.sql.suppressPlanningFailure=true`, which keeps Iceberg working with its stages on the
CPU (recommended), or a client config with no Iceberg (§3.4).

| Question | Answer |
|---|---|
| Runs as-is? | No. The ShimLoader rejects the Cloudera version string. |
| Runs with config overrides? | Yes, fully on the GPU, with no fallbacks and no errors. |
| Needs a Spark patch? | No. No binary incompatibilities with Cloudera's backports were seen. |
| Needs a RAPIDS patch? | Only to accelerate CDS's Iceberg 1.3; with `suppressPlanningFailure=true` it runs on the CPU (§3.4). |
| RAPIDS 26.02.0 (CDE's version)? | Only without DataSource V2 writes: those fail with `IllegalAccessError`, and no setting tried avoids it (§3.5). |

Internal technical prototype, not a supported configuration. Cloudera documents RAPIDS as supported
only on CDS 3.3; CDS 3.5.4 ships no `rapids-plugin/` directory.

## 1. Environment

| | |
|---|---|
| Cluster | `spark-base-cluster` (lab name `ds1113`), AWS, November 2026 |
| Services | ZooKeeper, HDFS, Hive Metastore, YARN, Spark 3 on YARN; Kerberos via FreeIPA |
| Runtime | CDH `7.1.9-1.cdh7.1.9.p1069.74330271` (SP1 + PATCH-1069), CM 7.13.2 SP2, RHEL 9.6 |
| Spark | SPARK3 parcel `3.5.4.3.5.7191000.0-30-1.p0.68499982`; `spark.version` = `3.5.4.3.5.7191000.0-30`; Scala 2.12.18; OpenJDK 17.0.21 |
| Notable jars | hadoop-client 3.1.1.7.1.9.1042-1, parquet-hadoop 1.10.99.7.1.9.1042-1, hive-exec 3.1.3000.7.1.9.1042-1, arrow-vector 12.0.1, Iceberg 1.3.0 (`lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.3.0.3.5.7191000.0-30.jar`) |
| Plugin | `com.nvidia:rapids-4-spark_2.12:26.10.0`, classifier `cuda12` (cudf 26.10.1) |
| Masters | 3 × `t3a.xlarge`; master-03 is the Spark 3 gateway the jobs were submitted from |
| Workers | 3 × `g5.4xlarge`: 1× NVIDIA A10G (24 GB), driver 620.70.06, kernel 5.14.0-741.22.1.el9_9, cgroup v1 |
| YARN | LinuxContainerExecutor + cgroups; per NodeManager 49152 MB, 14 vcores, `yarn.io/gpu` = 1 |

The cluster was built with the `cloudera-ce-ecs-aws` deployment automation, on its
`pvc-719sp1-rapids` branch (local, not pushed). That branch adds the Spark-only Base cluster
definition, the cgroup v1 boot on the workers, the NVIDIA driver role retargeted from ECS to Base
workers, and the CM YARN GPU settings listed in the customer guide §2.

## 2. Timeline: what failed, in order

Each run was submitted from the gateway with `spark3-shell -i` in client mode, so the driver log
(and the ShimLoader output) landed locally.

1. **As-is** (`base/plugin_load.sh`, `as-is.log`). The driver fails at plugin init:
   `IllegalArgumentException: This RAPIDS Plugin build does not support Spark build 3.5.4.3.5.7191000.0-30.`
2. **Shim override** (`shim-override.log`). Past the ShimLoader, the plugin refuses to run under
   Kryo (§3.2). With the registrator added, the V1 Parquet smoke test (write, read, join, aggregate,
   `show`) passes. Its pre-run `explain()` showed CPU operators, which was first read as a CPU
   fallback; it was only the pre-AQE plan (§4). The smoke test no longer prints it.
3. **Benchmark, stock config.** The CPU run was fine. The GPU run printed no results: every query
   writes to the `noop` sink, which is a DSv2 write, and each one died with
   `Unrecognized Iceberg version` (§3.4). A `try` around each query now keeps one failure from
   hiding the rest.
4. **Benchmark, Iceberg-free config.** All five queries ran on the GPU; results in §5.
5. **RAPIDS 26.02.0, stock config** (`WORK=/tmp/rapids-2602`). Shim override and smoke test pass; every
   benchmark query fails with `IllegalAccessError` (§3.5). Putting the jar on
   `spark.{driver,executor}.extraClassPath` changed nothing.
6. **Stock config, V1 sink** (`SINK=parquet`, `/tmp/rapids-exp/v1-*`). On both 26.02.0 and 26.10.0,
   all five queries ran fully on the GPU. The Iceberg failures are DataSource V2 only.
7. **Stock config, `suppressPlanningFailure=true`, `noop` sink** (`/tmp/rapids-exp/suppress-*`). On
   26.10.0, only the write node fell back and the query stages stayed on the GPU. On 26.02.0, the
   same `IllegalAccessError`.
8. **Iceberg and Hive tables** (`base/iceberg_check.sh`, 26.10.0, stock config). Results in §5.

Two harness problems also cost a run each, and both are fixed in the scripts:

- Spark's console progress bar ends lines with `\r`, so `println` output did not start at column 0
  and `^BENCH` greps found nothing. The scripts set `spark.ui.showConsoleProgress=false`, and
  `summary.sh` splits on `\r` for older logs.
- In `spark3-shell -i`, a line that starts with `.` is parsed as a new statement. Method chains must
  break after the dot, not before it.

## 3. The requirements

### 3.1 Shim override

The ShimLoader needs an exact version match. The jar has a shim for each Apache release (`spark350`
to `spark359`), and only Databricks gets special handling, so a vendor suffix is rejected. Forcing
the 3.5.4 shim works:

```
--conf spark.rapids.shims-provider-override=com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider
```

The driver then logs `Overriding Spark shims provider ... This may be an untested configuration!`.
The provider class sits in `spark-shared/` inside the jar, not in `spark354/`, but its class name has
no `spark-shared.` prefix. `plugin_load.sh` derives the name from the jar listing rather than
hard-coding it. This is the same override the CDE 7.1.9 image uses.

### 3.2 Kryo registrator

`/etc/spark3/conf/spark-defaults.conf` sets `spark.serializer=org.apache.spark.serializer.KryoSerializer`.
Without a registrator, RAPIDS stops with `The RAPIDS Accelerator when used with Kryo needs to
register some serializers`. Set `spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator`,
appended to any registrator CM already sets. `plugin_load.sh` does the append.

### 3.3 HDFS home directory

The submitting user needs `/user/<name>` for `.sparkStaging`. FreeIPA's `admin` had none. In the lab
it was created from the gateway with the SecondaryNameNode's `hdfs` keytab from the CM agent's
process directory, in a throwaway ticket cache. That is a lab shortcut, so it is not in `base/`; the
customer guide just says to have an HDFS superuser create it.

### 3.4 Iceberg in the session (26.10)

The CDS 3.5.4 client config enables Iceberg for every session:

```
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.iceberg.enabled=true
spark.hadoop.iceberg.engine.hive.enabled=true
```

`classpath.txt` also adds `SPARK3/lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.3.0.3.5.7191000.0-30.jar`
and the iceberg-hive runtime. With that config, any DSv2 read or write fails the query, Iceberg
table or not:

```
ERROR rapids.GpuOverrideUtil: Encountered an exception applying GPU overrides
java.lang.UnsupportedOperationException: Unrecognized Iceberg version: d38e7d2d800a1c64b01edc974cf1eabefdc62231
  at com.nvidia.spark.rapids.iceberg.IcebergProbeImpl.shimPackage(IcebergProbeImpl.scala:89)
  at com.nvidia.spark.rapids.iceberg.IcebergProbeImpl.getProvider(IcebergProbeImpl.scala:100)
  at com.nvidia.spark.rapids.iceberg.IcebergProvider$.apply(IcebergProvider.scala:58)
  at org.apache.spark.sql.rapids.ExternalSourceBase.icebergProvider(ExternalSource.scala:68)
  at org.apache.spark.sql.rapids.ExternalSourceBase$IcebergV2WriteRecognizer$.recognizes(ExternalSource.scala:110)
```

V1 operations are unaffected. These are Parquet paths through `spark.read` / `df.write`, and
non-Iceberg tables through `SparkSessionCatalog`, which delegates them to the V1 session catalog. That
is why the smoke test passed, and why the benchmark with `SINK=parquet` runs fully on the GPU with
the stock config. The `noop` sink made the failure look universal.

From the spark-rapids 26.10 source (`iceberg/common/.../IcebergProbeImpl.scala`, `ExternalSource.scala`):

- **Probe trigger.** RAPIDS probes Iceberg whenever
  `org.apache.iceberg.spark.source.SparkBatchQueryScan` is loadable and the Spark version is
  supported. The probe is reached from both the V2 scan and V2 write recognisers, so the `noop` sink
  triggers it.
- **Version detection.** The version comes from `IcebergBuild.gitCommitId()`, looked up in a map of
  Apache release commits. On a miss, RAPIDS parses an `iceberg-spark-runtime-X.Y_A.B-x.y.z` name
  from the jar that holds `IcebergBuild`. Cloudera's commit is not in the map, and the fallback did
  not yield a version here, so the raw commit id is used, which has no dotted parts.
- **No supported shim anyway.** Only Iceberg 1.6, 1.9, 1.10 and 1.11 have shims. Correctly detected,
  1.3 would fail with `Unsupported Iceberg version` instead. **RAPIDS 26.10 cannot accelerate CDS
  3.5.4's Iceberg at all.**
- **The format flag is ignored.** The probe does not consult `spark.rapids.sql.format.iceberg.enabled`;
  setting it to `false` changed nothing.
- **Failure, not fallback, by default.** `spark.rapids.sql.suppressPlanningFailure` defaults to
  `false`, so the exception is rethrown. With `true`, `GpuOverrideUtil` logs `Failed to apply GPU
  overrides, falling back on the original plan` and keeps the CPU plan, **but only for the plan it
  was applying to**. Under AQE the overrides run once on the whole plan, which holds the V2 node, and
  again on each query stage. Stages that contain no V2 node convert normally. In practice:
  - `noop` writes: the write node stays on the CPU, and every query stage is on the GPU.
  - Iceberg reads: the stage with the `BatchScan` (and its partial aggregate) is on the CPU, and the
    `Exchange` feeds a `GpuRowToColumnar`. The later stages are on the GPU.
  - Iceberg writes: `AppendData` stays on the CPU. The query under it is on the GPU if it has a
    shuffle. A shuffle-free CTAS is entirely on the CPU.

**Workarounds**, both in the customer guide §5:

- **`suppressPlanningFailure=true` with the stock config** (recommended). Iceberg keeps working, on
  the CPU, and everything else runs on the GPU. The cost is that a genuine planning failure elsewhere
  would also fall back silently, so check the final plan.
- **A private `SPARK_CONF_DIR`**, copied from `/etc/spark3/conf` with the Iceberg keys deleted and the
  Iceberg entries filtered out of `classpath.txt` (top of `base/benchmark.sh`). The benchmark checks
  this in-session: its `PROBE` line prints `icebergScan=not-loadable`. The session cannot read
  Iceberg tables at all. `spark_catalog` reverts to Spark's own session catalog.

**What a RAPIDS fix would look like** (upstream, NVIDIA/spark-rapids):

- make `ExternalSource.hasIcebergJar` honour `spark.rapids.sql.format.iceberg.enabled`, or
- treat an unknown or unsupported Iceberg version as "no Iceberg": log a warning and leave those
  scans on the CPU, instead of throwing.

### 3.5 RAPIDS 26.02.0 with the stock config

An earlier version of this note said 26.02.0 had no Iceberg V2 write recogniser and might run with the
stock config. **That was wrong.** It does have one, and it fails differently:

```
java.lang.IllegalAccessError: failed to access class org.apache.iceberg.spark.source.SparkWrite from class
  org.apache.iceberg.spark.source.GpuSparkWrite$ (org.apache.iceberg.spark.source.SparkWrite is in unnamed
  module of loader 'app'; org.apache.iceberg.spark.source.GpuSparkWrite$ is in unnamed module of loader
  scala.reflect.internal.util.ScalaClassLoader$URLClassLoader @111cbcda)
  at org.apache.iceberg.spark.source.GpuSparkWrite$.supports(GpuSparkWrite.scala:174) [spark-shared/]
  at com.nvidia.spark.rapids.iceberg.IcebergProviderImpl.isSupportedWrite(IcebergProviderImpl.scala:159)
  at org.apache.spark.sql.rapids.ExternalSourceBase.tagForGpu(ExternalSource.scala:302)
  at org.apache.spark.sql.rapids.ExternalSource$.tagForGpu(ExternalSource.scala:339) [spark354/]
  at com.nvidia.spark.rapids.OverwriteByExpressionExecMeta.tagPlanForGpu(v2WriteCommandMetas.scala:139)
```

`GpuSparkWrite` sits in Iceberg's own package so that it can reach the package-private `SparkWrite`.
That works only if both classes are in the same runtime package, which means the same class loader.
Cloudera's Iceberg is in the application loader, through `classpath.txt`. RAPIDS's class comes from
the jar's `spark-shared/` directory, which the ShimLoader adds to a URL class loader it can extend:
here the REPL's loader, and in `spark-submit`, Spark's mutable child loader. On JDK 17 the
application loader cannot be extended. So this fails on every V2 write, Iceberg table or not.

What was tried and did not help:
- The jar on `spark.driver.extraClassPath` / `spark.executor.extraClassPath`. The root of the jar
  moves, but `spark-shared/` is still loaded through the shim loader.
- `suppressPlanningFailure=true`. `IllegalAccessError` is a `LinkageError`, which `NonFatal` does
  not catch.

V1 operations work. With `SINK=parquet` and the stock config, all five queries ran fully on the GPU.
The CDE 7.1.9 smoke test, also 26.02.0, never did a V2 write, which is why it never hit this.

## 4. Proving GPU execution

Under AQE, `explain()` before execution prints the initial plan, which is CPU operators, even for a
query that runs entirely on the GPU. RAPIDS replaces operators per query stage as AQE re-plans. The
only trustworthy evidence is the executed plan after the query finishes. `benchmark.sh` registers a
`QueryExecutionListener`, stores `qe.executedPlan` in `onSuccess`, and prints for each query
whether it contains `isFinalPlan=true` and which distinct `Gpu*` operators it has. It also prints
the whole final plan of the window query (in the customer guide §7). The listener bus is
asynchronous, so the script sleeps 3 s before reading the plan.

`spark.rapids.sql.explain=NOT_ON_GPU` logged no fallback reasons for any of the five queries.

## 5. Benchmark

The data is 300M rows of synthetic Parquet on HDFS (3.4 GB): `k` long with 1M distinct values, `v`
double, `s` string with 97 values, `d` date. There is also a 1M-row dimension table. It is generated
on the GPU run's first pass, into `/tmp/rapids-bench`. Each query runs once to warm up, then once
timed, into the `noop` sink.

| | GPU | CPU |
|---|---|---|
| Executors | 3 × 12 cores | same |
| Heap + overhead | 16g + 10g | 22g + 4g |
| GPU | 1 per executor, `task.resource.gpu.amount=0.08`, `concurrentGpuTasks=2`, pinned pool 2g | none, plugin not loaded |
| Shuffle | Spark's own, 200 partitions | same |
| Config | Iceberg-free `SPARK_CONF_DIR` | same |

| Query | GPU (s) | CPU (s) | Speedup |
|---|---:|---:|---:|
| agg | 6.1 | 24.8 | 4.1× |
| join | 1.4 | 4.3 | 3.1× |
| sort | 3.9 | 19.8 | 5.1× |
| window | 1.9 | 12.2 | 6.4× |
| filter | 0.7 | 1.3 | 1.9× |
| **Total** | **14.0** | **62.4** | **4.5×** |

The CPU timings match an earlier CPU run with the stock config (agg 23.9, join 4.3, sort 19.5,
window 11.5, filter 1.3), so removing Iceberg did not change the baseline.

Caveats: single timed runs; 3.4 GB over three 64 GB nodes is likely in the page cache, so this is
compute more than I/O; `filter` is mostly fixed overhead.

**Other configurations of the same benchmark** (26.10.0 unless noted, GPU seconds, warm run):

| Config | agg | join | sort | window | filter | Total | CPU total |
|---|---:|---:|---:|---:|---:|---:|---:|
| Iceberg-free, `noop` (above) | 6.1 | 1.4 | 3.9 | 1.9 | 0.7 | 14.0 | 62.4 |
| Stock + `suppressPlanningFailure=true`, `noop` | 10.0 | 1.3 | 4.5 | 1.8 | 0.7 | 18.3 | – |
| Stock, `SINK=parquet` | 12.2 | 1.6 | 19.4 | 4.2 | 0.8 | 38.2 | 83.9 (2.2×) |
| Stock, `SINK=parquet`, earlier run | 7.6 | 1.6 | 38.7 | 2.4 | 0.8 | 51.1 | – |
| Stock, `SINK=parquet`, 26.02.0 | 7.1 | 1.6 | 10.5 | 2.3 | 0.8 | 22.3 | – |

The Parquet runs include HDFS writes: `sort` writes all 300M rows. GPU `sort` ranged from 10.5 to
38.7 s across runs, so read the Parquet rows as "fully on the GPU, roughly 2× faster", not as a
26.02-vs-26.10 comparison.

**Iceberg and Hive tables** (`base/iceberg_check.sh`, 26.10.0, stock config). The Iceberg table is 30M
rows CTAS'd from the facts (`k < 100000`). The checksum is (row count, sum of `k`) through a two-level
aggregation, compared with the same rows read from Parquet: `[30000000,1499985000000]`.

| Step | `suppressPlanningFailure=true` | `false` |
|---|---|---|
| Iceberg CTAS | OK, no `Gpu*` operators (no shuffle, whole plan fell back) | `Unrecognized Iceberg version` |
| `DESCRIBE` → Provider | `iceberg` | (no table) |
| Iceberg read + agg | checksum matches; `BatchScan` stage on the CPU → `GpuRowToColumnar` → `GpuHashAggregate` | (no table) |
| Iceberg append (1000 rows) | OK; `AppendData` on the CPU, the limit query under it on the GPU; count 30001000 | (no table) |
| Hive `saveAsTable` (Provider `parquet`) | `GpuCreateDataSourceTableAsSelectCommand` | same |
| Hive read + agg | checksum matches, all on the GPU | same |
| Hive `insertInto` (1000 rows) | `GpuInsertIntoHadoopFsRelationCommand`; count 30001000 | same |

Seven planning failures were suppressed in the `true` run, all `Unrecognized Iceberg version`.

## 6. Compared with CDE on 7.1.9

| | **Base 7.1.9 SP1 (this)** | **CDE on 7.1.9 SP2 CHF4** ([`README-719.md`](README-719.md)) |
|---|---|---|
| Spark build | `3.5.4.3.5.7191000.0-30` (SPARK3 parcel) | `3.5.4.127.354.7191000.0-30` (CDE runtime image) |
| How the jar gets there | `--jars` per job | baked into a custom runtime image |
| RAPIDS | 26.10.0 (26.02.0 fails V2 writes, §3.5) | 26.02.0 |
| Shim override | needed | needed |
| Kryo registrator | needed | needed |
| Shuffle manager | Spark's own | `RapidsShuffleManager` |
| Iceberg | V2 operations fail unless `suppressPlanningFailure=true` or the config has no Iceberg | not hit: the smoke test did no V2 write |
| `MapPartitionsRDD` ctor | 7 args, inferred (see below) | 7 args, checked with `javap` |
| Evidence | 5-query benchmark, all `Gpu*`, 4.5× | smoke test, 8 `Gpu*` operators |

**The ctor was not checked with `javap` on Base.** It is inferred to be the upstream 7-argument one, because every GPU
plan here ran, while on 7.3.2 every GPU plan dies at its first action with `NoSuchMethodError`.
`base/check_cluster.sh` now prints it.

The Iceberg difference is not Base vs CDE. It comes from what was tested. On Base, both 26.10.0 and
26.02.0 fail V2 writes with the stock config, and the CDE smoke test never did one. Whether the CDE
runtime image carries the Iceberg config was not checked. If it does, a V2 write on CDE should hit the
26.02.0 `IllegalAccessError` (§3.5).

## 7. Reproducing in the lab

The `base/` scripts run on the gateway host as a kinit'd user. In the lab they were piped over SSH
from the deployment repo with its `on-host.sh` helper, which jumps through the bastion, runs
`kinit admin` with the password from the gitignored `config.yml` over stdin, and runs the script:

```bash
# from the deployment repo checkout (pvc-719sp1-rapids branch)
bash on-host.sh <gateway-ip> - < <this-repo>/base/check_cluster.sh
bash on-host.sh <gateway-ip> - < <this-repo>/base/plugin_load.sh
bash on-host.sh <gateway-ip> - < <this-repo>/base/benchmark.sh    # ~10 min
bash on-host.sh <gateway-ip> - < <this-repo>/base/iceberg_check.sh
bash on-host.sh <gateway-ip> - < <this-repo>/base/summary.sh
bash on-host.sh <gateway-ip> - < <this-repo>/base/diagnose.sh
```

The scripts here are generalized from the ones on that branch (`rapids/01`–`05`): the jar version
and work directory are parameters, the lab IPs and helper are gone, and `check_cluster.sh` adds the
`javap` ctor check. Logs stay on the host in `$WORK` (default `/tmp/rapids`): `as-is.log`,
`shim-override.log`, `bench-gpu.log`, `bench-cpu.log`, `iceberg-suppress-{true,false}.log`.

The variant runs used env prefixes concatenated in front of the script, since `on-host.sh` sends the
script on stdin. For example, a GPU-only run with the stock config and the Parquet sink:

```bash
( echo 'export STOCK_CONF=1 RUNS=gpu SINK=parquet WORK=/tmp/rapids-exp/v1-2610'
  echo 'mkdir -p $WORK && ln -sf /tmp/rapids/rapids-4-spark_2.12-26.10.0-cuda12.jar /tmp/rapids/getGpusResources.sh $WORK/'
  cat <this-repo>/base/benchmark.sh ) | bash on-host.sh <gateway-ip> -
```

## 8. Still open

- **`MapPartitionsRDD` ctor on Base**, by `javap` (§6).
- **PySpark**, cluster deploy mode (`spark-submit`, where the shim loader is Spark's child loader
  rather than the REPL's), Hive ACID tables, `RapidsShuffleManager`, larger scale and spill.
- **A V2 write on CDE 7.1.9 with 26.02.0**, to confirm §3.5 there.
- **Upstream RAPIDS issues**: the 26.10 Iceberg probe should treat an unknown version as "no Iceberg"
  (§3.4); 26.02's `GpuSparkWrite` fails across class loaders (§3.5, possibly fixed by the 26.10
  rewrite).
- **Productionising the config**: the settings, `suppressPlanningFailure=true` included, could go
  into a CM Spark 3 gateway role group on a dedicated GPU submit host. The stock Iceberg settings can
  then stay. That is simpler than a private `SPARK_CONF_DIR`, which has to be rebuilt whenever CM
  redeploys the client config.
