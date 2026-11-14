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
`NoSuchMethodError`, `ClassCastException` or CPU fallback. It needs four settings, and one of them
is a real limitation, because the plugin cannot share a session with the Iceberg 1.3 that CDS 3.5.4
configures by default.

| Question | Answer |
|---|---|
| Runs as-is? | No. The ShimLoader rejects the Cloudera version string. |
| Runs with config overrides? | Yes, fully on the GPU, with no fallbacks and no errors. |
| Needs a Spark patch? | No. No binary incompatibilities with Cloudera's backports were seen. |
| Needs a RAPIDS patch? | Only to use RAPIDS and Iceberg in the same session (§3.4). |

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

Two harness problems also cost a run each, and both are fixed in the scripts:

- Spark's console progress bar ends lines with `\r`, so `println` output did not start at column 0
  and `^BENCH` greps found nothing. The scripts set `spark.ui.showConsoleProgress=false`, and
  `summary.sh` splits on `\r` for older logs.
- In `spark3-shell -i`, a line that starts with `.` is parsed as a new statement. Method chains must
  break after the dot, not before it.

## 3. The four requirements

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

### 3.4 No Iceberg on the driver classpath

The CDS 3.5.4 client config enables Iceberg for every session:

```
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.iceberg.enabled=true
spark.hadoop.iceberg.engine.hive.enabled=true
```

`classpath.txt` also adds `SPARK3/lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.3.0.3.5.7191000.0-30.jar`
and the iceberg-hive runtime. With that config, any DSv2 read or write fails the query:

```
ERROR rapids.GpuOverrideUtil: Encountered an exception applying GPU overrides
java.lang.UnsupportedOperationException: Unrecognized Iceberg version: d38e7d2d800a1c64b01edc974cf1eabefdc62231
  at com.nvidia.spark.rapids.iceberg.IcebergProbeImpl.shimPackage(IcebergProbeImpl.scala:89)
  at com.nvidia.spark.rapids.iceberg.IcebergProbeImpl.getProvider(IcebergProbeImpl.scala:100)
  at com.nvidia.spark.rapids.iceberg.IcebergProvider$.apply(IcebergProvider.scala:58)
  at org.apache.spark.sql.rapids.ExternalSourceBase.icebergProvider(ExternalSource.scala:68)
  at org.apache.spark.sql.rapids.ExternalSourceBase$IcebergV2WriteRecognizer$.recognizes(ExternalSource.scala:110)
```

V1 Parquet reads and writes are unaffected, which is why the smoke test passed.

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
- **Failure, not fallback.** `spark.rapids.sql.suppressPlanningFailure` defaults to `false`, so the
  exception is rethrown. With `true`, the whole query would run on the CPU instead. Not tried.

**Workaround:** a private `SPARK_CONF_DIR`, copied from `/etc/spark3/conf` with the Iceberg keys
deleted and the Iceberg entries filtered out of `classpath.txt` (top of `base/benchmark.sh`). The
benchmark verifies it in-session: its `PROBE` line prints `icebergScan=not-loadable`. The cost is
that a RAPIDS session cannot read Iceberg tables. `spark_catalog` reverts to Spark's own session
catalog, so Hive tables should still resolve, but that was not tested.

**What a RAPIDS fix would look like** (upstream, NVIDIA/spark-rapids):

- make `ExternalSource.hasIcebergJar` honour `spark.rapids.sql.format.iceberg.enabled`, or
- treat an unknown or unsupported Iceberg version as "no Iceberg": log a warning and leave those
  scans on the CPU, instead of throwing.

**26.02.0 is different.** The CDE 7.1.9 work used RAPIDS 26.02.0. That release has no
`IcebergProbeImpl` and no V2 write recogniser for Iceberg: `isSupportedWrite` covers Delta only.
`getScans` still calls `icebergProvider.getScans` when the Iceberg jar is present, but the provider
is a single implementation from `ShimLoaderTemp.newIcebergProvider()`, with no version probe. So
26.02.0 may well run with the stock config, at least for non-Iceberg queries. **Untested on Base.**
`RAPIDS_VERSION=26.02.0 base/plugin_load.sh` followed by a benchmark run against the stock config
would settle it. `benchmark.sh` always builds the Iceberg-free config, so that run needs the
`SPARK_CONF_DIR` export commented out.

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

## 6. Compared with CDE on 7.1.9

| | **Base 7.1.9 SP1 (this)** | **CDE on 7.1.9 SP2 CHF4** ([`README-719.md`](README-719.md)) |
|---|---|---|
| Spark build | `3.5.4.3.5.7191000.0-30` (SPARK3 parcel) | `3.5.4.127.354.7191000.0-30` (CDE runtime image) |
| How the jar gets there | `--jars` per job | baked into a custom runtime image |
| RAPIDS | 26.10.0 | 26.02.0 |
| Shim override | needed | needed |
| Kryo registrator | needed | needed |
| Shuffle manager | Spark's own | `RapidsShuffleManager` |
| Iceberg | must be removed from the session (26.10 probe) | not hit (26.02 has no probe) |
| `MapPartitionsRDD` ctor | 7 args, inferred (see below) | 7 args, checked with `javap` |
| Evidence | 5-query benchmark, all `Gpu*`, 4.5× | smoke test, 8 `Gpu*` operators |

**The ctor was not checked with `javap` on Base.** It is inferred to be the upstream 7-argument one, because every GPU
plan here ran, while on 7.3.2 every GPU plan dies at its first action with `NoSuchMethodError`.
`base/check_cluster.sh` now prints it.

The Iceberg difference is a RAPIDS-version difference, not a Base-vs-CDE one, as far as the code
shows. Whether the CDE runtime image also carries the Iceberg config was not checked.

## 7. Reproducing in the lab

The `base/` scripts run on the gateway host as a kinit'd user. In the lab they were piped over SSH
from the deployment repo with its `on-host.sh` helper, which jumps through the bastion, runs
`kinit admin` with the password from the gitignored `config.yml` over stdin, and runs the script:

```bash
# from the deployment repo checkout (pvc-719sp1-rapids branch)
bash on-host.sh <gateway-ip> - < <this-repo>/base/check_cluster.sh
bash on-host.sh <gateway-ip> - < <this-repo>/base/plugin_load.sh
bash on-host.sh <gateway-ip> - < <this-repo>/base/benchmark.sh    # ~10 min
bash on-host.sh <gateway-ip> - < <this-repo>/base/summary.sh
bash on-host.sh <gateway-ip> - < <this-repo>/base/diagnose.sh
```

The scripts here are generalized from the ones on that branch (`rapids/01`–`05`): the jar version
and work directory are parameters, the lab IPs and helper are gone, and `check_cluster.sh` adds the
`javap` ctor check. Logs stay on the host in `$WORK` (default `/tmp/rapids`): `as-is.log`,
`shim-override.log`, `bench-gpu.log`, `bench-cpu.log`.

## 8. Still open

- **`MapPartitionsRDD` ctor on Base**, by `javap` (§6).
- **RAPIDS 26.02.0 with the stock config** (§3.4). If it runs, Base and CDE could share one jar
  version, and the Iceberg-free config would only be needed on 26.10+.
- **`suppressPlanningFailure=true` with the stock config**: would Iceberg-reading sessions at least
  run, on the CPU?
- **Hive tables through the metastore**, PySpark, cluster deploy mode, `RapidsShuffleManager`,
  larger scale and spill.
- **An upstream RAPIDS issue** for the Iceberg probe (§3.4).
- **Productionising the config**: the four settings could go into a CM Spark 3 gateway role group on
  a dedicated GPU submit host. CM generates `classpath.txt` and the catalog settings for every
  gateway, though, and removing Iceberg cluster-wide breaks it for every non-GPU user, so a private
  `SPARK_CONF_DIR` per RAPIDS job or per host is the least invasive option until RAPIDS is fixed.
