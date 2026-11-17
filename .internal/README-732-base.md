# Spark RAPIDS on Cloudera Base, Runtime 7.3.2 / Spark 3 on YARN

← [Overview](../README.md) · Customer-facing version: [`README-732-base.md`](../README-732-base.md) · Base on 7.1.9: [`README-719-base.md`](README-719-base.md) · CDE on 7.3.2: [`../README-732.md`](../README-732.md)

> **Internal engineering write-up.** How the 7.3.2 Base result was reached, and what differs from
> 7.1.9 Base. Everything not repeated here (the shim override, the Kryo registrator, the HDFS home
> directory, the Iceberg probe, proving GPU execution) works as described in
> [`README-719-base.md`](README-719-base.md).

The question: **does 7.3.2's Spark 3, which on 7.3.x ships inside the CDH parcel, have the 8-argument
`MapPartitionsRDD` constructor that forced the patch on CDE 7.3.2?** The constructor is a per-build
property: CDS 3.5.4 on 7.1.9 has 7 arguments, while the Spark 3.3.2 in the 7.1.9 CDH parcel has 8
([`README-719-base.md` §6](README-719-base.md#6-compared-with-cde-on-719)). So the answer couldn't be
read off the Runtime version.

**Outcome: yes, it has 8, and the CDE patch works on Base unchanged, applied on the gateway instead of
in an image.** The published `rapids-4-spark_2.12-26.10.0-cuda12` jar fails the first GPU query with
`NoSuchMethodError` on `MapPartitionsRDD.<init>`. With two classes recompiled from NVIDIA's v26.10.0
source against the cluster's Spark ([`base/patch_rapids_jar.sh`](../base/patch_rapids_jar.sh)), the
same five benchmark queries ran entirely on the GPU, 4.2× faster than the CPU (4.5× on 7.1.9).
Everything else is as on 7.1.9: the same three settings, and Iceberg (now 1.5.2) still unsupported.

| Question | Answer |
|---|---|
| Runs as-is? | No. The ShimLoader rejects `3.5.4.7.3.2.0-957`. |
| Runs with config overrides? | No. RAPIDS loads, then every GPU query fails with `NoSuchMethodError` (§2). |
| Needs a Spark patch? | No. |
| Needs a RAPIDS patch? | Yes: 2 classes recompiled from NVIDIA's source, nothing else changed (§3). A whole-jar linkage scan finds nothing else reachable by default (§4). |
| Iceberg? | 1.5.2, still below RAPIDS 26.10's 1.6 minimum. Same behaviour and workarounds as 7.1.9 (§5). |

Internal technical prototype, not a supported configuration.

## 1. Environment

| | |
|---|---|
| Cluster | `spark-base-cluster` (lab name `ds1116`), AWS us-east-2, November 2026 |
| Services | ZooKeeper, HDFS, Hive Metastore, YARN, Spark 3 on YARN; Kerberos via FreeIPA |
| Runtime | CDH `7.3.2-1.cdh7.3.2.p0.77083870`, CM 7.13.2 (`7.13.2.6`), RHEL 9.6 |
| Spark | in the CDH parcel, `lib/spark3`; `spark.version` = `3.5.4.7.3.2.0-957` (built 2026-03-27, revision `ded2b044`); Scala 2.12.19; OpenJDK 17.0.21 |
| Notable jars | hadoop-client 3.4.2.7.3.2.0-957, hive-exec 3.1.3000.7.3.2.0-957, arrow-vector 15.0.0.7.3.2.0-957, Iceberg 1.5.2 (`lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.5.2.7.3.2.0-957.jar`) |
| Plugin | `com.nvidia:rapids-4-spark_2.12:26.10.0`, classifier `cuda12`, patched (§3) |
| Masters | 3 × `t3a.xlarge`; master-03 is the Spark 3 gateway the jobs were submitted from |
| Workers | 3 × `g5.4xlarge`: 1× NVIDIA A10G (24 GB), driver 620.70.06, cgroup v1 |
| YARN | LinuxContainerExecutor + cgroups; per NodeManager 49152 MB, 14 vcores, `yarn.io/gpu` = 1 |

The cluster was built with the `cloudera-ce-ecs-aws` deployment automation, on a `pvc-732-rapids`
branch (local, not pushed) cut from the 7.1.9 SP1 one. The changes were small: the 7.3.2 and CM
7.13.2.6 pins from the working ds0928 deployment, no SPARK3 parcel or repo (Spark 3 is in the CDH
parcel on 7.3.x), and the `rapids/` scripts taught to find Spark 3 in either parcel. The cgroup v1
boot, the NVIDIA driver role and the CM YARN GPU settings were unchanged: the 7.3.x GPU scheduling
docs still exclude cgroup v2. The `SPARK3_ON_YARN` service type and the Hive-dependency lookup from
the CM schema worked as they were. The only deploy failure was a transient RHUI mirror lookup in
`services.yml`, which passed on re-run.

The parcel's `meta/parcel.json` has a component's version (`3.4.2.7.3.2.0-957`) as its first
`"version"` line, which an early `check_cluster.sh` printed as the parcel version. The script now
prints the parcel directory name.

## 2. Timeline

Each run was submitted from the gateway with `spark3-shell -i` in client mode. On 7.3.2 the CDH parcel
still provides `spark3-shell`/`spark3-submit` and `/etc/spark3/conf`. The `base/` scripts were
generalized first, to find Spark 3 in either parcel.

1. **`check_cluster.sh`.** Three NodeManagers with `yarn.io/gpu: 1`. `javap` on the parcel's
   `spark-core` shows the 8-argument constructor:
   ```
   public org.apache.spark.rdd.MapPartitionsRDD(org.apache.spark.rdd.RDD<T>, scala.Function3<...>,
       boolean, boolean, boolean, scala.Option<java.lang.Object>, scala.reflect.ClassTag<U>, scala.reflect.ClassTag<T>);
   ```
   The `scala.Option<java.lang.Object>` is `isDeterministic: Option[Boolean]` after erasure, as on CDE
   7.3.2.
2. **As-is** (`plugin_load.sh`, `as-is.log`): `IllegalArgumentException: This RAPIDS Plugin build does
   not support Spark build 3.5.4.7.3.2.0-957`, as expected.
3. **Shim override** (`shim-override.log`). Past the ShimLoader and the Kryo check, the first action
   fails:
   ```
   java.lang.NoSuchMethodError: 'void org.apache.spark.rdd.MapPartitionsRDD.<init>(org.apache.spark.rdd.RDD,
       scala.Function3, boolean, boolean, boolean, scala.reflect.ClassTag, scala.reflect.ClassTag)'
   ```
   This is the CDE 7.3.2 failure, on a different distribution channel of the same Spark line.
4. **`patch_rapids_jar.sh`** (§3). Two classes recompiled, every check passed, 2 of 23,231 entries
   changed. The patched jar is 950,092,521 bytes.
5. **Plugin test with the patched jar** (`WORK=/tmp/rapids-patched`). The shim-override run passes:
   `SMOKE wall=15.6s`, and 15.4 s on a re-run, with no fallbacks.
6. **Benchmark**, patched jar, Iceberg-free config. All five queries fully on the GPU (§6).
7. **`iceberg_check.sh`**, patched jar, stock config, with `suppressPlanningFailure` true and false
   (§5).
8. **Linkage scan** (`linkage_scan.sh`), stock and patched jars (§4).

## 3. The patch

[`base/patch_rapids_jar.sh`](../base/patch_rapids_jar.sh) is the patcher stage of
[`runtime/Dockerfile`](../runtime/Dockerfile), ported to run on the gateway. The two classes are the
only ones in `spark354/` that construct a `MapPartitionsRDD`:

| Class | Source at `v26.10.0` | Constructor (unchanged) |
|---|---|---|
| `org.apache.spark.rapids.LocationPreservingMapPartitionsRDD` | its own file | `(RDD, Function3, ZZZ, ClassTag, ClassTag)` |
| `org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD` | cut out of `InternalColumnarRddConverter.scala` with `sed`, with a 4-import header (that file pulls in most of the plugin) | `(RDD, Function3, ZZZ)` |

They are compiled with the parcel's own `scala-compiler`/`scala-reflect` 2.12.19 (7.3.2 ships both
in `lib/spark3/jars`; the script falls back to Maven Central), against every jar in `lib/spark3/jars`,
with `-target:jvm-1.8`. The script then asserts, per class:

- the bytecode calls `MapPartitionsRDD.<init>` with `ZZZLscala/Option;`. On a 7-argument Spark,
  scalac would emit the 7-argument call again and the patch would silently do nothing;
- its own constructor descriptor is unchanged, because the published companion objects call it;
- its class-file major version (52) is unchanged.

It writes the classes into a copy of the jar with `jar uf`, and compares CRCs of every non-directory
entry: exactly the two `spark354/` entries may differ.

**Differences from the CDE Dockerfile.** The Dockerfile works from the 26.02.0 source in
`runtime/patch/`; the Base script fetches the source at the jar's own tag. Both now leave
`spark-shared/` alone:

- Neither companion calls `MapPartitionsRDD`, so neither needs recompiling.
- `GpuColumnToRowMapPartitionsRDD$` in the published jar carries `mapPartitions(...)`.
  `GpuColumnarToRowExec.doExecute` calls it when `exportColumnarRdd` is set, i.e. on the ColumnarRdd
  export path (XGBoost and similar). The standalone file compiled here doesn't define it, so the
  companion scalac produces from it lacks that method.
- **Found here, fixed in the Dockerfile afterwards.** The Dockerfile used to overwrite both
  companions (4 entries), which dropped `mapPartitions(...)`: a latent `NoSuchMethodError` on the
  ColumnarRdd path in CDE 7.3.2. No CDE test used that path, so nobody hit it. The Dockerfile now
  writes only the two `spark354/` classes and asserts the companion still has `mapPartitions(...)`.
  The `26.02.0-p1` image was built before the fix and still has the bug. `26.02.0-p2` is built
  from the fixed Dockerfile, and inside it both companions have the CRCs of NVIDIA's jar.

## 4. Linkage scan

[`base/linkage_scan.sh`](../base/linkage_scan.sh) is the hand scan of
[`docs/linkage-scan.md`](../docs/linkage-scan.md) (done against CDE 1.26) turned into a script, and
run here against the stock and the patched jar. It parses every class that a 3.5.4 session loads
(`spark354/`, then `spark-shared/`, then the unprefixed roots) and resolves every `org/apache/spark`
member and class reference against `lib/spark3/jars` plus `classpath.txt`, walking superclasses and
interfaces. A miss doesn't count as live if no caller is reachable from `spark354/` or the roots, or
if every live caller is Delta, Iceberg or Databricks code.

Results for the patched jar:

| | Total | Delta/Iceberg/Databricks only | Unreachable only | To triage |
|---|---:|---:|---:|---:|
| Missing members | 248 | 98 | 146 | 4 |
| Missing classes | 243 | 233 | 10 | 0 |
| Unverified (inherited from a JDK class) | 14 | – | 1 | 13 |

In scope: 6,459 of the jar's 22,400 classes, 5,867 of them reachable. External Spark member
references: 4,836 unique, 3,700 of them resolve. The stock jar has one more missing member, the
7-argument `MapPartitionsRDD.<init>`, and the stock-vs-patched diff contains only that.

The four to triage:

- **`FileScan` × 2.** The same Cloudera trait-forwarder difference as on CDE 1.26 (linkage-scan.md
  rows 1–2). Only the DataSource V2 file scan uses it, and `spark.sql.sources.useV1SourceList` keeps
  Parquet, ORC and CSV on V1 by default.
- **Push-based shuffle × 2**, `MapOutputTracker.getMapSizesForMergeResult` and
  `MergedBlockMeta.readChunkBitmaps` (linkage-scan.md rows 5–6). RAPIDS's descriptors have
  `com/nvidia/shaded/spark/org/roaringbitmap/RoaringBitmap`, while Spark declares
  `org/roaringbitmap/RoaringBitmap`. RAPIDS shaded a type that appears in Spark's own signatures, so
  these miss on Apache Spark too, and 26.02.0 has the same references. Reached only with
  `spark.shuffle.push.enabled=true` (off by default).

The 13 unverified are standard JDK methods reached through Spark classes (`Throwable.getCause`,
`AutoCloseable.close`, `Future.isCancelled`, `CompletableFuture.complete`, `ClassLoader.loadClass`,
`Random.nextDouble` and similar), so they're fine.

**A correction to the CDE 1.26 triage.** linkage-scan.md row 4, the Hive-serde CTAS
`getWritingCommand` mismatch, was marked "real and reachable". It isn't reachable on 3.5.x. Only the
`spark330`–`spark334` `HiveProviderCmdShims` register `CreateHiveTableAsSelectCommand`. From
`spark340` on, including `spark354`, they register only `InsertIntoHiveTable`, and
`GpuCreateHiveTableAsSelectCommandMeta` is referenced by nothing but itself. The script's
reachability filter drops it for that reason. The same holds in the 26.02.0 jar.

## 5. Iceberg

The 7.3.2 Spark 3 client config has the same Iceberg settings as CDS 3.5.4 on 7.1.9 (extensions,
`SparkSessionCatalog`, `spark.iceberg.enabled`, `spark.hadoop.iceberg.engine.hive.enabled`), with
Iceberg 1.5.2 through `classpath.txt`. The probe fails the same way, with a different commit:

```
java.lang.UnsupportedOperationException: Unrecognized Iceberg version: 763d3402e1b8efc76b76d6f3f488b50e2f10f9d9
```

1.5.2 has no RAPIDS 26.10 shim either (1.6, 1.9, 1.10 and 1.11 do), so even correctly detected it would
not be accelerated. `iceberg_check.sh` results, patched jar, stock config, 30M rows, checksum
`[30000000,1499985000000]`:

| Step | `suppressPlanningFailure=true` | `false` |
|---|---|---|
| Iceberg CTAS | OK, no `Gpu*` operators | `Unrecognized Iceberg version` |
| `DESCRIBE` → Provider | `iceberg` | `TABLE_OR_VIEW_NOT_FOUND` |
| Iceberg read + agg | checksum matches; `BatchScan` stage on the CPU → `GpuRowToColumnar` → `GpuHashAggregate` | `TABLE_OR_VIEW_NOT_FOUND` |
| Iceberg append (1000 rows) | OK; `AppendData` on the CPU, the limit query under it on the GPU; count 30001000 | `TABLE_OR_VIEW_NOT_FOUND` |
| Hive `saveAsTable` (Provider `parquet`) | `GpuCreateDataSourceTableAsSelectCommand` | `GpuCreateDataSourceTableAsSelectCommand` |
| Hive read + agg | checksum matches, all on the GPU | checksum matches, all on the GPU |
| Hive `insertInto` (1000 rows) | `GpuInsertIntoHadoopFsRelationCommand`; count 30001000 | `GpuInsertIntoHadoopFsRelationCommand`; count 30001000 |

Seven planning failures were suppressed in the `true` run, all `Unrecognized Iceberg version`. Step by
step, this is the 7.1.9 table.

## 6. Benchmark

Same data, queries and executor sizing as [7.1.9 §5](README-719-base.md#5-benchmark): 300M rows on
HDFS, `noop` sink, Iceberg-free `SPARK_CONF_DIR`, 3 × 12-core executors, one warm-up and one timed run.

| Query | GPU (s) | CPU (s) | Speedup | 7.1.9 SP1 Base GPU / CPU (s) |
|---|---:|---:|---:|---|
| agg | 7.2 | 27.1 | 3.8× | 6.1 / 24.8 |
| join | 1.6 | 4.6 | 2.9× | 1.4 / 4.3 |
| sort | 4.4 | 20.8 | 4.7× | 3.9 / 19.8 |
| window | 2.1 | 13.8 | 6.6× | 1.9 / 12.2 |
| filter | 0.9 | 1.3 | 1.4× | 0.7 / 1.3 |
| **Total** | **16.2** | **67.6** | **4.2×** | 14.0 / 62.4 (4.5×) |

All five final plans were `Gpu*` operators, with no `NOT_ON_GPU` reasons logged and no errors.

- **GPU `agg` comes from a second run** (`RUNS=gpu`, 7.2 s; `filter` 0.8 s in that run). In the
  first run, the data had just been generated in the same session, and `agg` took 25.4 s. The cause
  wasn't investigated.
- **7.3.2 is slower than 7.1.9 on the same hardware**: 16% on the GPU total, 8% on the CPU total. The
  CPU run doesn't load RAPIDS, so at least part of it comes from the cluster or the Spark build, not
  the patch, whose two classes are thin RDD wrappers. Single timed runs on two clusters can't say how
  the rest splits.

## 7. Compared with the other three

| | **Base 7.3.2 (this)** | **Base 7.1.9 SP1** | **CDE 7.3.2** ([`../README-732.md`](../README-732.md)) |
|---|---|---|---|
| Spark build | `3.5.4.7.3.2.0-957` (CDH parcel) | `3.5.4.3.5.7191000.0-30` (SPARK3 parcel) | `3.5.4.1.26.732.0-45` (CDE 1.26 runtime image) |
| `MapPartitionsRDD` ctor | 8 args | 7 args | 8 args |
| RAPIDS | 26.10.0, 2 classes recompiled on the gateway | 26.10.0 as published | 26.02.0, 2 classes recompiled in the runtime image (the published `p1` image also overwrote 2 companions, since fixed) |
| Shim override, Kryo registrator | needed | needed | set on every run |
| Iceberg | 1.5.2: `suppressPlanningFailure` or no Iceberg | 1.3: `suppressPlanningFailure` or no Iceberg | no Iceberg jars in the image |
| Evidence | 5-query benchmark, 4.2×; Iceberg/Hive check; linkage scan | 5-query benchmark, 4.5×; Iceberg/Hive check | smoke test |

So the patch belongs to the Spark build, not to CDE: whichever channel delivers a Spark with
`isDeterministic`, the same two-class recompile fixes it.

## 8. Reproducing in the lab

From the `pvc-732-rapids` deployment checkout, with the gateway at master-03, as on 7.1.9
([§7 there](README-719-base.md#7-reproducing-in-the-lab)):

```bash
bash on-host.sh <gateway-ip> - < <this-repo>/base/check_cluster.sh
bash on-host.sh <gateway-ip> - < <this-repo>/base/plugin_load.sh        # stock jar: expected to fail
bash on-host.sh <gateway-ip> - < <this-repo>/base/patch_rapids_jar.sh
( echo 'export WORK=/tmp/rapids-patched'; cat <this-repo>/base/plugin_load.sh ) | bash on-host.sh <gateway-ip> -
( echo 'export WORK=/tmp/rapids-patched'; cat <this-repo>/base/benchmark.sh ) | bash on-host.sh <gateway-ip> -
( echo 'export WORK=/tmp/rapids-patched'; cat <this-repo>/base/iceberg_check.sh ) | bash on-host.sh <gateway-ip> -
bash on-host.sh <gateway-ip> - < <this-repo>/base/linkage_scan.sh       # scans $WORK and $OUT
```

`patch_rapids_jar.sh` must run after `plugin_load.sh`, which downloads the jar and writes
`getGpusResources.sh`. With `WORK=/tmp/rapids-patched`, `plugin_load.sh` finds the patched jar under
the published name and doesn't download, and writes its own copy of the discovery script there.

## 9. Still open

- **The ColumnarRdd export path** with the patched jar (`ColumnarRdd`, XGBoost): it uses the
  recompiled `GpuColumnToRowMapPartitionsRDD` through the published companion. Untested.
- **`26.02.0-p2` on a CDE cluster.** The fixed Dockerfile (§3) is built and pushed as `p2`
  (`sha256:8180b205cdcd…`) and checked inside the image, but no CDE job has run on it yet. The CDE
  runs in the repo used `p1`.
- **Not run on 7.3.2:** stock config with `suppressPlanningFailure=true` on the `noop` sink, `SINK=parquet`,
  RAPIDS 26.02.0, `scripts/check_spark_core.sh` against the gateway (the `javap` in `check_cluster.sh`
  covered the one Spark build in the parcel).
- **The RoaringBitmap shading in push-based shuffle** (§4) is an upstream RAPIDS issue, not a
  Cloudera one. Worth reporting.
- The rest of [7.1.9's list](README-719-base.md#8-still-open): PySpark, cluster deploy mode, Hive
  ACID, `RapidsShuffleManager`, scale and spill, the upstream Iceberg probe fix.
