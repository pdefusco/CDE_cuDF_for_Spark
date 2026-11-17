# GPU-accelerated Spark on Cloudera Base: Runtime 7.3.2, Spark 3 on YARN

← [Overview and other versions](README.md)

This guide shows how to run Spark SQL and DataFrame jobs on NVIDIA GPUs on a **CDP Private Cloud
Base 7.3.2** cluster, with Spark 3 on YARN and no Data Services. It uses the open-source [NVIDIA
RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/) 26.10.0 from Maven
Central. Before you use the jar, you **patch** it on the gateway host with one script (§5).

The only cluster change is turning on GPU scheduling in YARN. Nothing is installed in the parcel.
Each job that wants the GPU brings the patched jar with `--jars` and adds a few settings. Every other
job runs as before.

> **Support status.** This is a tested reference configuration, not a Cloudera-supported one. Test
> it with your own workloads before you rely on it.

**In short:** this is the [7.1.9 SP1 Base setup](README-719-base.md) plus one step. Runtime 7.3.2's
Spark 3 changes the constructor of an internal Spark class, `MapPartitionsRDD`. The published RAPIDS
jar calls the old constructor, so every GPU query fails on its first action. The fix is
[`base/patch_rapids_jar.sh`](base/patch_rapids_jar.sh). It recompiles two RAPIDS classes, from
NVIDIA's own source and unchanged, against the cluster's Spark, and changes nothing else in the jar.
Everything else carries over from 7.1.9: three settings (§7) and one decision about Iceberg (§6).
With the patched jar, five benchmark queries ran entirely on the GPU, 4.2× faster than the CPU.

## 1. Tested versions

| Component | Version |
|---|---|
| Cloudera Manager | 7.13.2 (`7.13.2.6`) |
| Cloudera Runtime | 7.3.2 (`CDH-7.3.2-1.cdh7.3.2.p0.77083870`) |
| Spark 3 | inside the CDH parcel, at `/opt/cloudera/parcels/CDH/lib/spark3`. There is no separate SPARK3 parcel on 7.3.x |
| Spark | `spark.version` = `3.5.4.7.3.2.0-957`, Scala 2.12.19, OpenJDK 17.0.21 |
| Iceberg in the Spark 3 config | 1.5.2 (`iceberg-spark-runtime-3.5_2.12-1.5.2.7.3.2.0-957.jar`) |
| NVIDIA RAPIDS Accelerator | `rapids-4-spark_2.12-26.10.0-cuda12.jar`, **patched** with `base/patch_rapids_jar.sh` (§5), `spark354` shim |
| GPU hosts | 3 × AWS `g5.4xlarge` (16 vCPU, 64 GiB), 1 × NVIDIA A10G (24 GB) each, driver 620.70.06, RHEL 9.6, x86-64 |
| YARN | Kerberized; each NodeManager has 48 GB, 14 vcores and `yarn.io/gpu` = 1 |

Each RAPIDS release is built for specific Spark versions, and whether the jar needs the patch
depends on your exact Spark build. On a different Runtime, run
[`base/check_cluster.sh`](base/check_cluster.sh) first, and see [Other versions](#11-other-versions).

## 2. Prerequisites

These are the same as on 7.1.9: [`README-719-base.md` §2](README-719-base.md#2-prerequisites) has
the details. In brief:

- **YARN GPU scheduling**, following Cloudera's [GPU scheduling and
  isolation](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/yarn-allocate-resources/topics/yarn-configuring-gpu-scheduling-and-isolation.html)
  guide. The 7.3.x documentation still says GPU isolation doesn't support cgroup v2, so on RHEL 9
  every NodeManager host must still boot with cgroup v1 (`stat -fc %T /sys/fs/cgroup` prints
  `tmpfs`). The Cloudera Manager settings are the same six as on 7.1.9.
- **An NVIDIA driver that supports CUDA 12** on every GPU host, with `nvidia-smi` at
  `/usr/bin/nvidia-smi`. You don't need the CUDA toolkit.
- **The submitting user** needs a Kerberos ticket on the gateway host and an HDFS home directory,
  `/user/<name>`.
- **The gateway host** needs the Spark 3 gateway role, which on 7.3.2 still provides `spark3-shell`,
  `spark3-submit` and `/etc/spark3/conf`. It also needs a JDK with `javap` and `jar` (the Runtime's
  JDK 17 has both), `python3`, and outbound HTTPS to Maven Central and `raw.githubusercontent.com`
  for the patch step. Without that access, see §5.

## 3. Check the cluster

```bash
base/check_cluster.sh
```

Run it on the gateway host as the user with the Kerberos ticket. It checks that each GPU NodeManager
shows `yarn.io/gpu: 1`, and prints the Spark build, the key jar versions, the Iceberg settings in the
Spark config and the `MapPartitionsRDD` constructor. On 7.3.2, that constructor has an extra
`scala.Option` argument:

```
public org.apache.spark.rdd.MapPartitionsRDD(org.apache.spark.rdd.RDD<T>, scala.Function3<...>,
    boolean, boolean, boolean, scala.Option<java.lang.Object>, scala.reflect.ClassTag<U>, scala.reflect.ClassTag<T>);
```

That `scala.Option` is why §5 is needed. Without it, as on 7.1.9's CDS 3.5.4, the published jar
works as it is.

## 4. Download the jar and the GPU discovery script

```bash
base/plugin_load.sh
```

The script downloads `rapids-4-spark_2.12-26.10.0-cuda12.jar` and writes `getGpusResources.sh` to
`/tmp/rapids` (the commands are in [`README-719-base.md` §4](README-719-base.md#4-download-the-jar-and-the-gpu-discovery-script)).
It then starts RAPIDS twice. On 7.3.2 with the **published** jar, both attempts fail, as expected:

- **As-is:** `IllegalArgumentException: This RAPIDS Plugin build does not support Spark build
  3.5.4.7.3.2.0-957`. This is the version string problem, which the shim override fixes (§7).
- **With the shim override:** RAPIDS loads (`RAPIDS Accelerator build: version 26.10.0`), and then
  the first GPU query fails:

  ```
  java.lang.NoSuchMethodError: 'void org.apache.spark.rdd.MapPartitionsRDD.<init>(org.apache.spark.rdd.RDD,
      scala.Function3, boolean, boolean, boolean, scala.reflect.ClassTag, scala.reflect.ClassTag)'
  ```

## 5. Patch the jar

```bash
base/patch_rapids_jar.sh
```

The script runs on the gateway host. It:

1. downloads the two affected classes' Scala source from NVIDIA's GitHub, at the jar's own tag
   (`v26.10.0`);
2. compiles them against this host's Spark jars, with the Scala compiler that ships in the parcel
   (2.12.19 on 7.3.2);
3. checks that each recompiled class calls the 8-argument constructor, keeps its own constructor
   signature and keeps the jar's class-file version;
4. writes a copy of the jar to `/tmp/rapids-patched/`, under the published file name, and checks
   that exactly two entries changed, both under `spark354/`.

Expected output, ending with:

```
== Check the recompiled classes
8-arg call, major 52, constructor (Lorg/apache/spark/rdd/RDD;Lscala/Function3;ZZZLscala/reflect/ClassTag;Lscala/reflect/ClassTag;)V  org.apache.spark.rapids.LocationPreservingMapPartitionsRDD
8-arg call, major 52, constructor (Lorg/apache/spark/rdd/RDD;Lscala/Function3;ZZZ)V  org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD
== Write /tmp/rapids-patched/rapids-4-spark_2.12-26.10.0-cuda12.jar
spark354/org/apache/spark/rapids/LocationPreservingMapPartitionsRDD.class
spark354/org/apache/spark/sql/rapids/execution/GpuColumnToRowMapPartitionsRDD.class
23231 entries, 2 changed
950092521 /tmp/rapids-patched/rapids-4-spark_2.12-26.10.0-cuda12.jar
```

Then check that the patched jar works, by re-running the plugin test against it:

```bash
WORK=/tmp/rapids-patched base/plugin_load.sh
```

The shim-override run should now print `SMOKE wall=…` and no `NoSuchMethodError`.

**Why recompiling is enough.** The new constructor parameter has a default value. Spark *source*
that calls the old constructor still compiles, and the compiler fills in the default, but the
compiled *bytecode* names the exact constructor. So NVIDIA's unchanged source, compiled against this
Spark, calls the right constructor. No RAPIDS logic changes. The same patch has been used on CDE 7.3.2
since the first experiment; [`runtime/README-patch.md`](runtime/README-patch.md) has the background
and provenance.

**Run it once per cluster, and again after a Spark or RAPIDS upgrade.** The patched jar is tied to
this Spark build. Keep it somewhere shared, such as HDFS, if more than one gateway host submits RAPIDS
jobs. Don't copy it to a cluster whose Spark build differs; run the script there instead. If a new
build no longer has the extra parameter, the script stops at step 3 with `does not call the 8-arg
MapPartitionsRDD constructor`. That means the published jar works there as it is.

**No internet access on the gateway?** Put the two source files and, if the parcel lacks them,
`scala-compiler` and `scala-reflect` at the parcel's Scala version, where the script expects them, or
run it on a host that has access and the same Spark build.

**The patched jar is not NVIDIA's artifact.** It has the published file name, so that commands and
settings stay the same, but it isn't byte-identical to the Maven Central jar. Don't redistribute it
as NVIDIA's.

## 6. Choose how RAPIDS sessions handle Iceberg

This is unchanged from 7.1.9 except for the Iceberg version: [`README-719-base.md`
§5](README-719-base.md#5-choose-how-rapids-sessions-handle-iceberg) explains it in full.

The 7.3.2 Spark 3 config turns on Iceberg 1.5.2 in every session: the Iceberg SQL extensions, Iceberg's
`SparkSessionCatalog` as `spark_catalog`, and the Iceberg jars through `classpath.txt`. RAPIDS 26.10
supports only Iceberg 1.6 and later, and doesn't recognise Cloudera's build. So it fails every read or
write through Spark's DataSource V2 API (Iceberg tables, `writeTo(...)`, the `noop` sink), whether or
not the query touches an Iceberg table:

```
java.lang.UnsupportedOperationException: Unrecognized Iceberg version: 763d3402e1b8efc76b76d6f3f488b50e2f10f9d9
```

Parquet, ORC and CSV files through `spark.read`/`df.write`, and Hive tables through the metastore,
are not affected. Choose one of:

- **6a, recommended:** keep the stock config and set `spark.rapids.sql.suppressPlanningFailure=true`.
  The Iceberg parts of a query run on the CPU; the rest stays on the GPU. Check the final plan (§8),
  because this setting hides every GPU planning failure, not just the Iceberg one.
- **6b:** give RAPIDS jobs a copy of the Spark config with Iceberg removed, through `SPARK_CONF_DIR`.
  The recipe in the 7.1.9 guide works unchanged on 7.3.2: `/etc/spark3/conf` is in the same place,
  and the filter removes the 1.5.2 jars too. [`base/benchmark.sh`](base/benchmark.sh) builds the copy
  the same way.

## 7. Submit a job on the GPU

This is the 7.1.9 command with the patched jar. It uses option 6a. For option 6b, export
`SPARK_CONF_DIR` first and leave out the `suppressPlanningFailure` line.

```bash
spark3-submit --master yarn --deploy-mode client \
  --jars /tmp/rapids-patched/rapids-4-spark_2.12-26.10.0-cuda12.jar \
  --files /tmp/rapids/getGpusResources.sh \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.shims-provider-override=com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider \
  --conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator \
  --conf spark.rapids.sql.suppressPlanningFailure=true \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.executor.resource.gpu.discoveryScript=./getGpusResources.sh \
  --conf spark.task.resource.gpu.amount=0.08 \
  --conf spark.rapids.sql.concurrentGpuTasks=2 \
  --conf spark.executor.instances=3 --conf spark.executor.cores=12 \
  --conf spark.executor.memory=16g --conf spark.executor.memoryOverhead=10g \
  --conf spark.rapids.memory.pinnedPool.size=2g \
  your_job.py
```

What each setting does is in [`README-719-base.md` §6](README-719-base.md#6-submit-a-job-on-the-gpu).
On 7.3.2 the shim override is needed for the same reason, a version string (`3.5.4.7.3.2.0-957`)
that RAPIDS doesn't know, and the Spark 3 config again selects the Kryo serializer, so the registrator
is needed too. The executor sizes fit the tested hosts.

## 8. Verify that the job ran on the GPU

The job status doesn't tell you: RAPIDS runs what it can't handle on the CPU without an error, and
`explain()` before a query runs shows CPU operators even for a query that will run on the GPU. Check
the final plan after the query finishes, in the Spark UI's **SQL / DataFrame** tab or with a
`QueryExecutionListener`. It should be made of `Gpu*` operators. [`README-719-base.md`
§7](README-719-base.md#7-verify-that-the-job-ran-on-the-gpu) has an example.
[`base/benchmark.sh`](base/benchmark.sh) checks this for every query.

## 9. Results

**Benchmark** ([`base/benchmark.sh`](base/benchmark.sh), patched jar, Iceberg-free config (6b),
`noop` sink): 300M rows of synthetic Parquet on HDFS (3.4 GB) and a 1M-row dimension table. Each
query runs once to warm up and once more for the timing, with the same executors (3 × 12 cores) for
the GPU and CPU runs. The CPU run didn't load RAPIDS.

| Query | GPU (s) | CPU (s) | Speedup | 7.1.9 SP1 Base, for comparison |
|---|---:|---:|---:|---|
| agg: `groupBy(k)`, sum/avg/count/countDistinct | 7.2 | 27.1 | 3.8× | 6.1 / 24.8 s, 4.1× |
| join: broadcast join with dims, then groupBy | 1.6 | 4.6 | 2.9× | 1.4 / 4.3 s, 3.1× |
| sort: global `orderBy(desc v, k)` | 4.4 | 20.8 | 4.7× | 3.9 / 19.8 s, 5.1× |
| window: `row_number` per `k`, top 3 | 2.1 | 13.8 | 6.6× | 1.9 / 12.2 s, 6.4× |
| filter: IN + range filter, `groupBy(year(d))` | 0.9 | 1.3 | 1.4× | 0.7 / 1.3 s, 1.9× |
| **Total** | **16.2** | **67.6** | **4.2×** | 14.0 / 62.4 s, 4.5× |

Every query's final plan was made of `Gpu*` operators, and RAPIDS reported no CPU fallback. The two
clusters have the same hardware. 7.3.2 was slower on both sides, by 16% on the GPU total and 8% on the
CPU total, which doesn't load RAPIDS; so not all of the difference is the patch, and one timed run per
query can't say more. As on 7.1.9, the data probably sat in the page cache.

The GPU `agg` time is from a second GPU-only run (`RUNS=gpu`). In the first run, the same session had
just generated the data, and `agg` took 25.4 s.

**Iceberg and Hive tables** ([`base/iceberg_check.sh`](base/iceberg_check.sh), patched jar, stock
config, 30M rows). Each table's checksum, `[30000000,1499985000000]`, matched the same rows read
straight from Parquet:

| Step | `suppressPlanningFailure=true` | default (`false`) |
|---|---|---|
| Create an Iceberg table from a query | works, entirely on the CPU | fails: `Unrecognized Iceberg version` |
| Read the Iceberg table and aggregate | scan on the CPU, aggregation on the GPU; checksum matches | (no table to read) |
| Append to the Iceberg table | the write on the CPU, the query feeding it on the GPU; count 30001000 | (no table to append to) |
| Hive table: `saveAsTable`, read and aggregate, `insertInto` | all on the GPU; checksum matches | all on the GPU; checksum matches |

These are the same results as on 7.1.9, with Iceberg 1.5.2 in place of 1.3.

To reproduce all of this, after §2:

```bash
base/check_cluster.sh                          # YARN GPUs, Spark build, MapPartitionsRDD constructor
base/plugin_load.sh                            # published jar: rejected as-is, NoSuchMethodError with the override
base/patch_rapids_jar.sh                       # writes /tmp/rapids-patched/<same jar name>
WORK=/tmp/rapids-patched base/plugin_load.sh   # patched jar: smoke test passes
WORK=/tmp/rapids-patched base/benchmark.sh     # ~10 min at 300M rows
WORK=/tmp/rapids-patched base/iceberg_check.sh
WORK=/tmp/rapids-patched base/linkage_scan.sh  # optional: every Spark symbol the jar uses, resolved
```

`patch_rapids_jar.sh` takes `RAPIDS_VERSION` (default `26.10.0`), `WORK` (where the published jar
is, default `/tmp/rapids`) and `OUT` (default `/tmp/rapids-patched`). The other scripts take
`RAPIDS_VERSION` and `WORK`, as in the 7.1.9 guide.

## 10. Troubleshooting

The [7.1.9 table](README-719-base.md#9-troubleshooting) applies, with 7.3.2's version strings. Two
rows are specific to 7.3.2:

| Symptom | Cause | Fix |
|---|---|---|
| `NoSuchMethodError: 'void org.apache.spark.rdd.MapPartitionsRDD.<init>(…, boolean, boolean, boolean, scala.reflect.ClassTag, scala.reflect.ClassTag)'` on the first GPU query | The published, unpatched jar | Patch it (§5), and pass `/tmp/rapids-patched/…` with `--jars` |
| `patch_rapids_jar.sh` stops with `does not call the 8-arg MapPartitionsRDD constructor` | This Spark build has Apache's 7-argument constructor | Use the published jar as it is ([`README-719-base.md`](README-719-base.md)) |
| `UnsupportedOperationException: Unrecognized Iceberg version: 763d3402…` | A DataSource V2 read or write with the stock config, which loads Iceberg 1.5.2 | `suppressPlanningFailure=true` (§6a), or the Iceberg-free config (§6b) |

## 11. Other versions

- **Runtime 7.1.9 SP1 Base**, with CDS 3.5.4: [`README-719-base.md`](README-719-base.md). The same
  setup, without the patch.
- **CDE on Runtime 7.3.2**: [`README-732.md`](README-732.md). The same patch, built into a custom CDE
  runtime image instead of run on a gateway host.
- **CDE on Runtime 7.1.9**: [`README-719.md`](README-719.md).

Whether a Spark build needs the patch depends on the build, not on the Runtime version. To check
every Spark build on a cluster, run [`scripts/check_spark_core.sh`](scripts/check_spark_core.sh)
from your workstation (it needs a local JDK):

```bash
SSH_OPTS="-q -F <your-ssh-config>" scripts/check_spark_core.sh ssh://<any-cluster-host>
```

## 12. Known limitations

- **Iceberg runs on the CPU at best.** Iceberg 1.5.2 is older than any version RAPIDS 26.10 can
  accelerate (1.6 and later). With `suppressPlanningFailure=true` the Iceberg parts of a query run on
  the CPU; without it, they fail.
- **The patch fixes the one linkage error that was seen.** A scan of every Spark method and class the
  jar uses, resolved against this cluster's Spark jars, found four other mismatches on code paths that
  are reachable in principle. None is on a default path: two are in DataSource V2 file scans, which
  Spark doesn't use for Parquet, ORC or CSV unless `spark.sql.sources.useV1SourceList` is changed, and
  two are in push-based shuffle (`spark.shuffle.push.enabled`, off by default). Details in
  [`docs/linkage-scan.md`](docs/linkage-scan.md#runtime-732-base-spark-3-on-yarn).
- **The ColumnarRdd export path wasn't tested.** It is the path that hands GPU data to other libraries
  (for example XGBoost), and one of the two patched classes is used there.
- **NVIDIA hasn't tested RAPIDS with the shim override**, as on 7.1.9.
- **Only RAPIDS 26.10.0 was tried on 7.3.2.** 26.02.0 fails DataSource V2 writes on 7.1.9 Base, and
  wasn't tried here.
- **Not tested here:** PySpark and UDFs, cluster deploy mode, Hive ACID tables, larger data sets that
  spill, `RapidsShuffleManager`, the `cuda13` jar, and anything other than x86-64.
