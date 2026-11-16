# GPU-accelerated Spark on Cloudera Base: Runtime 7.1.9 SP1, Spark 3 on YARN

← [Overview and other versions](README.md)

This guide runs Spark SQL and DataFrame jobs on NVIDIA GPUs on a **CDP Private Cloud Base** cluster,
with Spark 3 (CDS 3.5.4) on YARN and no Data Services. It uses the open-source [NVIDIA RAPIDS
Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/), unmodified, from Maven Central.
Nothing is installed on the cluster beyond YARN GPU scheduling: each job ships the jar with `--jars`
and opts in with a handful of settings, and jobs that don't are unaffected.

> **Support status.** This is a tested reference configuration, not a Cloudera-supported one.
> Cloudera documents RAPIDS support only for CDS 3.3. Validate it against your own workloads before
> relying on it.

**In short:** no Spark patch is needed, but the plugin does not run as-is. It needs three settings
(§6), plus one choice about Iceberg (§5). CDS 3.5.4 enables Iceberg 1.3 in every session, and RAPIDS
fails every DataSource V2 operation when it sees that Iceberg. Iceberg tables and the `noop` sink are
both DataSource V2. Either let those queries fall back to the CPU (`suppressPlanningFailure=true`,
recommended), or run RAPIDS jobs with a client config that has no Iceberg. Parquet files and Hive
tables run on the GPU either way.

## 1. Tested versions

| Component | Version |
|---|---|
| Cloudera Manager | 7.13.2 SP2 |
| Cloudera Runtime | 7.1.9 SP1 (`7.1.9-1.cdh7.1.9.p1069.74330271`) |
| Spark 3 parcel | `SPARK3-3.5.4.3.5.7191000.0-30-1.p0.68499982` |
| Spark | `spark.version` = `3.5.4.3.5.7191000.0-30`, Scala 2.12.18, OpenJDK 17.0.21 |
| NVIDIA RAPIDS Accelerator | `rapids-4-spark_2.12-26.10.0-cuda12.jar`, unmodified, `spark354` shim (26.02.0 partly, see §11) |
| GPU hosts | 3 × AWS `g5.4xlarge`, 1 × NVIDIA A10G (24 GB) each, driver 620.70.06, RHEL 9.6, x86-64 |
| YARN | Kerberized; per NodeManager 48 GB, 14 vcores, `yarn.io/gpu` = 1 |

The RAPIDS jar version and its shim are locked to the Spark build. On another Runtime or CDS version,
run [`base/check_cluster.sh`](base/check_cluster.sh) first; see [Other versions](#10-other-versions).

## 2. Prerequisites

**YARN GPU scheduling.** Follow Cloudera's [GPU scheduling and
isolation](https://docs.cloudera.com/runtime/7.1.9/yarn-allocate-resources/topics/yarn-configuring-gpu-scheduling-and-isolation.html)
guide. On RHEL 9 that means:

- **cgroup v1 on every NodeManager host.** RHEL 9 boots cgroup v2, which YARN GPU isolation does not
  support. Switch with `grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=0"` and
  reboot; `stat -fc %T /sys/fs/cgroup` must then print `tmpfs`.
- **An NVIDIA driver that supports CUDA 12** on every GPU host, with `nvidia-smi` at `/usr/bin/nvidia-smi`.
  No CUDA toolkit is needed: the `cuda12` jar bundles cuDF and the CUDA runtime.
- In Cloudera Manager:

  | Where | Setting | Value |
  |---|---|---|
  | Hosts → each NodeManager host | Enable Cgroup-based Resource Management (`rm_enabled`) | on |
  | YARN service | Use CGroups for Resource Management (`yarn_service_cgroups`) | on |
  | YARN service | Always Use Linux Container Executor (`yarn_service_lce_always`) | on |
  | NodeManager role group | Enable GPU Usage (`gpu_enabled`) | on |
  | NodeManager role group | NodeManager GPU Devices Allowed (`gpu_plugin_allowed_devices`) | `auto` |
  | NodeManager role group | NodeManager GPU Detection Executable (`gpu_plugin_detector_path`) | `/usr/bin/nvidia-smi` |

  Size the NodeManager memory and vcores, and the scheduler's maximum allocation, so that one
  executor can take a whole node. Then restart YARN.

**User**

- A Kerberos ticket on the gateway host (`kinit`).
- An **HDFS home directory**, `/user/<name>`, owned by the user. Spark on YARN stages each job there.
  This is generic Spark-on-YARN setup; an HDFS superuser creates it once:
  `hdfs dfs -mkdir -p /user/<name> && hdfs dfs -chown <name>:<name> /user/<name>`.

**Gateway host**

- A host with the Spark 3 gateway role (`spark3-shell`, `spark3-submit`, `/etc/spark3/conf`).
- Outbound HTTPS to Maven Central, or a copy of the jar.
- This repository's `base/` scripts. They run on the gateway host as the kinit'd user.

## 3. Check that YARN advertises GPUs

```bash
yarn node -list -showDetails | grep -E "Node-Id|yarn.io/gpu"
```

Expect `yarn.io/gpu: 1` (or more) on each GPU NodeManager. If it is missing, revisit §2.
[`base/check_cluster.sh`](base/check_cluster.sh) prints this along with the Spark build, the key jar
versions and the Iceberg settings in the client config.

## 4. Download the jar and the GPU discovery script

```bash
mkdir -p /tmp/rapids && cd /tmp/rapids
curl -fL --retry 3 -O \
  https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0-cuda12.jar
cat > getGpusResources.sh <<'EOF'
#!/usr/bin/env bash
ADDRS=$(nvidia-smi --query-gpu=index --format=csv,noheader | sed -e ':a' -e 'N' -e'$!ba' -e 's/\n/","/g')
echo {\"name\": \"gpu\", \"addresses\":[\"$ADDRS\"]}
EOF
chmod +x getGpusResources.sh
```

YARN allocates the GPU to the container; Spark still needs the discovery script to learn its address.
[`base/plugin_load.sh`](base/plugin_load.sh) does both of these steps, then shows the plugin being
rejected as-is and loading with the shim override.

## 5. Choose how RAPIDS sessions handle Iceberg

CDS 3.5.4's client config (`/etc/spark3/conf`) enables Iceberg 1.3 for every session: it sets the
Iceberg SQL extensions, makes Iceberg's `SparkSessionCatalog` the `spark_catalog`, and puts the
Iceberg jars on the classpath through `classpath.txt`. RAPIDS 26.10 supports only Iceberg 1.6 and
later. With the stock config, it fails **every** DataSource V2 read or write, whether or not an
Iceberg table is involved:

```
java.lang.UnsupportedOperationException: Unrecognized Iceberg version: d38e7d2d800a1c64b01edc974cf1eabefdc62231
```

DataSource V1 operations are unaffected. These include Parquet, ORC and CSV paths through
`spark.read` / `df.write`, and Hive tables through the metastore (`spark.table`, `saveAsTable`,
`insertInto`). DataSource V2 covers Iceberg tables, `writeTo(...)` and the `noop` sink.
`spark.rapids.sql.format.iceberg.enabled=false` does not prevent the failure. Pick one of the two
options below.

### 5a. Keep the stock config and let Iceberg fall back to the CPU (recommended)

```
--conf spark.rapids.sql.suppressPlanningFailure=true
```

RAPIDS then logs `WARN GpuOverrideUtil: Failed to apply GPU overrides, falling back on the original
plan` for the part of the plan that touches Iceberg, and runs it on the CPU. Under adaptive query
execution (on by default), each query stage is planned separately. Only the stages that read or
write Iceberg stay on the CPU. Joins, aggregations and sorts after an Iceberg scan still run on the
GPU. Tested on Iceberg 1.3 tables: create (CTAS), read and append all returned exact results.

Two costs:
- A query that touches Iceberg and has no shuffle runs entirely on the CPU (for example, a plain CTAS).
- The setting hides any other GPU planning failure in the same way, and the query silently runs on
  the CPU. Check the final plan (§7).

### 5b. Give RAPIDS jobs a client config without Iceberg

Use this when RAPIDS jobs don't need Iceberg and you want planning failures to stay visible. Create a
private copy of the config with Iceberg removed:

```bash
conf=/tmp/rapids/conf-noiceberg
rm -rf $conf && mkdir -p $conf && cp -rL /etc/spark3/conf/. $conf/
sed -i -E '/^spark\.sql\.extensions=|^spark\.sql\.catalog\.spark_catalog|^spark\.[A-Za-z.]*iceberg/Id' $conf/spark-defaults.conf
for f in $(grep -rliE iceberg $conf); do
  sed -i -E "s#[^:,\"= ]*iceberg[^:,\"' ]*[:,]?##Ig" "$f"
done
[ -f $conf/classpath.txt ] && sed -i '/^\s*$/d' $conf/classpath.txt
grep -rciE iceberg $conf | grep -v ":0$" || echo "no Iceberg left"
export SPARK_CONF_DIR=$conf
```

Only the jobs that export `SPARK_CONF_DIR` see this config, and they cannot read Iceberg tables at
all. Everyone else keeps Iceberg. Cloudera Manager regenerates `/etc/spark3/conf` when the client
config is redeployed, so rebuild the copy after any Spark 3 configuration change.
[`base/benchmark.sh`](base/benchmark.sh) builds it the same way.

## 6. Submit a job on the GPU

With the stock config and option 5a; for option 5b, export `SPARK_CONF_DIR` instead and drop the
`suppressPlanningFailure` line:

```bash
spark3-submit --master yarn --deploy-mode client \
  --jars /tmp/rapids/rapids-4-spark_2.12-26.10.0-cuda12.jar \
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

| Setting | Value | Why |
|---|---|---|
| `spark.plugins` | `com.nvidia.spark.SQLPlugin` | loads RAPIDS |
| `spark.rapids.sql.enabled` | `true` | |
| `spark.rapids.shims-provider-override` | `com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider` | the plugin rejects Cloudera's version string `3.5.4.3.5.7191000.0-30`; this forces the Apache 3.5.4 shim |
| `spark.kryo.registrator` | `com.nvidia.spark.rapids.GpuKryoRegistrator` | the CDS client config selects Kryo, and RAPIDS refuses to run under Kryo without its registrator. If a registrator is already set, append this one, comma-separated |
| `spark.rapids.sql.suppressPlanningFailure` | `true` | option 5a: Iceberg (DataSource V2) stages fall back to the CPU instead of failing the query |
| `spark.executor.resource.gpu.amount` | `1` | one GPU per executor; YARN schedules it as `yarn.io/gpu` |
| `spark.executor.resource.gpu.discoveryScript` | `./getGpusResources.sh` | shipped with `--files` |
| `spark.task.resource.gpu.amount` | `1 / executor cores`, e.g. `0.08` for 12 cores | lets all the executor's tasks share the GPU |
| `spark.rapids.sql.concurrentGpuTasks` | `2` | tasks that may use the GPU at once; tune per workload |
| `spark.executor.memoryOverhead` | generous, e.g. `10g` | RAPIDS host memory is off-heap |
| `spark.rapids.memory.pinnedPool.size` | e.g. `2g` | pinned host memory for transfers; counts against the overhead |

The executor sizing matches the tested hosts: one 12-core executor per 14-vcore, 48 GB NodeManager.
Scale it to yours. While tuning, add `--conf spark.rapids.sql.explain=NOT_ON_GPU`: RAPIDS then logs
every operator it left on the CPU, with the reason. For more, see
[`docs/rapids-spark-conf.md`](docs/rapids-spark-conf.md).

The tested runs used Spark's own shuffle. `RapidsShuffleManager` was not tried on Base.

## 7. Verify that the job ran on the GPU

RAPIDS falls back to the CPU silently, so a successful job proves nothing by itself. Under adaptive
query execution (on by default), **`explain()` before the query runs shows the CPU plan** even when
the query will run on the GPU. Check the plan after the query finishes instead:

- In the Spark UI or History Server, open the **SQL / DataFrame** tab and the query's details. The
  final plan should be made of `Gpu*` operators.
- Or register a `QueryExecutionListener` and print `qe.executedPlan` in `onSuccess`. It contains
  `isFinalPlan=true` and the operators that actually ran. [`base/benchmark.sh`](base/benchmark.sh)
  does this for every query.

The final plan of the benchmark's window query, with every operator on the GPU:

```
GpuColumnarToRow
+- GpuNoopOverwriteByExpression
   +- AdaptiveSparkPlan isFinalPlan=true
      +- GpuCoalesceBatches
         +- GpuFilter (r <= 3)
            +- GpuRunningWindow [gpurownumber$() ...]
               +- GpuSort
                  +- GpuShuffleCoalesce
                     +- GpuCustomShuffleReader coalesced
                        +- ShuffleQueryStage 0
                           +- GpuColumnarExchange gpuhashpartitioning(k, 200)
                              +- GpuWindowGroupLimit ... 3, Partial
                                 +- GpuSort
                                    +- GpuFileGpuScan parquet
```

## 8. Benchmark results

[`base/benchmark.sh`](base/benchmark.sh) generates 300M rows of synthetic Parquet on HDFS (3.4 GB)
and a 1M-row dimension table, then runs five queries on the GPU and on the CPU. Each query runs once
to warm up and once timed. Both runs used the same executors (3 × 12 cores),
`spark.sql.shuffle.partitions=200` and `maxPartitionBytes=512m`. The CPU run had 22g heap + 4g
overhead and no plugin.

Each query's result is written to the `noop` sink, which measures the query and not the write. These
runs used the Iceberg-free config (§5b):

| Query | GPU (s) | CPU (s) | Speedup |
|---|---:|---:|---:|
| agg: `groupBy(k)`, sum/avg/count/countDistinct | 6.1 | 24.8 | 4.1× |
| join: broadcast join with dims, then groupBy | 1.4 | 4.3 | 3.1× |
| sort: global `orderBy(desc v, k)` | 3.9 | 19.8 | 5.1× |
| window: `row_number` per `k`, top 3 | 1.9 | 12.2 | 6.4× |
| filter: IN + range filter, `groupBy(year(d))` | 0.7 | 1.3 | 1.9× |
| **Total** | **14.0** | **62.4** | **4.5×** |

`NOT_ON_GPU` logged no fallback for any query. Each number is a single timed run on a small data set
that probably sits in the OS page cache, so it measures compute more than I/O. `filter` is too short
to say much about.

With option 5a instead (stock config, `suppressPlanningFailure=true`), only the `noop` write node fell
back. Every query stage stayed on the GPU: agg 10.0, join 1.3, sort 4.5, window 1.8 and filter 0.7 s.

**Writing Parquet instead** (`SINK=parquet`, stock config, no extra settings): every operator ran on
the GPU, including `GpuInsertIntoHadoopFsRelationCommand`. The sort query writes all 300M rows.

| Query | GPU (s) | CPU (s) | Speedup |
|---|---:|---:|---:|
| agg | 12.2 | 25.1 | 2.1× |
| join | 1.6 | 4.4 | 2.8× |
| sort | 19.4 | 39.5 | 2.0× |
| window | 4.2 | 13.5 | 3.2× |
| filter | 0.8 | 1.4 | 1.7× |
| **Total** | **38.2** | **83.9** | **2.2×** |

These timings include HDFS writes and vary from run to run. In three GPU runs, `sort` took 10.5, 19.4
and 38.7 s.

**Iceberg and Hive tables** ([`base/iceberg_check.sh`](base/iceberg_check.sh), stock config, 30M rows):

| Step | `suppressPlanningFailure=true` | default (`false`) |
|---|---|---|
| Iceberg CTAS (`writeTo(...).using("iceberg").create()`) | runs, entirely on the CPU | fails: `Unrecognized Iceberg version` |
| Iceberg read + two-level aggregation | scan stage on the CPU, aggregation on the GPU; exact checksum | (no table) |
| Iceberg append (`writeTo(...).append()`) | write on the CPU, the query feeding it on the GPU | (no table) |
| Hive table: `saveAsTable`, read + aggregation, `insertInto` | all on the GPU; exact checksum | all on the GPU; exact checksum |

To reproduce on your cluster, after §2 and §3:

```bash
base/plugin_load.sh   # downloads the jar; as-is load (rejected) vs. shim override
base/benchmark.sh     # Iceberg-free config, GPU and CPU runs, final plans (~10 min at 300M rows)
base/iceberg_check.sh # Iceberg and Hive tables with the stock config, suppressPlanningFailure on and off
base/summary.sh       # re-reads the logs into the table above
base/diagnose.sh      # if the GPU run fails: config, Iceberg jars, stack traces, log tails
```

All four take `RAPIDS_VERSION` (default `26.10.0`) and `WORK` (default `/tmp/rapids`);
`benchmark.sh` also takes `ROWS`, `RUNS`, `SINK=parquet`, `STOCK_CONF=1` and `GPU_CONF` (see its
header). Its executor sizing assumes the tested hosts; edit the `common`,
`gpu` and `cpu` arrays for yours.

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `IllegalArgumentException: This RAPIDS Plugin build does not support Spark build 3.5.4.3.5.7191000.0-30` | No shim override | Set `spark.rapids.shims-provider-override` (§6) |
| `The RAPIDS Accelerator when used with Kryo needs to register some serializers` | No Kryo registrator | Set `spark.kryo.registrator` (§6) |
| `UnsupportedOperationException: Unrecognized Iceberg version: d38e7d2d…` in `GpuOverrideUtil` | A DataSource V2 operation with the stock client config, which has Iceberg | Set `suppressPlanningFailure=true` (§5a) or use the Iceberg-free config (§5b) |
| `WARN GpuOverrideUtil: Failed to apply GPU overrides, falling back on the original plan` | Expected with option 5a: that part of the plan touches Iceberg | Nothing, if it names `Unrecognized Iceberg version`; anything else is a real planning failure to investigate |
| `IllegalAccessError: failed to access class org.apache.iceberg.spark.source.SparkWrite from class org.apache.iceberg.spark.source.GpuSparkWrite$` | RAPIDS 26.02.0 with the stock config, on any DataSource V2 write | Use 26.10.0, or the Iceberg-free config (§5b). `suppressPlanningFailure` does not catch it |
| Submission fails writing `/user/<name>/.sparkStaging` | No HDFS home directory | Create `/user/<name>` (§2) |
| The application stays `ACCEPTED`, or executors never start | Not enough `yarn.io/gpu`, or no GPUs advertised | Check §3; request no more GPU executors than there are GPUs |
| `explain()` shows only CPU operators | The plan before AQE re-plans it | Check the final plan after the query (§7) |
| The final plan has CPU operators | An unsupported operator or type | Add `spark.rapids.sql.explain=NOT_ON_GPU` and read the reasons in the driver log |

## 10. Other versions

- **Cloudera Data Engineering (CDE) on Runtime 7.1.9**: [`README-719.md`](README-719.md). Same Spark
  3.5.4 source line and the same shim override; the jar ships in a custom runtime image instead of
  `--jars`.
- **Runtime 7.3.2**: [`README-732.md`](README-732.md). Cloudera's 7.3.2 Spark build changes a
  `MapPartitionsRDD` constructor, and the published jar has to be patched before it can link.

The published jar links against Apache's 7-argument `MapPartitionsRDD` constructor.
[`base/check_cluster.sh`](base/check_cluster.sh) prints the one in your `spark-core` jar (it needs a
JDK for `javap`). If it ends `boolean, boolean, boolean, scala.reflect.ClassTag<U>,
scala.reflect.ClassTag<T>)`, this guide applies; if it has an extra `scala.Option` argument, see
[`README-732.md`](README-732.md).

## 11. Known limitations

- **Iceberg runs on the CPU at best.** RAPIDS 26.10 accelerates only Iceberg 1.6 and later. CDS
  3.5.4 ships 1.3, so with `suppressPlanningFailure=true` (§5a) the stages that read or write Iceberg
  run on the CPU; without it, they fail. Accelerating them needs RAPIDS support for Iceberg 1.3, not
  a change in Spark.
- **RAPIDS 26.02.0 (the version used on CDE) is partly tested.** With the stock config, it ran the
  Parquet-writing benchmark fully on the GPU. Every DataSource V2 write (Iceberg, `noop`) fails with
  `IllegalAccessError`, and no setting tried avoids it, including the jar on `extraClassPath` and
  `suppressPlanningFailure`. Use 26.10.0.
- **The shim override is an untested combination for NVIDIA.** The driver logs `Overriding Spark
  shims provider ... This may be an untested configuration!`. Five query shapes ran fully on the GPU
  with no errors; that is the extent of the evidence.
- **Not tested here:** PySpark and UDFs, cluster deploy mode, Hive ACID tables, larger data sets and
  spill, `RapidsShuffleManager`, and the `cuda13` jar.
- **x86-64 only.** The tested jar classifier is `cuda12` on `amd64`.
