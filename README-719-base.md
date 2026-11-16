# GPU-accelerated Spark on Cloudera Base: Runtime 7.1.9 SP1, Spark 3 on YARN

← [Overview and other versions](README.md)

This guide shows how to run Spark SQL and DataFrame jobs on NVIDIA GPUs on a **CDP Private Cloud
Base** cluster, with Spark 3 (CDS 3.5.4) on YARN and no Data Services. It uses the open-source [NVIDIA
RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/), unmodified, straight
from Maven Central.

The only cluster change is turning on GPU scheduling in YARN. Nothing else is installed. Each job
that wants the GPU brings the RAPIDS jar with `--jars` and adds a few settings. Every other job runs
as before.

> **Support status.** This is a tested reference configuration, not a Cloudera-supported one.
> Cloudera documents RAPIDS support only for CDS 3.3. Test it with your own workloads before you rely
> on it.

**In short:** Spark doesn't need a patch, but RAPIDS doesn't work out of the box either. You need
three settings (§6) and one decision about Iceberg (§5). The decision comes from CDS 3.5.4 turning on
Iceberg 1.3 in every Spark session. RAPIDS doesn't support that Iceberg version, and when it sees it,
it fails any query that goes through Spark's DataSource V2 API, which includes all Iceberg tables. You
can let those queries run on the CPU (`suppressPlanningFailure=true`, recommended), or give RAPIDS
jobs a Spark config without Iceberg. Parquet files and Hive tables run on the GPU either way.

## 1. Tested versions

| Component | Version |
|---|---|
| Cloudera Manager | 7.13.2 SP2 |
| Cloudera Runtime | 7.1.9 SP1 + PATCH-1069 (`7.1.9-1.cdh7.1.9.p1069.74330271`) |
| Spark 3 parcel | `SPARK3-3.5.4.3.5.7191000.0-30-1.p0.68499982` |
| Spark | `spark.version` = `3.5.4.3.5.7191000.0-30`, Scala 2.12.18, OpenJDK 17.0.21 |
| NVIDIA RAPIDS Accelerator | `rapids-4-spark_2.12-26.10.0-cuda12.jar`, unmodified, `spark354` shim. 26.02.0 was also tried and only partly works (§11) |
| GPU hosts | 3 × AWS `g5.4xlarge` (16 vCPU, 64 GiB), 1 × NVIDIA A10G (24 GB) each, driver 620.70.06, RHEL 9.6, x86-64 |
| YARN | Kerberized; each NodeManager has 48 GB, 14 vcores and `yarn.io/gpu` = 1 |

Each RAPIDS release is built for specific Spark versions, so the jar has to match your Spark build.
On a different Runtime or CDS version, run [`base/check_cluster.sh`](base/check_cluster.sh) first, and
see [Other versions](#10-other-versions).

## 2. Prerequisites

**YARN GPU scheduling.** Follow Cloudera's [GPU scheduling and
isolation](https://docs.cloudera.com/runtime/7.1.9/yarn-allocate-resources/topics/yarn-configuring-gpu-scheduling-and-isolation.html)
guide. On RHEL 9, that comes down to the following:

- **Boot every NodeManager host with cgroup v1.** RHEL 9 uses cgroup v2 by default, and YARN's GPU
  isolation doesn't support v2. Switch with
  `grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=0"` and reboot. Afterwards,
  `stat -fc %T /sys/fs/cgroup` should print `tmpfs`.
- **Install an NVIDIA driver that supports CUDA 12** on every GPU host, with `nvidia-smi` at
  `/usr/bin/nvidia-smi`. You don't need the CUDA toolkit: the `cuda12` jar already contains cuDF and
  the CUDA runtime.
- **Change these settings in Cloudera Manager:**

  | Where | Setting | Value |
  |---|---|---|
  | Hosts → each NodeManager host | Enable Cgroup-based Resource Management (`rm_enabled`) | on |
  | YARN service | Use CGroups for Resource Management (`yarn_service_cgroups`) | on |
  | YARN service | Always Use Linux Container Executor (`yarn_service_lce_always`) | on |
  | NodeManager role group | Enable GPU Usage (`gpu_enabled`) | on |
  | NodeManager role group | NodeManager GPU Devices Allowed (`gpu_plugin_allowed_devices`) | `auto` |
  | NodeManager role group | NodeManager GPU Detection Executable (`gpu_plugin_detector_path`) | `/usr/bin/nvidia-smi` |

  Give each NodeManager enough memory and vcores, and raise the scheduler's maximum allocation, so
  that one executor can use a whole node. Then restart YARN.

**The user who submits jobs needs:**

- A Kerberos ticket on the gateway host (`kinit`).
- An **HDFS home directory**, `/user/<name>`, that they own. Spark on YARN uploads each job's files
  there. This isn't specific to RAPIDS; any Spark-on-YARN user needs one. An HDFS superuser creates
  it once: `hdfs dfs -mkdir -p /user/<name> && hdfs dfs -chown <name>:<name> /user/<name>`.

**The gateway host needs:**

- The Spark 3 gateway role, which provides `spark3-shell`, `spark3-submit` and `/etc/spark3/conf`.
- Outbound HTTPS to Maven Central, or a copy of the jar.
- A copy of this repository's `base/` scripts. Run them on the gateway host as the user with the
  Kerberos ticket.

## 3. Check that YARN advertises GPUs

```bash
yarn node -list -showDetails | grep -E "Node-Id|yarn.io/gpu"
```

Each GPU NodeManager should show `yarn.io/gpu: 1` (or more). If it doesn't, go back over §2.
[`base/check_cluster.sh`](base/check_cluster.sh) prints this too, together with the Spark build, the
versions of the key jars, and the Iceberg settings in the Spark config.

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

YARN gives the container its GPU, but Spark still needs the discovery script to find out which GPU
that is. [`base/plugin_load.sh`](base/plugin_load.sh) does both downloads for you. It then starts
RAPIDS twice: once as-is, where it is rejected, and once with the shim override from §6, where it
loads.

## 5. Choose how RAPIDS sessions handle Iceberg

CDS 3.5.4's Spark config (`/etc/spark3/conf`) turns on Iceberg 1.3 in every session. It adds the
Iceberg SQL extensions, makes Iceberg's `SparkSessionCatalog` the default catalog (`spark_catalog`),
and puts the Iceberg jars on the classpath through `classpath.txt`. RAPIDS 26.10 supports only
Iceberg 1.6 and later. When it finds the older Iceberg, it fails **every** read or write through
Spark's DataSource V2 API, whether or not the query touches an Iceberg table:

```
java.lang.UnsupportedOperationException: Unrecognized Iceberg version: d38e7d2d800a1c64b01edc974cf1eabefdc62231
```

What goes through which API:

- **DataSource V2, affected:** Iceberg tables, `df.writeTo(...)`, and the `noop` sink used for
  benchmarking.
- **DataSource V1, not affected:** Parquet, ORC and CSV files read and written with `spark.read` and
  `df.write`, and Hive tables through the metastore (`spark.table`, `saveAsTable`, `insertInto`).

Turning off RAPIDS's own Iceberg support (`spark.rapids.sql.format.iceberg.enabled=false`) doesn't
stop the failure. Choose one of the two options below.

### 5a. Keep the stock config and let Iceberg fall back to the CPU (recommended)

```
--conf spark.rapids.sql.suppressPlanningFailure=true
```

With this setting, RAPIDS no longer fails the query. It logs `WARN GpuOverrideUtil: Failed to apply
GPU overrides, falling back on the original plan` and runs the part that touches Iceberg on the CPU.
Spark's adaptive query execution (on by default) plans each stage of a query separately, so only the
stages that read or write Iceberg move to the CPU. Joins, aggregations and sorts after an Iceberg scan
still run on the GPU. On Iceberg 1.3 tables, creating a table from a query (CTAS), reading and
appending all gave exact results.

This has two costs:
- A query that touches Iceberg and has no shuffle has only one stage, so it runs entirely on the
  CPU. A plain CTAS is an example.
- The setting hides every GPU planning failure, not just the Iceberg one: the query quietly runs on
  the CPU instead. Check the final plan (§7) to see what really ran on the GPU.

### 5b. Give RAPIDS jobs a Spark config without Iceberg

Choose this if your RAPIDS jobs don't need Iceberg and you'd rather see planning failures than have
them hidden. Make a private copy of the config with Iceberg taken out:

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

Only jobs started with this `SPARK_CONF_DIR` use the copy, and they can't read Iceberg tables at
all. Everyone else still has Iceberg. Cloudera Manager rewrites `/etc/spark3/conf` whenever the client
configuration is redeployed, so rebuild the copy after any Spark 3 configuration change.
[`base/benchmark.sh`](base/benchmark.sh) builds the copy the same way.

## 6. Submit a job on the GPU

This command uses the stock config with option 5a. For option 5b, export `SPARK_CONF_DIR` first (§5b)
and leave out the `suppressPlanningFailure` line.

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
| `spark.rapids.sql.enabled` | `true` | turns on GPU execution of SQL and DataFrame queries |
| `spark.rapids.shims-provider-override` | `com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider` | RAPIDS doesn't recognise Cloudera's version string, `3.5.4.3.5.7191000.0-30`, and refuses to start. This tells it to use its Apache Spark 3.5.4 support |
| `spark.kryo.registrator` | `com.nvidia.spark.rapids.GpuKryoRegistrator` | the CDS config uses the Kryo serializer, and RAPIDS won't run under Kryo unless its classes are registered. If your jobs already set a registrator, add this one after a comma |
| `spark.rapids.sql.suppressPlanningFailure` | `true` | option 5a: the Iceberg (DataSource V2) parts of a query run on the CPU instead of failing it |
| `spark.executor.resource.gpu.amount` | `1` | one GPU per executor; YARN schedules it as `yarn.io/gpu` |
| `spark.executor.resource.gpu.discoveryScript` | `./getGpusResources.sh` | the script from §4, shipped with `--files` |
| `spark.task.resource.gpu.amount` | `1 / executor cores`, e.g. `0.08` for 12 cores | lets all of an executor's tasks share its GPU |
| `spark.rapids.sql.concurrentGpuTasks` | `2` | how many tasks can use the GPU at the same time; tune it per workload |
| `spark.executor.memoryOverhead` | generous, e.g. `10g` | RAPIDS uses a lot of memory outside the JVM heap |
| `spark.rapids.memory.pinnedPool.size` | e.g. `2g` | pinned host memory for copies to and from the GPU; it comes out of the overhead |

The executor sizes fit the tested hosts: one 12-core executor on each 14-vcore, 48 GB NodeManager.
Adjust them to your hosts. While tuning, add `--conf spark.rapids.sql.explain=NOT_ON_GPU`, and RAPIDS
will log every operator it left on the CPU, with the reason. More settings are in
[`docs/rapids-spark-conf.md`](docs/rapids-spark-conf.md).

These tests used Spark's built-in shuffle. RAPIDS's own shuffle manager (`RapidsShuffleManager`)
wasn't tried on Base.

## 7. Verify that the job ran on the GPU

A job that succeeds hasn't necessarily used the GPU: when RAPIDS can't handle part of a query, it
runs that part on the CPU without any error. There's a second trap. With adaptive query execution
(on by default), **`explain()` before the query runs shows CPU operators**, even for a query that
will run on the GPU. Look at the plan after the query has finished instead:

- In the Spark UI or History Server, open the **SQL / DataFrame** tab and click the query. The final
  plan should be made of `Gpu*` operators.
- Or register a `QueryExecutionListener` and print `qe.executedPlan` in `onSuccess`. That plan
  contains `isFinalPlan=true` and the operators that actually ran.
  [`base/benchmark.sh`](base/benchmark.sh) does this for every query.

Here is the final plan of the benchmark's window query, with every operator on the GPU:

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

[`base/benchmark.sh`](base/benchmark.sh) generates 300M rows of synthetic Parquet data on HDFS
(3.4 GB) and a 1M-row dimension table. It then runs five queries, once on the GPU and once on the
CPU. Each query runs once to warm up and once more for the timing. Both runs used the same executors
(3 × 12 cores), `spark.sql.shuffle.partitions=200` and `maxPartitionBytes=512m`. The CPU run didn't
load RAPIDS and had a 22g heap with 4g overhead.

**Main result.** Each query's result goes to the `noop` sink, which runs the query but writes
nothing, so the timing is the query alone. These runs used the Iceberg-free config (§5b):

| Query | GPU (s) | CPU (s) | Speedup |
|---|---:|---:|---:|
| agg: `groupBy(k)`, sum/avg/count/countDistinct | 6.1 | 24.8 | 4.1× |
| join: broadcast join with dims, then groupBy | 1.4 | 4.3 | 3.1× |
| sort: global `orderBy(desc v, k)` | 3.9 | 19.8 | 5.1× |
| window: `row_number` per `k`, top 3 | 1.9 | 12.2 | 6.4× |
| filter: IN + range filter, `groupBy(year(d))` | 0.7 | 1.3 | 1.9× |
| **Total** | **14.0** | **62.4** | **4.5×** |

RAPIDS reported no CPU fallback for any query. Keep in mind that each number comes from a single
timed run on a small data set, which probably sat in the OS page cache, so this measures computation
more than disk I/O. The `filter` query is too short to tell you much.

**With option 5a instead** (stock config, `suppressPlanningFailure=true`): only the `noop` write
itself fell back to the CPU, and every stage of every query stayed on the GPU. The GPU times were
agg 10.0, join 1.3, sort 4.5, window 1.8 and filter 0.7 s.

**Writing Parquet files instead** (`SINK=parquet`, stock config, no extra settings): Parquet writes
go through DataSource V1, so everything ran on the GPU, including the write
(`GpuInsertIntoHadoopFsRelationCommand`). The sort query writes all 300M rows.

| Query | GPU (s) | CPU (s) | Speedup |
|---|---:|---:|---:|
| agg | 12.2 | 25.1 | 2.1× |
| join | 1.6 | 4.4 | 2.8× |
| sort | 19.4 | 39.5 | 2.0× |
| window | 4.2 | 13.5 | 3.2× |
| filter | 0.8 | 1.4 | 1.7× |
| **Total** | **38.2** | **83.9** | **2.2×** |

These times include writing to HDFS, and they vary between runs: over three GPU runs, `sort` took
10.5, 19.4 and 38.7 s.

**Iceberg and Hive tables** ([`base/iceberg_check.sh`](base/iceberg_check.sh), stock config, 30M
rows). Each table's checksum was compared with the same rows read straight from Parquet:

| Step | `suppressPlanningFailure=true` | default (`false`) |
|---|---|---|
| Create an Iceberg table from a query (`writeTo(...).using("iceberg").create()`) | works, entirely on the CPU | fails: `Unrecognized Iceberg version` |
| Read the Iceberg table and aggregate | scan on the CPU, aggregation on the GPU; checksum matches | (no table to read) |
| Append to the Iceberg table (`writeTo(...).append()`) | the write on the CPU, the query feeding it on the GPU | (no table to append to) |
| Hive table: `saveAsTable`, read and aggregate, `insertInto` | all on the GPU; checksum matches | all on the GPU; checksum matches |

To reproduce all of this on your cluster, after §2 and §3:

```bash
base/plugin_load.sh   # downloads the jar; RAPIDS as-is (rejected) vs. with the shim override
base/benchmark.sh     # Iceberg-free config, GPU and CPU runs, final plans (~10 min at 300M rows)
base/iceberg_check.sh # Iceberg and Hive tables with the stock config, suppressPlanningFailure on and off
base/summary.sh       # rebuilds the results table from the logs
base/diagnose.sh      # if the GPU run fails: config, Iceberg jars, stack traces, log tails
```

All of them take `RAPIDS_VERSION` (default `26.10.0`) and `WORK` (default `/tmp/rapids`).
`benchmark.sh` also takes `ROWS`, `RUNS`, `SINK=parquet`, `STOCK_CONF=1` and `GPU_CONF`; its header
explains each one. Its executor sizes match the tested hosts, so edit the `common`, `gpu` and `cpu`
arrays to fit yours.

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `IllegalArgumentException: This RAPIDS Plugin build does not support Spark build 3.5.4.3.5.7191000.0-30` | The shim override isn't set | Set `spark.rapids.shims-provider-override` (§6) |
| `The RAPIDS Accelerator when used with Kryo needs to register some serializers` | The Kryo registrator isn't set | Set `spark.kryo.registrator` (§6) |
| `UnsupportedOperationException: Unrecognized Iceberg version: d38e7d2d…` in `GpuOverrideUtil` | A DataSource V2 read or write with the stock config, which loads Iceberg 1.3 | Set `suppressPlanningFailure=true` (§5a), or use the Iceberg-free config (§5b) |
| `WARN GpuOverrideUtil: Failed to apply GPU overrides, falling back on the original plan` | Expected with option 5a when a query touches Iceberg | Nothing to do if the message mentions `Unrecognized Iceberg version`. Any other reason is a real planning failure worth investigating |
| `IllegalAccessError: failed to access class org.apache.iceberg.spark.source.SparkWrite from class org.apache.iceberg.spark.source.GpuSparkWrite$` | RAPIDS 26.02.0 with the stock config, on any DataSource V2 write | Use 26.10.0, or the Iceberg-free config (§5b). `suppressPlanningFailure` doesn't help here |
| Submission fails writing `/user/<name>/.sparkStaging` | The user has no HDFS home directory | Create `/user/<name>` (§2) |
| The application stays `ACCEPTED`, or executors never start | Not enough free `yarn.io/gpu`, or YARN doesn't see the GPUs | Check §3, and don't ask for more GPU executors than there are GPUs |
| `explain()` shows only CPU operators | That's the plan before adaptive execution replans it | Check the final plan after the query finishes (§7) |
| The final plan has CPU operators | RAPIDS doesn't support an operator or data type in the query | Add `spark.rapids.sql.explain=NOT_ON_GPU` and read the reasons in the driver log |

## 10. Other versions

- **Cloudera Data Engineering (CDE) on Runtime 7.1.9**: see [`README-719.md`](README-719.md). It's the
  same Spark 3.5.4 code line and needs the same shim override, but the jar is baked into a custom CDE
  runtime image instead of being passed with `--jars`.
- **Runtime 7.3.2**: see [`README-732.md`](README-732.md). Cloudera's 7.3.2 Spark changes the
  constructor of an internal class, `MapPartitionsRDD`, so the published RAPIDS jar has to be patched
  before it works.

To tell which case you're in, look at that constructor in your `spark-core` jar.
[`base/check_cluster.sh`](base/check_cluster.sh) prints it (it needs a JDK, for `javap`). The published
RAPIDS jar expects Apache Spark's 7-argument version. If yours ends in `boolean, boolean, boolean,
scala.reflect.ClassTag<U>, scala.reflect.ClassTag<T>)`, this guide applies. If it has an extra
`scala.Option` argument, follow [`README-732.md`](README-732.md) instead.

To check every Spark build on a host at once, including parcels that aren't active, run
[`scripts/check_spark_core.sh`](scripts/check_spark_core.sh) from your workstation. It needs a local
JDK, not one on the cluster:

```bash
SSH_OPTS="-q -F <your-ssh-config>" scripts/check_spark_core.sh ssh://<any-cluster-host>
```

On the cluster in this guide, it found two Spark 3 builds:

| Build | Where | `MapPartitionsRDD` ctor | Stock RAPIDS jar |
|---|---|---|---|
| `3.5.4.3.5.7191000.0-30` | SPARK3 parcel (CDS 3.5), which `spark3-submit` runs | 7 args | links |
| `3.3.2.3.3.7191000.1-126` | bundled in the CDH parcel, `CDH-7.1.9-1.cdh7.1.9.p1069.74330271/jars/` | 8 args | **doesn't link** |

So the version of 7.1.9 doesn't settle it either. Run RAPIDS jobs on CDS 3.5's Spark, and check
with `spark3-submit --version` that it is the one your gateway uses. The Spark 3.3.2 build has the
same extra parameter as 7.3.2. Later CDS 3.5 builds for 7.1.9 may pick it up too, so check again
after a Spark upgrade.

## 11. Known limitations

- **Iceberg runs on the CPU at best.** RAPIDS 26.10 can only accelerate Iceberg 1.6 and later, and CDS
  3.5.4 ships 1.3. With `suppressPlanningFailure=true` (§5a), the Iceberg parts of a query run on the
  CPU; without it, they fail. Getting them onto the GPU would take RAPIDS support for Iceberg 1.3, not
  a change to Spark.
- **RAPIDS 26.02.0, the version used on CDE, only partly works here.** With the stock config, it ran
  the Parquet-writing benchmark fully on the GPU. But every DataSource V2 write (Iceberg tables, the
  `noop` sink) fails with `IllegalAccessError`. Nothing tried got around it, including putting the
  jar on `extraClassPath` and `suppressPlanningFailure`. Use 26.10.0.
- **NVIDIA hasn't tested RAPIDS with the shim override.** The driver log says so: `Overriding Spark
  shims provider ... This may be an untested configuration!`. The evidence here is five query shapes
  that ran fully on the GPU without errors, plus the Iceberg and Hive table tests in §8.
- **Not tested here:** PySpark and UDFs, cluster deploy mode, Hive ACID tables, larger data sets that
  spill, `RapidsShuffleManager`, and the `cuda13` jar.
- **x86-64 only.** Only the `cuda12` jar on `amd64` was tested.
