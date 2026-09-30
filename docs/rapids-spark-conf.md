# Spark RAPIDS configuration for CDE

Not yet validated on CDE — the GPU runtime image cannot be pulled (README section 6). This records the
configuration that is **known to work on Spark 3.5.4** from prior Spark RAPIDS work on Cloudera AI, plus
the three things that must change for CDE. Keeping it written down avoids rediscovering the version
lock the hard way.

## The version lock

The RAPIDS jar, the shim override, the shuffle-manager class and the Spark **patch** version are a
four-way lock. Get one wrong and it fails at the first shuffle, not at startup.

| Spark | RAPIDS jar | Shim override | Shuffle manager |
|---|---|---|---|
| 3.5.4 | `com.nvidia:rapids-4-spark_2.12:26.02.0` | `com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider` | `com.nvidia.spark.rapids.spark354.RapidsShuffleManager` |
| 3.5.1 | `…:25.10.0` | `…shims.spark351.…` | `…rapids.spark351.…` |
| 3.3.0 | `…:25.08.0` | `…shims.spark330.…` | — |

**CDE here is Spark 3.5.4, so the first row is the one to use.** The `spark351` shim was compiled
against Spark 3.5.1's `IndexShuffleBlockResolver(SparkConf, BlockManager)` constructor, which does not
exist on 3.5.4 — loading it there throws `NoSuchMethodError` at the first shuffle. A patch-version
mismatch is not a warning, it is a hard failure partway into the job.

**CUDA:** the proven `26.02.0` is a CUDA 12 build, and this cluster's driver is CUDA 13.4, which runs
CUDA 12 binaries fine. A `cuda13`-classified jar (26.08.x) is also plausible here but is *not* the
combination we have seen work against Spark 3.5.4. Treat cuda12-vs-cuda13 as an open decision; prefer
the proven one first and only change one variable at a time.

## Known-good Spark configuration

From a 250M-row star-schema ETL (5 joins, 2 wide aggregations, 2 `saveAsTable`) on T4 GPUs.

```
# --- plugin and jar ---------------------------------------------------------------
spark.plugins                                    com.nvidia.spark.SQLPlugin
spark.rapids.sql.enabled                         true
spark.rapids.shims-provider-override             com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider
spark.shuffle.manager                            com.nvidia.spark.rapids.spark354.RapidsShuffleManager
spark.kryo.registrator                           com.nvidia.spark.rapids.GpuKryoRegistrator
spark.driver.extraClassPath                      <path to rapids jar in the image>
spark.executor.extraClassPath                    <path to rapids jar in the image>

# --- GPU resource wiring ----------------------------------------------------------
spark.executor.resource.gpu.amount               1
spark.task.resource.gpu.amount                   0.125
spark.executor.resource.gpu.vendor               nvidia.com
spark.executor.resource.gpu.discoveryScript      <path inside the image>

# --- sizing: 3 GPUs total on this cluster, 1 per worker ---------------------------
spark.dynamicAllocation.enabled                  false
spark.executor.instances                         3
spark.executor.cores                             8
spark.executor.memory                            12g
spark.executor.memoryOverhead                    8g

# --- RAPIDS memory / throughput ---------------------------------------------------
spark.rapids.memory.pinnedPool.size              2g
spark.rapids.sql.batchSizeBytes                  1g
spark.rapids.sql.concurrentGpuTasks              2
spark.rapids.sql.multiThreadedRead.numThreads    32
spark.rapids.shuffle.multiThreaded.reader.threads 24
spark.rapids.shuffle.multiThreaded.writer.threads 24

# --- the two knobs that actually produced the speedup -----------------------------
spark.sql.autoBroadcastJoinThreshold             512m
spark.sql.files.maxPartitionBytes                1g
spark.sql.shuffle.partitions                     1000
spark.locality.wait                              0
```

Two findings worth carrying over rather than re-deriving:

- **The speedup came from the non-GPU knobs.** `autoBroadcastJoinThreshold -1 → 512m` and
  `maxPartitionBytes 4g → 1g` accounted for essentially all of a ~2.3× improvement. Raising
  `concurrentGpuTasks` or `task.resource.gpu.amount` was net-*negative*. Tune the SQL side before the
  GPU side.
- **Executor memory must leave room for RAPIDS off-heap.** JVM heap, the pinned pool (2g), the host
  spill store (~3g) and JVM overhead all count against the pod cgroup limit. Rule of thumb: for a pod
  cap of N GB, keep JVM heap ≤ N − 8 GB. `12g` heap + `8g` overhead worked where `16g` + `4g` got
  OOMKilled the moment spilling began.

## What must change for CDE

1. **Bake the jar into the image.** The CAI runs resolved it via `spark.jars.packages` (Ivy, from Maven
   Central / `edge.urm.nvidia.com`). On-prem has no guaranteed route there, and `RapidsShuffleManager`
   needs the jar on `driver/executor.extraClassPath` early in startup regardless — `spark.jars.packages`
   alone does not get it there in time. Put it in the image and reference the in-image path.
2. **The GPU discovery script must live inside the image**, not at `/home/cdsw/getGpusResources.sh`.
   It is just the stock Apache Spark script: `nvidia-smi --query-gpu=index --format=csv,noheader`
   formatted as `{"name": "gpu", "addresses": ["0"]}`.
3. **Size for 3 GPUs, not 8.** One A10G per worker, `sharing-strategy=none`, `mig.capable=false`. The
   qualification tool's sizing suggestions are unusable here — it recommended 63 executors, which with
   `executor.resource.gpu.amount=1` would demand 63 concurrent GPUs.

## Verifying it actually used the GPU

**RAPIDS falls back to CPU silently.** A green job proves nothing, and a wall-clock number alone can be
explained by other changes. Two checks:

```
spark.rapids.sql.explain    NOT_ON_GPU
```

Then read the executor logs for operators that did *not* make it onto the GPU. This is a diagnostic,
not a tuning — set it, read the logs, confirm no unexpected fallbacks, then unset it.

Second, watch `nvidia-smi` on a worker while the job runs. Neither the prior scripts nor this repo
assert on GPU usage, so nothing fails loudly if the plan quietly reverts to CPU.

## Porting target

`cai_rapids_articles/spark-rapids-qualification-tool/04_etl_gpu_V2.py` is the workload to port, with
the `01_generate_*_skewed.py` datagen scripts. Those build every input table from `spark.range()` with
`F.rand()`/`F.expr()` columns — no external data dependency — which is what makes the whole workload
reproducible on a fresh cluster. There is a byte-identical CPU twin (`02_etl_cpu.py`) for A/B timing.

Couplings to strip when porting: `import cml.data_v1`, `os.environ["CDSW_ENGINE_ID"]` and
`CDSW_DOMAIN` (hard `KeyError` off CAI), the `/home/cdsw/...` jar and discovery-script paths, the
`s3a://…` value for `spark.kerberos.access.hadoopFileSystems`, and the hardcoded database name.
