# The blocker: OSS spark-rapids cannot link against Cloudera's Spark 3.5.4

This is the result of the custom-runtime attempt. Everything we built works; the jar inside it
cannot. Read this before trying to tune anything.

## What works (all verified on CDE 1.26.0-b557, PVC CE 1.5.5-h3000)

| Step | Evidence |
|---|---|
| Custom runtime image builds and pushes | `amd64`, uid 1345, 8.59 GB, digest `sha256:7de4da05…` |
| CDE accepts it as a resource | `cde resource create --type custom-runtime-image` → `ready` |
| **It overrides the VC's `gpuImage`** | Livy's `spark-submit` carries `spark.kubernetes.container.image=pauldefusco/cde-rapids-runtime-3.5.4:26.02.0`; both pods pulled it |
| GPU scheduling | executor pod gets `nvidia.com/gpu: 1` in requests **and** limits; yunikorn allocates `nvidia.com/gpu:1` |
| **Executors see the GPU** | `host=…exec-1  gpu=0, NVIDIA A10G` — the in-image discovery script works |
| RAPIDS plugin loads | `RAPIDS Accelerator build: version 26.02.0`, `cudf 26.02.0` |
| The shim loads | `Overriding Spark shims provider to …spark354…`, classloader updated with `spark354/` |
| **RAPIDS plans the query on the GPU** | stack trace shows `GpuHashAggregateExec`, `GpuShuffleExchangeExecBase` |

So the custom-runtime route is sound and the GPU plumbing is complete. The approach is not the problem.

## What fails

The first action on any GPU plan throws:

```
java.lang.NoSuchMethodError: 'void org.apache.spark.rdd.MapPartitionsRDD.<init>(
    org.apache.spark.rdd.RDD, scala.Function3, boolean, boolean, boolean,
    scala.reflect.ClassTag, scala.reflect.ClassTag)'
  at org.apache.spark.rapids.LocationPreservingMapPartitionsRDD.<init>(…:44)
  at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar(GpuExec.scala:341)
```

## This is specific to the 7.3.2 / CDE 1.26 build — not to Cloudera Spark in general

Read this section before concluding anything general. The incompatibility is a **build-window**
problem. Cloudera's own older Spark links against OSS RAPIDS fine:

| Spark build | Runtime / CDP | `MapPartitionsRDD` ctor |
|---|---|---|
| Cloudera `3.5.1.1.23.7218.0-28` | 7.2.18 / 1.23 | 7 args — **links** |
| Cloudera `3.5.4.1.25.731.0-41` | 7.3.1 / 1.25 — **what CAI runs** | not yet measured; see below |
| Cloudera `3.5.4.1.26.732.0-45` | 7.3.2 / 1.26 — **this cluster** | 8 args — **fails** |
| Apache 3.5.4, 3.5.5, 3.5.6, 3.5.7 | — | 7 args |
| Apache 4.0.0, 4.0.1 | — | 7 args |

The eighth parameter is `isDeterministic: Option[Boolean]` — a *defaulted* sixth constructor
parameter (`$lessinit$greater$default$6`) feeding a new private `defaultMapOutputDeterministicLevel`
used by `getOutputDeterministicLevel`. Being defaulted it is source-compatible, so Cloudera's own
build recompiles cleanly; only **pre-compiled** callers like the RAPIDS jar break.

It appears in **no Apache Spark release**, 3.5.x or 4.x, and not in Cloudera 7.2.18 — so it entered
in the Cloudera 7.3.2 / 1.26 line.

### Why this matters: the CAI comparison

`pdefusco/CAI_Rapids_Demos/spark-rapids-qualification-tool` runs the **same dependency set** —
`com.nvidia:rapids-4-spark_2.12:26.02.0`, the `spark354` shim override, the `spark354`
`RapidsShuffleManager`, a cuda12 jar, an equivalent `getGpusResources.sh`. Nothing there is out of
line with this project. What differs is the Spark build underneath: CAI's event logs report
`sparkVersion 3.5.4.1.25.731.0-41` (7.3.1), **not** this cluster's `3.5.4.1.26.732.0-45` (7.3.2).

So "it works in CAI" and "it fails here" are consistent, and the deciding variable is the Cloudera
patch line — not the RAPIDS version, the shim, or the CUDA build. Confirm with one command in a CAI
session:

```bash
javap -p -cp $(ls $SPARK_HOME/jars/spark-core_2.12-*.jar) \
  org.apache.spark.rdd.MapPartitionsRDD | grep 'MapPartitionsRDD('
```

Seven arguments there confirms the whole diagnosis.

## Why no amount of configuration fixes it on *this* build

`javap` on the cluster's own jar, inside the runtime image:

```bash
docker run --rm --platform linux/amd64 --entrypoint bash \
  pauldefusco/cde-rapids-runtime-3.5.4:26.02.0 -c '
    unzip -o -q /opt/spark/jars/spark-core_2.12-*.jar \
      org/apache/spark/rdd/MapPartitionsRDD.class -d /tmp/x
    javap -p -classpath /tmp/x org.apache.spark.rdd.MapPartitionsRDD | grep "MapPartitionsRDD("'
```

| | constructor |
|---|---|
| Cloudera `3.5.4.1.26.732.0-45` | `(RDD, Function3, boolean, boolean, boolean, `**`scala.Option<Object>`**`, ClassTag, ClassTag)` |
| What OSS RAPIDS 26.02.0 calls | `(RDD, Function3, boolean, boolean, boolean, ClassTag, ClassTag)` |

Cloudera added an eighth parameter, and there is **no 7-arg overload**. Three consequences:

1. **Not version skew between RAPIDS releases.** No OSS RAPIDS release can carry the 8-arg call,
   because the 8-arg constructor exists in no Apache Spark release — so NVIDIA has never had a Spark
   to compile it against. Upgrading or downgrading RAPIDS cannot reach this.
2. **`shims-provider-override` cannot help — verified, not assumed.**
   `LocationPreservingMapPartitionsRDD` *is* compiled per-shim (one copy under each of
   `spark330` … `spark357`), so the override genuinely does select different bytecode. It still
   cannot help: every candidate emits the 7-arg call.

   ```bash
   for s in spark354 spark355 spark356 spark357; do
     unzip -o -q rapids-4-spark_2.12-26.02.0-cuda12.jar \
       "$s/org/apache/spark/rapids/LocationPreservingMapPartitionsRDD.class" -d lp
     javap -c -p -classpath "lp/$s" org.apache.spark.rapids.LocationPreservingMapPartitionsRDD \
       | grep -oE 'MapPartitionsRDD\."<init>":\([^)]*\)V' | head -1
   done
   # all four: (Lorg/apache/spark/rdd/RDD;Lscala/Function3;ZZZLscala/reflect/ClassTag;Lscala/reflect/ClassTag;)V
   ```
3. **Not avoidable per-query.** The call site is `GpuExec.doExecuteColumnar` — the base method for
   *every* GPU operator — so it fires on any GPU plan, whatever the operators or shuffle manager.

## There is no OSS CDH shim for Spark 3.5.x

`rapids-4-spark_2.12-26.02.0-cuda12.jar` ships shims `spark330 … spark357` plus three Databricks
shims (`spark332db`, `spark341db`, `spark350db143`) and **no CDH shim**. It *does* contain
`com.nvidia.spark.rapids.ClouderaShimVersion(major, minor, patch, clouderaVersion)`, so NVIDIA's build
system treats Cloudera as a first-class shim target — the public Maven artifact simply has no
`spark354cdh` build. Historically CDH shims existed only for Spark 3.3.x (`spark330cdh`,
`spark332cdh`).

```bash
unzip -Z1 rapids-4-spark_2.12-26.02.0-cuda12.jar | awk -F/ '/^spark[0-9]/{print $1}' | sort -u
```

## Conclusion

On CDE 1.26 / Spark 3.5.4 the only viable RAPIDS jar is a **Cloudera-built** one — which is what
`dex-spark-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557` must contain. That image returns `NotFound` on
**both** registries we can reach:

- `container.repository.cloudera.com/cdp-private/…` (the cluster's own path — CPU variants pull fine)
- `docker-private.infra.cloudera.com/cloudera/dex/…` (the internal registry this image is built *from*)

So GPU Spark on **this** CDE build needs either a Cloudera-built RAPIDS jar or a Spark build without
the `isDeterministic` parameter. Baking the OSS jar into a custom runtime on 1.26 is a dead end — but
it is a dead end for this build, not for CDE generally.

### If you pick this up again — cheapest test first

1. **Confirm the build window.** Run the `javap` one-liner above in a CAI session (Spark
   `3.5.4.1.25.731.0-41`). Seven arguments proves the parameter is new in 7.3.2 and that 7.3.1-era
   Spark is a viable target.
2. **Rebuild the runtime `FROM` a CDE 1.25 / 7.3.1 Spark base** — i.e. match the Spark CAI runs,
   keeping every other dependency identical since those are already proven correct. The custom
   runtime image fully controls the container and we have shown it overrides the VC's `gpuImage`, so
   this needs no VC recreate. **Untested caveat:** a 7.3.1 runtime inside a 1.26 virtual cluster may
   fall out of step with Livy / CDE job integration. Treat it as a hypothesis to test, not a
   recommendation.
3. **Ask Cloudera** for the published GPU runtime image for the exact CDE build, or for their RAPIDS
   jar, if 1 and 2 do not land.
4. Re-run `probes/probe_images.sh` after any entitlement change; it is the cheapest re-test.
