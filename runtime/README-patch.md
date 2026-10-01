# Why this image patches the RAPIDS jar

**This image contains a modified third-party artifact.** It is an internal prototype. Do not present
it as a supported configuration, and do not redistribute the jar as if it were NVIDIA's.

## The problem

OSS `rapids-4-spark_2.12:26.02.0` cannot link against this cluster's Spark,
`3.5.4.1.26.732.0-45` (Runtime 7.3.2 / CDE 1.26). The first action on any GPU plan throws:

```
java.lang.NoSuchMethodError: 'void org.apache.spark.rdd.MapPartitionsRDD.<init>(
    org.apache.spark.rdd.RDD, scala.Function3, boolean, boolean, boolean,
    scala.reflect.ClassTag, scala.reflect.ClassTag)'
  at org.apache.spark.rapids.LocationPreservingMapPartitionsRDD.<init>(…:44)
  at com.nvidia.spark.rapids.GpuExec.doExecuteColumnar(GpuExec.scala:341)
```

Cloudera added a sixth constructor parameter to `MapPartitionsRDD` in this build line —
`isDeterministic: Option[Boolean]`, feeding a new private `defaultMapOutputDeterministicLevel()` that
`getOutputDeterministicLevel()` reads. There is no 7-arg overload.

Full diagnosis, including the build-range evidence showing this is a 7.3.2-window change and not a
Cloudera-fork-wide one, is in [`../docs/oss-rapids-vs-cloudera-spark.md`](../docs/oss-rapids-vs-cloudera-spark.md).

## Why recompiling is enough — and why it is not a code change

The new parameter is **defaulted**. That is the whole reason this works:

- **Source-compatible.** A 5-arg `super(...)` call in Scala source is legal against either ctor; the
  compiler supplies `isDeterministic` from `$lessinit$greater$default$6()`.
- **Binary-incompatible.** The *call site* bakes the full descriptor into the bytecode, so a class
  compiled against Apache Spark emits the 7-arg invocation and fails at link time.

So the unmodified upstream source, recompiled against Cloudera's `spark-core`, emits the 8-arg call
by itself. **No RAPIDS logic is altered.** The two classes are re-targeted at the Spark that is
actually present — nothing more.

## Why only two classes

Of the 17,833 classes in the jar, a byte-exact scan for the 7-arg ctor descriptor found 43 matching
files resolving to just **three** distinct classes — two real ones and one synthetic companion:

| Class | Copies (one per shim) |
|---|---|
| `org.apache.spark.rapids.LocationPreservingMapPartitionsRDD` | 21 |
| `org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD` | 21 |
| `…LocationPreservingMapPartitionsRDD$` (default-arg holder) | 1 |

Both are thin `MapPartitionsRDD` subclasses whose entire body is a constructor forward — one adds a
`getPreferredLocations` override, the other adds nothing at all. That is what makes this patch small
enough to trust by reading it.

## What the `patcher` stage does

1. Compiles `patch/*.scala` with `scala.tools.nsc.Main` against `/opt/spark/jars/*.jar`.
   **The base image is its own toolchain** — it already ships `scala-compiler-2.12.19.jar` *and* the
   exact `spark-core_2.12-3.5.4.1.26.732.0-45.jar` to compile against. No external toolchain, and no
   question about which Spark was on the classpath.
2. `-target:jvm-1.8`, because every existing class in the jar is major version 52. Mixing in major 61
   classes would be a second, self-inflicted linkage problem.
3. Asserts via `javap -c` that both classes emit
   `MapPartitionsRDD."<init>":(…;ZZZLscala/Option;…)V`. **This guard matters:** if a future base
   image drops the parameter, scalac silently emits the 7-arg call again and the patch becomes a
   no-op. Better to fail the build than to ship a silent no-op.
4. Writes the classes back to the two prefixes RAPIDS loads them from — `spark354/` for the classes,
   `spark-shared/` for the `$` default-arg companions — then asserts 4 entries landed.

Only the patched jar crosses into the runtime stage, so the unpatched 887 MB copy never lands in a
layer of the final image.

## Provenance of `patch/`

Both files come from `github.com/NVIDIA/spark-rapids` at tag **`v26.02.0`** — the same project and
version as the jar being patched. Apache-2.0; copyright headers preserved verbatim.

| File | Relationship to upstream |
|---|---|
| `LocationPreservingMapPartitionsRDD.scala` | byte-identical to upstream `sql-plugin/src/main/scala/com/nvidia/spark/rapids/LocationPreservingMapPartitionsRDD.scala` |
| `GpuColumnToRowMapPartitionsRDD.scala` | class body verbatim from `…/execution/InternalColumnarRddConverter.scala` lines 760-773, extracted standalone; **only the imports are narrowed** |

Verify the first yourself:

```bash
curl -sfL https://raw.githubusercontent.com/NVIDIA/spark-rapids/v26.02.0/sql-plugin/src/main/scala/com/nvidia/spark/rapids/LocationPreservingMapPartitionsRDD.scala \
  | diff - patch/LocationPreservingMapPartitionsRDD.scala && echo IDENTICAL
```

The second was extracted rather than compiled in place because its enclosing file is 773 lines that
drag in most of the plugin; the class itself needs only four imports.

## Known limits

- **This fixes exactly one linkage error.** If Cloudera changed other signatures RAPIDS depends on,
  the next failure will surface on the next run. The loop is now fast: read the `NoSuchMethodError`,
  `javap` the named Spark class out of this image, add the offending RAPIDS source to `patch/`.
  It is iterative, not a one-shot guarantee.
- **The upstream fix is a Cloudera-built RAPIDS jar**, which is what
  `dex-spark-runtime-gpu-3.5.4-7.3.2.0:1.26.0-b557` must contain. That image is `NotFound` on every
  registry reachable from here. If it becomes available, delete this patch and use it.
- **The filename is deliberately unchanged** (`rapids-4-spark_2.12-26.02.0-cuda12.jar`) so the
  `extraClassPath` lines in `../scripts/submit_rapids_smoke_test.sh` stay valid. It is **not** the OSS
  artifact byte-for-byte. Anyone inspecting this image by filename alone will be misled — which is
  why this file exists.

## Rebuilding

```bash
docker build --platform linux/amd64 -t <user>/cde-rapids-runtime-3.5.4:26.02.0-p1 runtime/
```

`--platform linux/amd64` is mandatory: the workers are amd64 RHEL 9.6 and the build host is arm64
Apple Silicon. The patcher stage runs scalac under emulation, which costs seconds for two small files.
