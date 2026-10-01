# How much else is broken? A jar-wide linkage scan

Background detail for README §8.

The patch in `runtime/` fixes **one** `NoSuchMethodError`. The obvious next question is whether it is
the only one, or the first of many. Guessing is cheap and worthless in both directions — "it's one
build-window difference, everything else is fine" and "a vendor fork could differ anywhere" are both
priors, not findings. So this was measured.

## What was actually measured

**Not** a diff of Cloudera's Spark against Apache's. That would produce thousands of differences, almost
all irrelevant. The question that matters for a pre-compiled plugin is narrower and one-directional:

> For every Spark symbol the RAPIDS jar *references*, does a member with a **matching descriptor** exist
> in the Spark jars actually present in the image?

That is precisely what the JVM asks at link time, and the only thing that can produce the class of
failure we hit.

### Shim filtering is not optional

`rapids-4-spark_2.12-26.02.0.jar` stores many classes ~21 times, once per Spark shim
(`spark330/` … `spark357/`, plus `spark332db`/`spark341db`/`spark350db143` for Databricks and a
`spark-shared/` dedup bucket). Only **`spark354/`**, **`spark-shared/`** and the unprefixed roots
(`com/`, `org/`, `ai/`) load on Spark 3.5.4.

An unfiltered scan is worse than no scan: a `spark330` class referencing Spark 3.3 APIs is *correct*
and absent from 3.5.4, so the output drowns in false positives. After filtering, **5,397** of the
jar's 17,833 classes are in scope.

## Result

| | |
|---|---|
| Classes in scope | 5,397 of 17,833 |
| Unique external Spark references | 4,473 |
| Resolve against 1.26's jars | 4,457 — **99.6%** |
| Did not resolve | **16** |

Plus 48 member misses and 189 class-level misses that are moot on their face: callers confined to the
Delta Lake / Iceberg / Databricks code paths, or dead shim variants. Hive, Avro, Kafka and Parquet jars
**are** present in the image; only Delta, Iceberg and Databricks are genuinely absent, and nothing here
uses them.

**Two-way control:** the scan correctly reports the two patched classes as resolving, *and* correctly
flags the original 7-arg `MapPartitionsRDD` descriptor as unresolved. It detects the bug we know about
and the fix we know about.

## Triage of the 16

A raw miss list is **not** a defect list. Each was traced back to its calling bytecode:

| # | missing symbol | verdict |
|---|---|---|
| 1–2 | `FileScan.org$apache$spark$…$$normalizedPartitionFilters$`, `…$$normalizedDataFilters$` | **Real mismatch, unreachable by default.** See below. |
| 3 | `CreateHiveTableAsSelectCommand.outputColumns()` | **False positive — mis-attributed.** RAPIDS calls `DataWritingCommand.outputColumns$(DataWritingCommand)`, which 1.26 declares. |
| 4 | `CreateHiveTableAsSelectCommand.getWritingCommand(…)` | **Real and reachable.** The one that survives. See below. |
| 5–6 | `MergedBlockMeta.readChunkBitmaps()`, `MapOutputTracker.getMapSizesForMergeResult(…)` | Push-based shuffle merge only — off by default. |
| 7–9 | `StoragePartitionJoinParams.<init>`, `.apply$default$3`, `.apply$default$5` | Storage-partitioned join only — `spark.sql.sources.v2.bucketing.enabled` defaults to `false`. |
| 10–16 | `InSubqueryExec.copy` + `copy$default$4…9` | **False positive — dead code.** See below. |

### 10–16, `InSubqueryExec`: dead code

This was initially ranked the *highest* risk, because dynamic partition pruning fires on exactly the
star-schema joins the target ETL is built from. It is not a risk at all.

The caller is
`spark-shared/com/nvidia/spark/rapids/shims/FileSourceScanExecMeta$$anonfun$$nestedInanonfun$convertDynamicPruningFilters$1$1`
— an orphan inner class. `spark354/` ships its **own** `FileSourceScanExecMeta`, which shadows the
shared one, and `javap -c` on it finds **zero** references to `InSubqueryExec` or
`convertDynamicPruningFilters`; it rewrites DPP through `SubqueryBroadcastExec` instead. The outer class
that would invoke that anonfun does not exist on the 3.5.4 path.

**The lesson generalises:** `spark-shared/` means "byte-identical across *some* subset of shims," which
can exclude yours — and a version-specific shim directory can shadow it outright. Presence in
`spark-shared/` is not evidence of being live.

### 1–2, `FileScan`: real, but the v2 scan path never loads

RAPIDS calls the **private-mangled** trait forwarder; Cloudera declares the **short public** form:

```
RAPIDS calls:  FileScan.org$apache$spark$sql$execution$datasources$v2$FileScan$$normalizedPartitionFilters$(FileScan)
1.26 declares: FileScan.normalizedPartitionFilters$(FileScan)
```

Two things demote it:

- **It is not a 1.26 regression.** `javap` on CAI's `$SPARK_HOME` Spark (CDE 1.25) shows the **same
  short form**. The *long* name is Apache's — pip `pyspark` 3.5.9 has it — which is why RAPIDS, compiled
  against Apache 3.5.4, baked it in.
- **The callers never load.** `FileScan` is the **DataSource v2** path, and
  `spark.sql.sources.useV1SourceList` defaults to `avro,csv,json,kafka,orc,parquet,text`. Those formats
  go **v1** via `FileSourceScanExec` → `GpuFileSourceScanExec`; `GpuParquetScan`, `GpuOrcScan` and
  `GpuAvroScan` are never instantiated.

So this fires only if you explicitly shorten `useV1SourceList` or read through a v2-only catalog. It is
also the structural reason the CAI demos ran Parquet at scale on 1.25 without ever hitting it — not luck.

### 4, Hive-serde CTAS: the one that survives

```
RAPIDS calls:  CreateHiveTableAsSelectCommand.getWritingCommand(SessionCatalog, CatalogTable, boolean)
1.26 declares: private                        getWritingCommand(CatalogTable, boolean)
```

Apache 3.5.9 declares it identically to Cloudera, so this is **Apache maintenance drift after 3.5.4**,
not a Cloudera fork difference — it would break on Apache 3.5.9 too.

It is reached only by `GpuCreateHiveTableAsSelectCommand`, i.e. `CREATE TABLE … AS SELECT` or
`saveAsTable` against a **Hive-serde** table (`USING hive`, `STORED AS …`). The default datasource
provider routes `saveAsTable` to `GpuCreateDataSourceTableAsSelectCommand`, and writes into an
*existing* table go to `GpuInsertIntoHiveTable`. Neither touches it. Hive **reads** never touch it.

**This one would not be a pure recompile.** The argument list changed, so unlike `MapPartitionsRDD` the
upstream source is no longer source-compatible; `runtime/patch/` would need a real one-line edit to drop
the `SessionCatalog` argument. It is deliberately **not** patched pre-emptively — see "limits" below.

## Limits — what this scan does not tell you

Stated plainly, because 99.6% invites over-reading:

1. **Linkage only, not behaviour.** A method with a matching descriptor that *does something different*
   is invisible here. The patch itself is an example: RAPIDS now inherits Cloudera's default
   `isDeterministic`, which feeds `getOutputDeterministicLevel()` and decides whether a partition may be
   recomputed after a fetch failure. Whether that default is right for a GPU columnar RDD is
   uninvestigated, and would surface only under task retry or node loss.
2. **Reflection is invisible.** `Class.forName`, service loaders, `spark.sql.extensions` and Scala
   reflection leave nothing in a constant pool.
3. **Scope was RAPIDS → Spark.** Not RAPIDS → Hadoop/Hive/Parquet/ORC/Arrow, not Spark's callbacks into
   RAPIDS, and not the JNI/cuDF native layer.
4. **Reachability was hand-traced** for the ambiguous shim cases. That is where the `InSubqueryExec`
   false positive was caught — and the same hand-tracing could in principle discard a real one.
5. **The harness had bugs.** Four were found and fixed during the run (a `javap` path quirk for
   hyphenated entries, a resolution-performance bug, a constant-pool quoting mismatch worth 284 false
   positives, and a missing implicit `java.lang.Object` supertype worth 19). The triage above then found
   a fifth class of error. Assume there is a sixth.
6. **It is pinned to these exact jars.** Any CDE patch release moves the target; re-run it.

**So: this is a strong bound on one class of failure, not a completeness proof.** The 16 are
*predictions to test*, and running the real 250M-row ETL is the test.

## Re-running it

The scan is not checked in as a script — it was a one-off and the triage mattered more than the
automation. To redo it: extract the jar, keep `spark354/`, `spark-shared/`, `com/`, `org/`, `ai/`, and
for each class resolve every `Methodref`/`Fieldref`/`InterfaceMethodref` whose owner is an
`org/apache/spark/**` class against `javap -p -s` output from `/opt/spark/jars/*.jar` inside the runtime
image — walking supertypes, and including implicit `java.lang.Object`.

The cheap per-finding version, which is what the triage actually used:

```bash
# what RAPIDS calls
javap -p -c -cp spark354 <caller.class.Name> | grep '<SparkClass>\.'
# what Cloudera declares
docker run --rm --platform linux/amd64 --entrypoint bash <image> -c \
  'javap -p -cp $(ls /opt/spark/jars/spark-sql_2.12-*.jar) <org.apache.spark.X> | grep <member>'
```

On macOS, prefix greps over `.class` files with `LC_ALL=C` — BSD grep silently matches nothing in files
containing invalid UTF-8, and `strings` misreads Java's `CAFEBABE` magic as a Mach-O fat binary.
