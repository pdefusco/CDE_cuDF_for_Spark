# How much else is broken? A jar-wide linkage scan

Background detail for `README-732.md` §8 (CDE 7.3.2) and `README-732-base.md` (Base 7.3.2). The scan
below was done by hand against CDE 1.26 and the 26.02.0 jar. It is now a script,
[`base/linkage_scan.sh`](../base/linkage_scan.sh), and its Base 7.3.2 results are in
[their own section](#runtime-732-base-spark-3-on-yarn), which also corrects row 4 of the triage.

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
| 4 | `CreateHiveTableAsSelectCommand.getWritingCommand(…)` | **Real mismatch, unreachable on Spark 3.4+.** First ranked as the one that survives; corrected after the Base 7.3.2 scan. See below. |
| 5–6 | `MergedBlockMeta.readChunkBitmaps()`, `MapOutputTracker.getMapSizesForMergeResult(…)` | **Real, and misses on Apache Spark too.** Push-based shuffle merge only — off by default. See below. |
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

### 4, Hive-serde CTAS: real, but not registered on 3.4+

```
RAPIDS calls:  CreateHiveTableAsSelectCommand.getWritingCommand(SessionCatalog, CatalogTable, boolean)
1.26 declares: private                        getWritingCommand(CatalogTable, boolean)
```

Apache 3.5.9 declares it identically to Cloudera, so this is **Apache maintenance drift after 3.5.4**,
not a Cloudera fork difference — it would break on Apache 3.5.9 too.

Its only caller is `GpuCreateHiveTableAsSelectCommand`, i.e. `CREATE TABLE … AS SELECT` or
`saveAsTable` against a **Hive-serde** table (`USING hive`, `STORED AS …`).

**Correction: that caller is never reached on 3.5.4.** This was first ranked "real and reachable, the
one that survives". The automated reachability pass on Base 7.3.2 dropped it, and the bytecode agrees:
only the `spark330`–`spark334` `HiveProviderCmdShims` register `CreateHiveTableAsSelectCommand` for GPU
replacement. From `spark340` on, `spark354` included, they register only `InsertIntoHiveTable`, and
`GpuCreateHiveTableAsSelectCommandMeta` is referenced by nothing but itself. The same holds in the
26.02.0 jar used here. A Hive-serde CTAS on 3.5.4 is not converted by RAPIDS at all, so it never
reaches the mismatched call.

A fix would not be a pure recompile anyway: the argument list changed, so unlike `MapPartitionsRDD`
the upstream source is no longer source-compatible. Nothing reaches it, so nothing is patched.

### 5–6, push-based shuffle: a RAPIDS shading bug

Both descriptors name RAPIDS's shaded copy of RoaringBitmap:

```
RAPIDS calls:   MergedBlockMeta.readChunkBitmaps()[Lcom/nvidia/shaded/spark/org/roaringbitmap/RoaringBitmap;
Spark declares: MergedBlockMeta.readChunkBitmaps()[Lorg/roaringbitmap/RoaringBitmap;
```

and likewise for `getMapSizesForMergeResult(II…RoaringBitmap;)`. RAPIDS relocated a library type that
appears in Spark's own method signatures, so these miss on Apache Spark as well; it isn't a Cloudera
difference. Same references in 26.02.0 and 26.10.0. The callers are `RapidsPushBasedFetchHelper` and
its anonymous class, which run only with `spark.shuffle.push.enabled=true` (default `false`).

## Runtime 7.3.2 Base, Spark 3 on YARN

Run with [`base/linkage_scan.sh`](../base/linkage_scan.sh) on ds1116's gateway, against the CDH
parcel's `lib/spark3/jars` (Spark `3.5.4.7.3.2.0-957`) plus the client `classpath.txt`, and the
26.10.0 jar both stock and after `base/patch_rapids_jar.sh`. Patched jar:

| | Total | Delta/Iceberg/Databricks only | Unreachable only | To triage |
|---|---:|---:|---:|---:|
| Missing members | 248 | 98 | 146 | **4** |
| Missing classes | 243 | 233 | 10 | **0** |
| Unverified (inherited from a JDK class) | 14 | – | 1 | 13 |

In scope: 6,459 of the jar's 22,400 classes, 5,867 of them reachable. External Spark member
references: 4,836 unique, 3,700 resolve. These counts aren't comparable with the 1.26 table above:
a different jar (26.10.0, 22,400 classes against 17,833), a different classpath, and here the Delta, Iceberg and Databricks
misses are inside the total rather than set aside.

**Two-way control:** the stock-vs-patched diff is exactly one member, the 7-argument
`MapPartitionsRDD.<init>`, missing from the stock jar only.

The four to triage are the 1.26 rows 1–2 and 5–6, unchanged:

| missing symbol | caller | verdict |
|---|---|---|
| `FileScan.…$$normalizedDataFilters$`, `…$$normalizedPartitionFilters$` | `spark354/…/GpuOrcScan` (+2 more) | As rows 1–2: DataSource V2 file scans only; `useV1SourceList` keeps Parquet, ORC and CSV on V1. |
| `MapOutputTracker.getMapSizesForMergeResult(II…RoaringBitmap;)` | `spark354/…/RapidsPushBasedFetchHelper` | As rows 5–6: the shading bug; push-based shuffle only. |
| `MergedBlockMeta.readChunkBitmaps()` | `spark-shared/…/RapidsPushBasedFetchHelper$anon$1` (+1 more) | Same. |

The 13 unverified are standard JDK methods reached through Spark types (`Throwable.getCause`,
`AutoCloseable.close`, `Future.isCancelled`, `ClassLoader.loadClass` and similar). Rows 3, 4 and
7–16 aren't among the four: each one either resolves against 7.3.2's Spark or has no reachable
caller (row 4 for the reason given above). Nothing reachable by default is left, which
matches the benchmark and Iceberg runs in `README-732-base.md`.

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
4. **Reachability was hand-traced** on CDE 1.26 for the ambiguous shim cases. That is where the
   `InSubqueryExec` false positive was caught — and the same hand-tracing could in principle discard a
   real one. `base/linkage_scan.sh` now computes it, but per class, not per method: a dead method in a
   live class still counts as live. So it over-reports, and it is what showed row 4 was over-reported.
5. **The harness had bugs.** Four were found and fixed during the run (a `javap` path quirk for
   hyphenated entries, a resolution-performance bug, a constant-pool quoting mismatch worth 284 false
   positives, and a missing implicit `java.lang.Object` supertype worth 19). The triage above then found
   a fifth class of error. Assume there is a sixth.
6. **It is pinned to these exact jars.** Any CDE patch release or Runtime upgrade moves the target;
   re-run it.

**So: this is a strong bound on one class of failure, not a completeness proof.** The 16 are
*predictions to test*, and running the real 250M-row ETL is the test.

## Re-running it

On a Base gateway, after `base/plugin_load.sh` (and `base/patch_rapids_jar.sh`, if it applies):

```bash
base/linkage_scan.sh       # RAPIDS_VERSION, WORK, OUT as for the other base/ scripts
```

It finds the Spark 3 jars in the SPARK3 parcel (7.1.9) or the CDH parcel (7.3.x). The scan itself is
an embedded Python script whose arguments are a Spark jars directory, a `classpath.txt` (may be
absent) and the RAPIDS jar(s), so for CDE it can be pointed at `/opt/spark/jars` copied out of the
runtime image. That hasn't been done; the 1.26 numbers above come from the earlier hand scan.

The cheap per-finding check, which is what the triage actually used:

```bash
# what RAPIDS calls
javap -p -c -cp spark354 <caller.class.Name> | grep '<SparkClass>\.'
# what Cloudera declares
docker run --rm --platform linux/amd64 --entrypoint bash <image> -c \
  'javap -p -cp $(ls /opt/spark/jars/spark-sql_2.12-*.jar) <org.apache.spark.X> | grep <member>'
```

On macOS, prefix greps over `.class` files with `LC_ALL=C` — BSD grep silently matches nothing in files
containing invalid UTF-8, and `strings` misreads Java's `CAFEBABE` magic as a Mach-O fat binary.
