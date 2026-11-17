#!/usr/bin/env bash
# Time the same queries on GPU and CPU, and prove from the *final* (post-AQE) plan which operators
# actually ran on the GPU.
# By default both runs use a private copy of the Spark 3 client config with Iceberg removed (catalog,
# extensions, jars): with the stock config, RAPIDS fails every DataSource V2 write, and the noop sink used
# here is one (26.10: "Unrecognized Iceberg version: d38e7d2d..."; 26.02: IllegalAccessError from
# GpuSparkWrite$). Same executors, cores and memory for both; the CPU run does not load the plugin at all.
# Run on a Spark 3 gateway host after plugin_load.sh (which downloads the jar and discovery script),
# as a user with a Kerberos ticket. Env: RAPIDS_VERSION (default 26.10.0), WORK (default /tmp/rapids),
# ROWS (default 300000000; generated once into /tmp/rapids-bench on HDFS), RUNS (default "gpu cpu"),
# STOCK_CONF=1 to keep the stock Spark 3 client config, Iceberg included, SINK=parquet to write each result
# with the DataSource V1 Parquet writer instead of the (V2) noop sink, and GPU_CONF for extra spark args
# on the GPU run only (e.g. "--conf spark.rapids.sql.suppressPlanningFailure=true").
# Executor sizing below assumes 3 NodeManagers with 14 vcores, 48 GB and one GPU each.
set -u
# Spark 3 is the SPARK3 (CDS 3.5) parcel on 7.1.9 and part of the CDH parcel on 7.3.x.
spark_conf=/etc/spark3/conf; [[ -d $spark_conf ]] || spark_conf=/etc/spark/conf
spark_shell=$(command -v spark3-shell || command -v spark-shell)
dir=${WORK:-/tmp/rapids}
jar=$dir/rapids-4-spark_2.12-${RAPIDS_VERSION:-26.10.0}-cuda12.jar
provider=com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider
export ROWS=${ROWS:-300000000}
cd $dir

echo "== Where Iceberg enters the Spark 3 client config"
grep -rliE iceberg $spark_conf/ 2>/dev/null
grep -rhoiE "[^:,\"= ]*iceberg[^:,\"' ]*" $spark_conf/ 2>/dev/null | sort -u | cut -c1-200 | head -20
find /opt/cloudera/parcels/ -name 'iceberg-spark*.jar' 2>/dev/null | head

if [[ "${STOCK_CONF:-0}" == 1 ]]; then
  echo "== STOCK_CONF=1: using $spark_conf as-is, Iceberg included"
  unset SPARK_CONF_DIR
else
  conf=$dir/conf-noiceberg
  rm -rf $conf && mkdir -p $conf && cp -rL $spark_conf/. $conf/
  sed -i -E '/^spark\.sql\.extensions=|^spark\.sql\.catalog\.spark_catalog|^spark\.[A-Za-z.]*iceberg/Id' $conf/spark-defaults.conf
  for f in $(grep -rliE iceberg $conf); do
    sed -i -E "s#[^:,\"= ]*iceberg[^:,\"' ]*[:,]?##Ig" "$f"
  done
  [ -f $conf/classpath.txt ] && sed -i '/^\s*$/d' $conf/classpath.txt
  echo "== Iceberg left in $conf (expect none)"
  grep -rciE iceberg $conf | grep -v ":0$" || echo none
  export SPARK_CONF_DIR=$conf
fi

cat > $dir/bench.scala <<'EOF'
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
val mode = spark.conf.get("spark.rapids.sql.enabled", "cpu-no-plugin")
val base = "/tmp/rapids-bench"
val sink = sys.env.getOrElse("SINK", "noop")
// noop is a DataSource V2 write; Parquet to a path goes through the V1 writer (InsertIntoHadoopFsRelationCommand).
def save(name: String, df: org.apache.spark.sql.DataFrame): Unit =
  if (sink == "noop") df.write.format("noop").mode("overwrite").save()
  else df.write.mode("overwrite").parquet(s"$base/out/$name")
val icebergScan = scala.util.Try(Class.forName("org.apache.iceberg.spark.source.SparkBatchQueryScan").
  getProtectionDomain.getCodeSource.getLocation.toString).getOrElse("not-loadable")
println(s"PROBE sink=$sink conf=${sys.env.getOrElse("SPARK_CONF_DIR", "-")} extensions=${spark.conf.get("spark.sql.extensions", "-")} catalog=${spark.conf.get("spark.sql.catalog.spark_catalog", "-")} icebergScan=$icebergScan")
// The executed plan of each finished query, as Spark reports it after AQE has re-planned the stages.
val lastPlan = new java.util.concurrent.atomic.AtomicReference("")
spark.listenerManager.register(new QueryExecutionListener {
  override def onSuccess(f: String, qe: QueryExecution, d: Long): Unit = lastPlan.set(qe.executedPlan.toString)
  override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = lastPlan.set("FAILED " + e)
})
if (sys.env.contains("BENCH_GEN")) {
  val t0 = System.nanoTime
  val f = spark.range(0, sys.env("ROWS").toLong, 1, 240).selectExpr("id % 1000000 as k", "rand(42) * 1000 as v",
    "cast(id % 97 as string) as s", "date_add(date'2020-01-01', cast(id % 2000 as int)) as d")
  f.write.mode("overwrite").parquet(base + "/facts")
  val dm = spark.range(0, 1000000).selectExpr("id as k", "concat('name-', id) as name", "id % 50 as region")
  dm.write.mode("overwrite").parquet(base + "/dims")
  println(f"BENCH gen mode=$mode wall=${(System.nanoTime - t0) / 1e9}%.1fs")
}
val facts = spark.read.parquet(base + "/facts")
val dims = spark.read.parquet(base + "/dims")
val queries = Seq(
  "agg"    -> (() => facts.groupBy("k").agg(sum("v"), avg("v"), count("*"), countDistinct("s"))),
  "join"   -> (() => facts.join(dims, "k").groupBy("region", "s").agg(sum("v").as("t"), max("d"))),
  "sort"   -> (() => facts.orderBy(desc("v"), col("k"))),
  "window" -> (() => facts.withColumn("r", row_number().over(Window.partitionBy("k").orderBy(desc("v")))).where("r <= 3")),
  "filter" -> (() => facts.where("s in ('1','7','42') and v > 500").groupBy(year(col("d"))).agg(sum("v")))
)
for ((name, q) <- queries) {
  try {
    save(name, q())   // warm-up
    val t0 = System.nanoTime
    save(name, q())
    val wall = (System.nanoTime - t0) / 1e9
    Thread.sleep(3000)   // the listener bus is asynchronous
    val p = lastPlan.get
    val gpuOps = "Gpu[A-Za-z]+".r.findAllIn(p).toSeq.distinct.mkString(",")
    println(f"BENCH query=$name mode=$mode wall=$wall%.1fs finalPlan=${p.contains("isFinalPlan=true")} gpuOps=$gpuOps")
    if (name == "window") println("FINALPLAN\n" + p + "\nENDPLAN")
  } catch { case e: Throwable => println(s"BENCH query=$name mode=$mode FAILED ${e.toString.take(240)}") }
}
System.exit(0)
EOF

common=(--master yarn --deploy-mode client --conf spark.ui.showConsoleProgress=false
  --conf spark.executor.instances=3 --conf spark.executor.cores=12
  --conf spark.sql.shuffle.partitions=200 --conf spark.sql.files.maxPartitionBytes=512m)
gpu=(--jars $jar --files $dir/getGpusResources.sh
  --conf spark.plugins=com.nvidia.spark.SQLPlugin
  --conf spark.rapids.shims-provider-override=$provider
  --conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator
  --conf spark.rapids.sql.enabled=true --conf spark.rapids.sql.explain=NOT_ON_GPU
  --conf spark.rapids.sql.concurrentGpuTasks=2 --conf spark.rapids.memory.pinnedPool.size=2g
  --conf spark.executor.memory=16g --conf spark.executor.memoryOverhead=10g
  --conf spark.executor.resource.gpu.amount=1
  --conf spark.executor.resource.gpu.discoveryScript=./getGpusResources.sh
  --conf spark.task.resource.gpu.amount=0.08)
gpu+=(${GPU_CONF:-})
cpu=(--conf spark.executor.memory=22g --conf spark.executor.memoryOverhead=4g)

bench() {  # $1 = label, rest = spark args
  local label=$1; shift
  echo; echo "########## $label"
  timeout 3000 $spark_shell "${common[@]}" "$@" -i $dir/bench.scala < /dev/null > $dir/bench-$label.log 2>&1
  echo "exit=$?  (full log: $dir/bench-$label.log)"
  grep -E "^(PROBE|BENCH)" $dir/bench-$label.log | cut -c1-400
  echo "-- errors"
  grep -E "NoSuchMethod|NoClassDefFound|ClassCast|AbstractMethod|IncompatibleClass|Exception|error:|^\s*!" $dir/bench-$label.log \
    | grep -vE "^\s+at " | awk '!seen[$0]++' | cut -c1-260 | head -15
  echo "-- CPU fallback reasons (spark.rapids.sql.explain=NOT_ON_GPU)"
  grep -E "cannot run on GPU|will not work on GPU" $dir/bench-$label.log | sed -E 's/^\s+//' | sort | uniq -c | sort -rn | cut -c1-260 | head -15
}

# Generate the data once (on GPU), unless it is already there from an earlier run.
hdfs dfs -test -e /tmp/rapids-bench/dims/_SUCCESS < /dev/null || export BENCH_GEN=1
runs=${RUNS:-gpu cpu}
[[ " $runs " == *" gpu "* ]] && bench gpu "${gpu[@]}"
unset BENCH_GEN
[[ " $runs " == *" cpu "* ]] && bench cpu "${cpu[@]}"

echo; echo "== Final (post-AQE) window query plan on GPU"
awk '/^FINALPLAN/{p=1; next} /^ENDPLAN/{exit} p' $dir/bench-gpu.log | cut -c1-200 | head -40
echo; echo "== Summary (warm run, seconds)"
awk '/^BENCH query=.*wall=/ {
       match($0, /query=[a-z]+/); q = substr($0, RSTART + 6, RLENGTH - 6)
       match($0, /wall=[0-9.]+/); w = substr($0, RSTART + 5, RLENGTH - 5)
       if (FILENAME ~ /gpu/) g[q] = w; else c[q] = w
       if (!(q in seen)) { seen[q] = 1; order[++n] = q } }
     END { printf "%-8s %8s %8s %8s\n", "query", "gpu", "cpu", "speedup"
           for (i = 1; i <= n; i++) { q = order[i]
             printf "%-8s %8s %8s %8s\n", q, g[q], c[q], (g[q] && c[q]) ? sprintf("%.1fx", c[q] / g[q]) : "-" } }' \
  $(ls $dir/bench-gpu.log $dir/bench-cpu.log 2>/dev/null)
