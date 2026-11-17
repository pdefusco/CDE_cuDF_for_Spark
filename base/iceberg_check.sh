#!/usr/bin/env bash
# Can a RAPIDS session keep the stock Spark 3 client config, Iceberg included? Runs Iceberg and Hive
# (metastore) table writes and reads with the plugin, once with spark.rapids.sql.suppressPlanningFailure
# =true and once without, and prints which operators of each final (post-AQE) plan ran on the GPU, plus
# exact checksums to compare against the same rows read straight from Parquet.
# Run on a Spark 3 gateway host after plugin_load.sh and benchmark.sh (it reads the benchmark's data in
# /tmp/rapids-bench), as a user with a Kerberos ticket. Env: RAPIDS_VERSION (default 26.10.0), WORK
# (default /tmp/rapids), SUPPRESS_RUNS (default "true false"), DB (default rapids_test; dropped tables
# only, the database is kept), DB_LOCATION (default /tmp/rapids-bench/rapids_test.db).
set -u
# Spark 3 is the SPARK3 (CDS 3.5) parcel on 7.1.9 and part of the CDH parcel on 7.3.x.
spark_shell=$(command -v spark3-shell || command -v spark-shell)
dir=${WORK:-/tmp/rapids}
jar=$dir/rapids-4-spark_2.12-${RAPIDS_VERSION:-26.10.0}-cuda12.jar
provider=com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider
export DB=${DB:-rapids_test} DB_LOCATION=${DB_LOCATION:-/tmp/rapids-bench/rapids_test.db}
unset SPARK_CONF_DIR
cd $dir

cat > $dir/iceberg.scala <<'EOF'
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
val db = sys.env("DB")
println(s"PROBE suppress=${spark.conf.get("spark.rapids.sql.suppressPlanningFailure", "false")} extensions=${spark.conf.get("spark.sql.extensions", "-")} catalog=${spark.conf.get("spark.sql.catalog.spark_catalog", "-")}")
val lastPlan = new java.util.concurrent.atomic.AtomicReference("")
spark.listenerManager.register(new QueryExecutionListener {
  override def onSuccess(f: String, qe: QueryExecution, d: Long): Unit = lastPlan.set(qe.executedPlan.toString)
  override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = lastPlan.set("FAILED " + e)
})
// Runs one step; its plan is the last query it executed.
def step(name: String)(body: => Any): Unit = {
  try {
    lastPlan.set("")
    val t0 = System.nanoTime
    val r = body
    val wall = (System.nanoTime - t0) / 1e9
    Thread.sleep(3000)   // the listener bus is asynchronous
    val p = lastPlan.get
    val gpuOps = "Gpu[A-Za-z]+".r.findAllIn(p).toSeq.distinct.mkString(",")
    println(f"ICE step=$name wall=$wall%.1fs result=${r.toString.take(120)} gpuOps=$gpuOps")
    println(s"PLAN $name\n$p\nENDPLAN")
  } catch { case e: Throwable => println(s"ICE step=$name FAILED ${e.toString.take(300)}") }
}
// Exact checksum of a table's rows: row count and sum of the long key, through a shuffle.
def checksum(t: org.apache.spark.sql.DataFrame): String =
  t.groupBy("s").agg(count("*").as("n"), sum("k").as("sk")).agg(sum("n"), sum("sk")).collect().mkString
spark.sql(s"CREATE DATABASE IF NOT EXISTS $db LOCATION '${sys.env("DB_LOCATION")}'")
val sample = spark.read.parquet("/tmp/rapids-bench/facts").where("k < 100000")   // 30M of the 300M rows
step("parquet-read-agg") { checksum(sample) }
step("iceberg-create") {
  spark.sql(s"DROP TABLE IF EXISTS $db.ice PURGE")
  sample.writeTo(s"$db.ice").using("iceberg").create(); "ok" }
step("iceberg-provider") { spark.sql(s"DESCRIBE TABLE EXTENDED $db.ice").where("col_name = 'Provider'").collect().mkString }
step("iceberg-read-agg") { checksum(spark.table(s"$db.ice")) }
step("iceberg-append") { sample.limit(1000).writeTo(s"$db.ice").append(); "ok" }
step("iceberg-count") { spark.table(s"$db.ice").count() }
step("hive-create") {
  spark.sql(s"DROP TABLE IF EXISTS $db.hv PURGE")
  sample.write.format("parquet").saveAsTable(s"$db.hv"); "ok" }
step("hive-provider") { spark.sql(s"DESCRIBE TABLE EXTENDED $db.hv").where("col_name = 'Provider'").collect().mkString }
step("hive-read-agg") { checksum(spark.table(s"$db.hv")) }
step("hive-insert") { sample.limit(1000).write.insertInto(s"$db.hv"); "ok" }
step("hive-count") { spark.table(s"$db.hv").count() }
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

for s in ${SUPPRESS_RUNS:-true false}; do
  log=$dir/iceberg-suppress-$s.log
  echo; echo "########## suppressPlanningFailure=$s"
  timeout 1800 $spark_shell "${common[@]}" "${gpu[@]}" --conf spark.rapids.sql.suppressPlanningFailure=$s \
    -i $dir/iceberg.scala < /dev/null > $log 2>&1
  echo "exit=$?  (full log: $log)"
  grep -E "^(PROBE|ICE)" $log | cut -c1-400
  echo "-- planning failures"
  grep -oE "Failed to apply GPU overrides.*|Encountered an exception applying GPU overrides.*" $log \
    | cut -c1-200 | sort | uniq -c
  echo "-- other errors"
  grep -E "NoSuchMethod|NoClassDefFound|ClassCast|IllegalAccess|AbstractMethod|Exception|error:" $log \
    | grep -vE "^\s+at |GPU overrides|Unrecognized Iceberg version" | awk '!seen[$0]++' | cut -c1-260 | head -10
  for p in iceberg-create iceberg-read-agg iceberg-append; do
    echo "-- final plan: $p"
    awk -v p="PLAN $p" '$0 == p {f=1; next} /^ENDPLAN/ {f=0} f' $log | grep -vE "^\s*$" | cut -c1-170 | head -25
  done
done
