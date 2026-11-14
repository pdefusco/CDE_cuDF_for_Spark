#!/usr/bin/env bash
# Does the published RAPIDS jar load on Cloudera's Spark 3.5.4 build as-is, and if not, does forcing
# the spark354 shim work? Client mode, so the driver log (ShimLoader output) lands here.
# Run on a Spark 3 gateway host, as a user with a Kerberos ticket and an HDFS home directory.
# Env: RAPIDS_VERSION (default 26.10.0), WORK (default /tmp/rapids).
set -u
ver=${RAPIDS_VERSION:-26.10.0}
dir=${WORK:-/tmp/rapids}
jar=$dir/rapids-4-spark_2.12-$ver-cuda12.jar
mkdir -p $dir && cd $dir

echo "== Download $(basename $jar)"
[[ -s $jar ]] || curl -sSfL -o $jar https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/$ver/$(basename $jar)
ls -l $jar | awk '{print $5, $NF}'

echo "== Spark 3.5 shims in the jar"
python3 -c 'import sys, zipfile; print("\n".join(zipfile.ZipFile(sys.argv[1]).namelist()))' $jar > $dir/jar.lst
grep -oE '^spark3[0-9]+[a-z]*/' $dir/jar.lst | sort -u | tr -d '/' | paste -sd' '
# Providers sit outside the per-shim dirs (they are loaded before a shim is chosen), so match anywhere.
grep -E 'shims/spark354/SparkShimServiceProvider\.class$' $dir/jar.lst
provider=$(grep -E 'shims/spark354/SparkShimServiceProvider\.class$' $dir/jar.lst | head -1 \
  | sed -E 's#^(spark3[0-9]+[a-z]*|spark-shared)/##; s#\.class$##; s#/#.#g')
echo "spark354 provider: ${provider:-NOT FOUND}"
[[ -n "$provider" ]] || grep -E 'SparkShimServiceProvider' $dir/jar.lst | head -10

# YARN allocates the GPU; Spark still needs a discovery script to learn its address.
cat > $dir/getGpusResources.sh <<'EOF'
#!/usr/bin/env bash
ADDRS=$(nvidia-smi --query-gpu=index --format=csv,noheader | sed -e ':a' -e 'N' -e'$!ba' -e 's/\n/","/g')
echo {\"name\": \"gpu\", \"addresses\":[\"$ADDRS\"]}
EOF
chmod +x $dir/getGpusResources.sh

cat > $dir/smoke.scala <<'EOF'
import org.apache.spark.sql.functions._
val t0 = System.nanoTime
val df = spark.range(0, 50000000L).selectExpr("id % 1000 as k", "id as v", "cast(id % 7 as string) as s")
df.write.mode("overwrite").parquet("/tmp/rapids-smoke/facts")
val facts = spark.read.parquet("/tmp/rapids-smoke/facts")
val dims = spark.range(0, 1000).selectExpr("id as k", "concat('d', id) as name")
val q = facts.join(dims, "k").groupBy("name", "s").agg(sum("v").as("total"), count("*").as("n")).orderBy(desc("total"))
q.show(5, false)
println(f"SMOKE wall=${(System.nanoTime - t0) / 1e9}%.1fs plugins=${spark.conf.getOption("spark.plugins")}")
System.exit(0)
EOF

run() {  # $1 = label, rest = extra --conf args
  local label=$1; shift
  echo; echo "########## $label"
  timeout 900 spark3-shell --master yarn --deploy-mode client --conf spark.ui.showConsoleProgress=false \
    --jars $jar --files $dir/getGpusResources.sh \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.explain=NOT_ON_GPU \
    --conf spark.executor.instances=3 \
    --conf spark.executor.cores=8 \
    --conf spark.executor.memory=16g \
    --conf spark.executor.memoryOverhead=8g \
    --conf spark.rapids.memory.pinnedPool.size=2g \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.executor.resource.gpu.discoveryScript=./getGpusResources.sh \
    --conf spark.task.resource.gpu.amount=0.125 \
    --conf spark.sql.files.maxPartitionBytes=512m \
    "$@" -i $dir/smoke.scala < /dev/null > $dir/$label.log 2>&1
  echo "exit=$?  (full log: $dir/$label.log)"
  grep -E "RAPIDS|Rapids|[Ss]him|cudf|SMOKE|!Exec|!Expression|cannot run on GPU|NoSuchMethod|NoClassDefFound|ClassCast|AbstractMethod|IncompatibleClass|UnsupportedOperation|IllegalArgument|Exception|Error" \
    $dir/$label.log | grep -vE "^\s+at " | awk '!seen[$0]++' | cut -c1-260 | head -60
}

echo "== Serializer settings from the Spark 3 client config"
grep -hE 'serializer|kryo' /etc/spark3/conf/spark-defaults.conf || echo "(none)"
# Cloudera's Spark 3 defaults to Kryo, and RAPIDS refuses to start under Kryo without its registrator.
# Append to any registrator CM already sets rather than replacing it.
existing=$(sed -nE 's/^spark\.kryo\.registrator=(.*)/\1/p' /etc/spark3/conf/spark-defaults.conf)
kryo=com.nvidia.spark.rapids.GpuKryoRegistrator${existing:+,$existing}

# The as-is result does not change between runs, so only repeat it if it has not already failed.
grep -q "does not support Spark build" $dir/as-is.log 2>/dev/null || run as-is
grep -m1 -A2 "does not support Spark build" $dir/as-is.log | cut -c1-600
if ! grep -q "^SMOKE wall=" $dir/as-is.log && [[ -n "$provider" ]]; then
  run shim-override --conf spark.rapids.shims-provider-override=$provider --conf spark.kryo.registrator=$kryo
fi
