#!/usr/bin/env bash
# When the GPU run fails or prints no BENCH lines: what the Spark 3 client config loads, where the
# Iceberg jars are, the RAPIDS lines and Iceberg stack from the logs, and the tail of each log.
# Run on the gateway host that ran plugin_load.sh / benchmark.sh. Env: WORK (default /tmp/rapids).
set -u
# Spark 3 is the SPARK3 (CDS 3.5) parcel on 7.1.9 and part of the CDH parcel on 7.3.x.
spark=/opt/cloudera/parcels/SPARK3; [[ -d $spark ]] || spark=/opt/cloudera/parcels/CDH
spark_conf=/etc/spark3/conf; [[ -d $spark_conf ]] || spark_conf=/etc/spark/conf
dir=${WORK:-/tmp/rapids}
clean() { sed -E 's/\r/\n/g' "$1" | grep -vE "^\s*$|\[Stage [0-9]+:" ; }

echo "== Spark 3 client defaults touching extensions/plugins/iceberg/catalog"
grep -hiE "extensions|plugins|iceberg|catalog|adaptive" $spark_conf/spark-defaults.conf | cut -c1-300
echo "== Iceberg jars (the ones on the Spark 3 classpath are under $(basename $spark)/lib/iceberg)"
find $spark/lib/ /opt/cloudera/parcels/CDH/jars/ -name 'iceberg-*.jar' 2>/dev/null | sort | head -20

for f in shim-override bench-gpu; do
  echo; echo "################ $f.log"
  echo "== RAPIDS / extension lines"
  clean $dir/$f.log | grep -E "SQLExecPlugin|spark.sql.extensions|GpuOverride|rapids\.[A-Za-z]+:|will not work on GPU|cannot run on GPU|^\s*[!*@#]" | awk '!seen[$0]++' | cut -c1-300 | head -25
  echo "== Iceberg stack (first frames)"
  clean $dir/$f.log | grep -m1 -A14 "Unrecognized Iceberg version" | cut -c1-200
done

echo; echo "################ bench-gpu.log: last 50 lines"
clean $dir/bench-gpu.log | tail -50 | cut -c1-250
echo; echo "################ bench-cpu.log: last 30 lines"
clean $dir/bench-cpu.log | tail -30 | cut -c1-250
echo; echo "== HDFS bench data"
hdfs dfs -du -s -h /tmp/rapids-bench/facts /tmp/rapids-bench/dims < /dev/null 2>&1
