#!/usr/bin/env bash
# Is YARN advertising GPUs, and what exactly is in the Spark 3 parcel?
# Run on a Spark 3 gateway host, as a user with a Kerberos ticket (kinit first).
set -u
# Spark 3 is the SPARK3 (CDS 3.5) parcel on 7.1.9 and part of the CDH parcel on 7.3.x.
spark=/opt/cloudera/parcels/SPARK3; [[ -d $spark ]] || spark=/opt/cloudera/parcels/CDH
spark_conf=/etc/spark3/conf; [[ -d $spark_conf ]] || spark_conf=/etc/spark/conf
spark_submit=$(command -v spark3-submit || command -v spark-submit)

echo "== klist"; klist | sed -n '1,3p'

echo "== YARN nodes (expect yarn.io/gpu on each GPU NodeManager)"
yarn node -list -showDetails 2>/dev/null | grep -E "Node-Id|Total-Nodes|Resource|yarn.io/gpu|memory-mb|vcores"

echo "== Parcels"
ls -1 /opt/cloudera/parcels

# The parcel's own version is in its directory name; parcel.json's first "version" is a component's.
echo "== Spark 3 parcel"
basename "$(readlink -f $spark)"
echo "== $(basename $spark_submit) --version"
$spark_submit --version 2>&1 | grep -vE "^\s*$" | head -12
echo "== Key jars"
ls -1 $spark/lib/spark3/jars | grep -E "^(spark-(core|sql|catalyst)_|scala-library|hadoop-client|parquet-hadoop|arrow-vector|hive-exec)"

# 7 args is Apache's constructor, which the published RAPIDS jar links against. Cloudera's 7.3.2
# Spark line has 8 (an added isDeterministic), and needs the patched jar: base/patch_rapids_jar.sh on
# Base (README-732-base.md), a custom runtime image on CDE (README-732.md).
echo "== MapPartitionsRDD constructor"
if command -v javap >/dev/null; then
  # The parcel also ships an unversioned spark-core_2.12.jar alias, so take one jar for the classpath.
  javap -cp "$(ls $spark/lib/spark3/jars/spark-core_*.jar | head -1)" -p org.apache.spark.rdd.MapPartitionsRDD | grep "MapPartitionsRDD("
else
  echo "javap not found (JRE only); install a JDK, or run the javap in README-719.md §8 against the jar"
fi

echo "== Iceberg in the Spark 3 client config (RAPIDS 26.10 fails DSv2 queries while it is there)"
grep -rhoiE "[^:,\"= ]*iceberg[^:,\"' ]*" $spark_conf/ 2>/dev/null | sort -u | cut -c1-200

echo "== Anything RAPIDS/GPU-related in the parcel"
find $spark -maxdepth 4 \( -iname '*rapids*' -o -iname '*cudf*' -o -iname '*gpu*' \) 2>/dev/null | head -20
echo "== Java"
java -version 2>&1 | head -1
echo "== Outbound to Maven Central"
curl -sS -o /dev/null -w '%{http_code}\n' --max-time 10 https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/maven-metadata.xml
