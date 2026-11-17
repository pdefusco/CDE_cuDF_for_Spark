#!/usr/bin/env bash
# Patch the published RAPIDS jar for a Spark build whose MapPartitionsRDD constructor has Cloudera's
# extra isDeterministic: Option[Boolean] parameter (8 args in bytecode, as in 7.3.2's Spark 3.5.4).
# Without it every GPU plan fails on its first action with NoSuchMethodError MapPartitionsRDD.<init>.
# check_cluster.sh prints the constructor; scripts/check_spark_core.sh gives the verdict.
#
# This is the patcher stage of runtime/Dockerfile, run on the gateway instead of in an image (see
# runtime/README-patch.md for why a recompile is enough). The two RAPIDS classes that call the
# constructor are recompiled, from NVIDIA's source at the jar's own tag, against this host's
# spark-core, and replace their spark354/ copies. Nothing else in the jar changes. Unlike the
# Dockerfile, the spark-shared/ companions are left as published: neither calls MapPartitionsRDD, and
# GpuColumnToRowMapPartitionsRDD$ carries mapPartitions (the ColumnarRdd export path), which the
# standalone class compiled here does not.
#
# Run on a Spark 3 gateway host after plugin_load.sh (which downloads the jar). Needs a JDK (javap,
# jar), python3, and outbound access to GitHub and Maven Central. Writes the patched jar, under the
# published filename, to $OUT, so the other scripts use it when run with WORK=$OUT.
# Env: RAPIDS_VERSION (default 26.10.0), WORK (default /tmp/rapids), OUT (default /tmp/rapids-patched).
set -euo pipefail
# Spark 3 is the SPARK3 (CDS 3.5) parcel on 7.1.9 and part of the CDH parcel on 7.3.x.
spark=/opt/cloudera/parcels/SPARK3; [[ -d $spark ]] || spark=/opt/cloudera/parcels/CDH
jars=$spark/lib/spark3/jars
ver=${RAPIDS_VERSION:-26.10.0}
dir=${WORK:-/tmp/rapids}
out=${OUT:-/tmp/rapids-patched}
name=rapids-4-spark_2.12-$ver-cuda12.jar
[[ -s $dir/$name ]] || { echo "No $dir/$name; run plugin_load.sh first" >&2; exit 1; }
classes=(org/apache/spark/rapids/LocationPreservingMapPartitionsRDD
         org/apache/spark/sql/rapids/execution/GpuColumnToRowMapPartitionsRDD)
build=$(mktemp -d); trap 'rm -rf $build' EXIT
mkdir -p $build/src $build/classes $build/orig $build/stage $out

echo "== Spark: $(ls $jars/spark-core_2.12-*.jar | head -1 | xargs basename)"

echo "== Sources from NVIDIA/spark-rapids v$ver"
raw=https://raw.githubusercontent.com/NVIDIA/spark-rapids/v$ver/sql-plugin/src/main/scala
curl -sSfL -o $build/src/LocationPreservingMapPartitionsRDD.scala \
  $raw/com/nvidia/spark/rapids/LocationPreservingMapPartitionsRDD.scala
# The second class sits in a file that drags in most of the plugin, so compile the class on its own,
# with only the imports it needs (as runtime/patch/GpuColumnToRowMapPartitionsRDD.scala does).
curl -sSfL -o $build/converter.scala \
  $raw/org/apache/spark/sql/rapids/execution/InternalColumnarRddConverter.scala
{
  printf '%s\n' 'package org.apache.spark.sql.rapids.execution' '' \
    'import org.apache.spark.TaskContext' 'import org.apache.spark.rdd.{MapPartitionsRDD, RDD}' \
    'import org.apache.spark.sql.catalyst.InternalRow' 'import org.apache.spark.sql.vectorized.ColumnarBatch' ''
  sed -n '/^class GpuColumnToRowMapPartitionsRDD(/,/^}/p' $build/converter.scala
} > $build/src/GpuColumnToRowMapPartitionsRDD.scala
grep -q 'extends MapPartitionsRDD' $build/src/GpuColumnToRowMapPartitionsRDD.scala \
  || { echo "FATAL: GpuColumnToRowMapPartitionsRDD not found in InternalColumnarRddConverter.scala" >&2; exit 1; }
wc -l $build/src/*.scala | sed '$d'

echo "== Scala compiler"
# The compiler must match the scala-library Spark ships. Use the parcel's if it has one.
scala_ver=$(ls $jars | sed -nE 's/^scala-library-([0-9.]+)\.jar$/\1/p' | head -1)
[[ -n $scala_ver ]] || { echo "FATAL: no scala-library-<version>.jar in $jars" >&2; exit 1; }
scala_cp=
for a in scala-compiler scala-reflect; do
  j=$(ls $jars/$a-$scala_ver.jar 2>/dev/null || true)
  if [[ -z $j ]]; then
    j=$dir/scalac/$a-$scala_ver.jar
    mkdir -p $dir/scalac
    [[ -s $j ]] || curl -sSfL -o $j https://repo1.maven.org/maven2/org/scala-lang/$a/$scala_ver/$a-$scala_ver.jar
  fi
  scala_cp+=$j:
done
scala_cp+=$jars/scala-library-$scala_ver.jar
echo "$scala_ver: ${scala_cp//:/ }"

echo "== Compile"
# jvm-1.8, because every class in the jar is major version 52; mixing in newer classes would be a
# second, self-inflicted linkage problem.
java -cp "$scala_cp" scala.tools.nsc.Main -target:jvm-1.8 \
  -classpath "$(ls $jars/*.jar | tr '\n' ':')" -d $build/classes $build/src/*.scala
python3 - $dir/$name $build/orig "${classes[@]}" <<'EOF'
import os, sys, zipfile
jar, dest, names = sys.argv[1], sys.argv[2], sys.argv[3:]
z = zipfile.ZipFile(jar)
for n in names:
    p = os.path.join(dest, n + '.class')
    os.makedirs(os.path.dirname(p), exist_ok=True)
    open(p, 'wb').write(z.read('spark354/' + n + '.class'))
EOF

echo "== Check the recompiled classes"
for c in "${classes[@]}"; do
  cls=${c//\//.}
  # Asserted, not assumed: on a 7-arg Spark scalac emits the 7-arg call again and the patch would be
  # a silent no-op.
  javap -c -p -cp $build/classes $cls \
    | grep -qE 'MapPartitionsRDD\."<init>":\(Lorg/apache/spark/rdd/RDD;Lscala/Function3;ZZZLscala/Option;' \
    || { echo "FATAL: $cls does not call the 8-arg MapPartitionsRDD constructor" >&2; exit 1; }
  # The class's own constructor must keep its signature, because the published companions call it.
  sig() { javap -s -p -cp $1 $cls | grep -A1 " $cls(" | sed -n 's/^ *descriptor: //p'; }
  [[ "$(sig $build/orig)" == "$(sig $build/classes)" ]] \
    || { echo "FATAL: $cls constructor changed: $(sig $build/orig) -> $(sig $build/classes)" >&2; exit 1; }
  major() { od -An -j6 -N2 -tu1 $1/$c.class | awk '{print $1 * 256 + $2}'; }
  [[ $(major $build/orig) == $(major $build/classes) ]] \
    || { echo "FATAL: $cls class version $(major $build/orig) -> $(major $build/classes)" >&2; exit 1; }
  echo "8-arg call, major $(major $build/classes), constructor $(sig $build/classes)  $cls"
  mkdir -p $build/stage/spark354/$(dirname $c)
  cp $build/classes/$c.class $build/stage/spark354/$c.class
done

echo "== Write $out/$name"
cp $dir/$name $out/$name.tmp
(cd $build/stage && jar uf $out/$name.tmp $(printf 'spark354/%s.class ' "${classes[@]}"))
# Directory entries are skipped: they have no content, and jar may add any the original lacks.
python3 - $dir/$name $out/$name.tmp <<'EOF'
import sys, zipfile
a, b = ({i.filename: i.CRC for i in zipfile.ZipFile(p).infolist() if not i.is_dir()} for p in sys.argv[1:])
changed = sorted(n for n in a.keys() | b.keys() if a.get(n) != b.get(n))
print('\n'.join(changed))
want = {'spark354/org/apache/spark/rapids/LocationPreservingMapPartitionsRDD.class',
        'spark354/org/apache/spark/sql/rapids/execution/GpuColumnToRowMapPartitionsRDD.class'}
if set(changed) != want:
    sys.exit('FATAL: expected exactly the 2 spark354 entries to change, got %d' % len(changed))
print('%d entries, 2 changed' % len(b))
EOF
mv $out/$name.tmp $out/$name
ls -l $out/$name | awk '{print $5, $NF}'
