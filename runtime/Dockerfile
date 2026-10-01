# CDE custom runtime: the stock CDE Spark 3.5.4 runtime + the OSS Spark RAPIDS jar, with two
# classes inside that jar recompiled against this cluster's own Spark. See README-patch.md for the
# full reasoning; the short version is in the `patcher` stage below.
#
# Base tag matches the cluster exactly: CDE 1.26.0-b557, Spark 3.5.4, Runtime 7.3.2.0. A custom
# runtime must stay on the CDE runtime base — that is what makes it a legal `custom-runtime-image`
# resource. Do not rebase this on an NVIDIA CUDA image: the OSS rapids-4-spark jar already carries
# the CUDA libraries, which is the whole reason this is a COPY and not a toolkit install.
#
# The base reports BASE_IMAGE_KIND=chainguard, SPARK_HOME=/opt/spark, DEX_UID=1345.
#
# The build host is arm64 Apple Silicon and the workers are amd64 RHEL 9.6, so this is always built
# with `--platform linux/amd64`. The patcher stage runs scalac under emulation — it compiles two
# small files, so that costs seconds, not minutes.

ARG BASE=docker-private.infra.cloudera.com/cloudera/dex/dex-spark-runtime-3.5.4-7.3.2.0:1.26.0-b557

# ---------------------------------------------------------------------------------------------
# Stage 1: patch the RAPIDS jar.
#
# OSS rapids-4-spark 26.02.0 cannot link against Cloudera's Spark 3.5.4.1.26.732.0-45: Cloudera
# added a sixth constructor parameter to org.apache.spark.rdd.MapPartitionsRDD
# (`isDeterministic: Option[Boolean]`), so every GPU query died at the first action with
# NoSuchMethodError from GpuExec.doExecuteColumnar.
#
# Exactly two classes in the whole 17,833-class jar call that constructor, and because the new
# parameter is *defaulted*, their unmodified upstream source recompiles against Cloudera's jar and
# emits the 8-arg call on its own. So this stage changes no RAPIDS logic whatsoever — it only
# re-targets two thin MapPartitionsRDD subclasses at the Spark actually present.
#
# This is why the base image is the right place to compile: it already ships
# scala-compiler-2.12.19.jar AND the exact spark-core jar to compile against.
FROM ${BASE} AS patcher

USER root
WORKDIR /build

COPY rapids-4-spark_2.12-26.02.0-cuda12.jar ./rapids.jar
COPY patch/*.scala ./src/

# The jar lays classes out per shim and the synthetic default-arg companions in spark-shared, so
# replacements must go back to those same two prefixes or the RAPIDS classloader will not find them.
#   spark354/    <- the classes
#   spark-shared/ <- the `$` default-arg holders
#
# Target jvm-1.8 because every existing class in the jar is major version 52; mixing in major 61
# classes would be a second, self-inflicted linkage problem.
#
# The `grep -q` at the end is a build-time assertion, not decoration: if a future base image drops
# the extra parameter, scalac silently emits the 7-arg call again and the patch becomes a no-op that
# would otherwise fail opaquely at runtime. Fail the build here instead.
RUN set -eu; \
    SCALA="/opt/spark/jars/scala-compiler-2.12.19.jar:/opt/spark/jars/scala-library-2.12.19.jar:/opt/spark/jars/scala-reflect-2.12.19.jar"; \
    CP="$(ls /opt/spark/jars/*.jar | tr '\n' ':')"; \
    mkdir -p classes stage/spark354 stage/spark-shared; \
    java -cp "$SCALA" scala.tools.nsc.Main \
        -target:jvm-1.8 -classpath "$CP" -d classes src/*.scala; \
    \
    for c in org.apache.spark.rapids.LocationPreservingMapPartitionsRDD \
             org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD; do \
      javap -c -p -classpath classes "$c" \
        | grep -qE 'MapPartitionsRDD\."<init>":\(Lorg/apache/spark/rdd/RDD;Lscala/Function3;ZZZLscala/Option;' \
        || { echo "FATAL: $c did not compile to the 8-arg MapPartitionsRDD ctor" >&2; exit 1; }; \
    done; \
    \
    for p in org/apache/spark/rapids/LocationPreservingMapPartitionsRDD \
             org/apache/spark/sql/rapids/execution/GpuColumnToRowMapPartitionsRDD; do \
      mkdir -p "stage/spark354/$(dirname "$p")" "stage/spark-shared/$(dirname "$p")"; \
      cp "classes/$p.class"  "stage/spark354/$p.class"; \
      cp "classes/$p\$.class" "stage/spark-shared/$p\$.class"; \
    done; \
    \
    jar uf rapids.jar -C stage .; \
    unzip -l rapids.jar | grep -cE 'spark(354|-shared)/.*(LocationPreserving|GpuColumnToRow)MapPartitionsRDD' \
      | grep -qx 4 || { echo "FATAL: expected 4 patched entries in the jar" >&2; exit 1; }; \
    echo "patched rapids.jar OK"

# ---------------------------------------------------------------------------------------------
# Stage 2: the runtime image. Only the patched jar crosses over, so the unpatched 887 MB copy
# never lands in a layer here.
FROM ${BASE}

# The base runs as uid 1345 (sparkuser) and /opt/spark is not writable by it.
USER root

# /opt/spark/jars, NOT /opt/spark/optional-lib. optional-lib (HWC, Iceberg) is not on the default
# classpath — it is opted into per job. RapidsShuffleManager must be resolvable while the executor
# is still starting up, so the jar has to be somewhere unconditionally on the classpath.
#
# Filename deliberately unchanged from the OSS artifact so the --conf extraClassPath lines in
# scripts/submit_rapids_smoke_test.sh stay valid. It is NOT the OSS artifact byte-for-byte any
# more — see README-patch.md.
COPY --from=patcher /build/rapids.jar /opt/spark/jars/rapids-4-spark_2.12-26.02.0-cuda12.jar

# Our own discovery script rather than the stock /opt/spark/examples/src/main/scripts/
# getGpusResources.sh that ships in the base: this one emits a valid empty address list when
# nvidia-smi is missing, so a runtime misconfiguration surfaces as "0 GPUs" instead of a JSON
# parse error that names nothing.
COPY getGpusResources.sh /opt/cde/gpu/getGpusResources.sh

RUN chmod 0755 /opt/cde/gpu/getGpusResources.sh \
 && chown -R 1345:0 /opt/cde/gpu \
 && chown 1345:0 /opt/spark/jars/rapids-4-spark_2.12-26.02.0-cuda12.jar

# Back to the CDE uid. CDE rejects a runtime image that would run as root.
USER 1345
