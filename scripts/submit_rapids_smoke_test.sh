#!/usr/bin/env bash
# Submit the RAPIDS smoke test to CDE on the custom runtime image.
#
# Prerequisites, all of which cost a separate discovery if missed:
#   1. The custom runtime resource exists:
#        cde resource create --name cde-rapids-runtime --type custom-runtime-image \
#          --image pauldefusco/cde-rapids-runtime-3.5.4:26.02.0 --image-engine spark3
#   2. The CDE user holds a **DE resource role**. Authentication succeeding is not enough: with no DE
#      role every call returns `authorization policy denies` and dex-authz logs
#      "user: <name> does not have any DE roles". Fix is an IAM grant, not a VC ACL change:
#        cdp iam assign-user-resource-role --user <userCrn> \
#          --resource-role-crn crn:altus:iam:us-west-1:altus:resourceRole:DEAdmin \
#          --resource-crn <environmentCrn>
#   3. The VC was created with `dexapp.api.gpuAcceleration.enabled:true` (create-time only).
#
# Sizing here is deliberately smaller than docs/rapids-spark-conf.md: that block is tuned for a
# 250M-row ETL, this is a smoke test whose only job is to prove Gpu* operators appear in the plan.
set -euo pipefail

PROFILE="${CDE_PROFILE:-ds0928}"
RUNTIME_RESOURCE="${RUNTIME_RESOURCE:-cde-rapids-runtime}"
JOB_FILE="${JOB_FILE:-jobs/rapids_smoke_test.py}"

# In-image paths. These are fixed by runtime/Dockerfile — change them in both places or not at all.
RAPIDS_JAR=/opt/spark/jars/rapids-4-spark_2.12-26.02.0-cuda12.jar
DISCOVERY=/opt/cde/gpu/getGpusResources.sh

# Sizing goes through CDE's OWN flags, not --conf. CDE sets spark.driver.memory / spark.executor.memory
# / spark.executor.cores from its job spec *after* merging --conf, so `--conf spark.executor.memory=8g`
# is silently replaced by the spec default of 1g. Run 3 proved it: the submitted command line showed
# `spark.executor.memory=1g` despite the conf. Confs CDE has no spec field for (memoryOverhead,
# extraClassPath, the rapids.* keys) survive — and extraClassPath is appended to, not overwritten.
#
# --driver-memory is not cosmetic here. At the 1g default the driver JVM, the RAPIDS plugin and the
# PySpark Python process share a ~2 GiB pod cap; the driver is then cgroup-OOM-killed, which leaves
# NO Java stack trace — the log simply stops mid-stream. That is what killed run 3.
#
# Note: the CLI flag is --config-profile, not --profile.
exec cde --config-profile "$PROFILE" spark submit "$JOB_FILE" \
  --runtime-image-resource-name "$RUNTIME_RESOURCE" \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  \
  `# --- plugin and jar. extraClassPath matters because RapidsShuffleManager must resolve while` \
  `# the executor is still starting up, before the normal jar list is in play. ---` \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.shims-provider-override=com.nvidia.spark.rapids.shims.spark354.SparkShimServiceProvider \
  --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark354.RapidsShuffleManager \
  --conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator \
  --conf spark.driver.extraClassPath="$RAPIDS_JAR" \
  --conf spark.executor.extraClassPath="$RAPIDS_JAR" \
  \
  `# --- GPU resource wiring. vendor must be nvidia.com to match the k8s device-plugin name. ---` \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=0.25 \
  --conf spark.executor.resource.gpu.vendor=nvidia.com \
  --conf spark.executor.resource.gpu.discoveryScript="$DISCOVERY" \
  \
  `# --- sizing: 1 executor is enough to prove the plan went to the GPU. The cluster total is 3` \
  `# GPUs (1 per worker, sharing-strategy=none), so this leaves headroom. memoryOverhead is large` \
  `# on purpose: the pinned pool and host spill store are off-heap and count against the pod cap. ---` \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.executor.instances=1 \
  --conf spark.executor.memoryOverhead=6g \
  --conf spark.rapids.memory.pinnedPool.size=2g \
  \
  `# --- diagnostic, not tuning: makes RAPIDS print one line per operator it declined, naming the` \
  `# cause. This is the only thing that explains a silent CPU fallback. Unset once green. ---` \
  --conf spark.rapids.sql.explain=NOT_ON_GPU \
  "$@"
