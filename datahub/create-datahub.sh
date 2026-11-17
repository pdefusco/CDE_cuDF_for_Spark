#!/usr/bin/env bash
# Data Hub 7.3.2 Data Engineering with a GPU compute group, on AWS.
#   ENV_NAME=<environment> CLUSTER_NAME=<name> datahub/create-datahub.sh
# No --image: the default 7.3.2 image of the cdp-default catalog. p4d.24xlarge in the GPU groups,
# the only GPU family Data Hub validates (p4d, p4de). Both p4d groups start at 0 nodes: the image
# has no NVIDIA driver, so the driver recipe is attached first, then compute is scaled to 1
# (~$22/hr per p4d.24xlarge on demand). The gateway host template has no NodeManager: keep it at 0.
set -euo pipefail
: "${ENV_NAME:?set ENV_NAME to the CDP environment name}"
CLUSTER_NAME=${CLUSTER_NAME:-dh732-gpu}
vol() { echo "attachedVolumeConfiguration=[{volumeSize=$1,volumeCount=1,volumeType=$2}],recoveryMode=MANUAL,volumeEncryption={enableEncryption=true}"; }

cdp datahub create-aws-cluster \
  --cluster-name "$CLUSTER_NAME" \
  --environment-name "$ENV_NAME" \
  --cluster-template-name "7.3.2 - Data Engineering: Apache Spark3, Apache Hive, Apache Oozie" \
  --instance-groups \
    "nodeCount=1,instanceGroupName=master,instanceGroupType=GATEWAY,instanceType=m5.4xlarge,rootVolumeSize=300,$(vol 100 gp3)" \
    "nodeCount=3,instanceGroupName=worker,instanceGroupType=CORE,instanceType=r5d.2xlarge,rootVolumeSize=200,$(vol 100 gp3)" \
    "nodeCount=0,instanceGroupName=compute,instanceGroupType=CORE,instanceType=p4d.24xlarge,rootVolumeSize=200,$(vol 300 standard)" \
    "nodeCount=0,instanceGroupName=gateway,instanceGroupType=CORE,instanceType=p4d.24xlarge,rootVolumeSize=200,$(vol 100 gp3)" \
  --no-multi-az \
  --no-enable-load-balancer
