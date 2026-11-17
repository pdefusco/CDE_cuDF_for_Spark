# GPU-accelerated Spark on Data Hub: Runtime 7.3.2, CDP Public Cloud (work in progress)

← [Overview and other versions](README.md)

> **Status: work in progress, blocked, and largely untested.** The Data Hub has been created and
> the driver recipe attached. Turning on YARN GPU scheduling failed: Cloudera Manager needs a host
> setting that an environment owner's Cloudera Manager role can't change (§6). No GPU node has been
> started, so the recipe has never run, and no RAPIDS query has run. Each step below says whether
> it has been run.

This guide is the Data Hub version of the [Base 7.3.2 guide](README-732-base.md): Spark 3 on YARN,
with GPU executors on a Data Hub compute group, using the open-source [NVIDIA RAPIDS Accelerator
for Apache Spark](https://nvidia.github.io/spark-rapids/). On Base, the hosts and Cloudera Manager
are yours. On Data Hub they are provisioned for you, which changes three things:

- **Only the p4d family is accepted for GPUs.** Data Hub validates `p4d.24xlarge` and
  `p4de.24xlarge` (8 × NVIDIA A100). `g5` (A10G), used in every other setup in this repo, fails
  validation. A request to support g5 is open with the Data Hub team.
- **Data Hub sets up nothing for GPUs.** The runtime image has no NVIDIA driver or CUDA, the 7.3.2
  Data Engineering template doesn't turn on YARN GPU scheduling (`yarn.io/gpu`), and provisioning
  installs nothing GPU-related. You install the driver with a recipe and turn on GPU scheduling in
  Cloudera Manager yourself.
- **Your Cloudera Manager role is Limited Cluster Administrator,** not Full Administrator, even if
  you own the environment. That is what blocks §6.

The scripts are in [`datahub/`](datahub/). Run them from a workstation with the CDP CLI.

## 1. Versions

| Component | Version |
|---|---|
| Platform | CDP Public Cloud, AWS. Data Hub on the environment's Data Lake; in Cloudera Manager the Data Hub is a compute cluster attached to the Data Lake |
| Cluster template | "7.3.2 - Data Engineering: Apache Spark3, Apache Hive, Apache Oozie" |
| Image | The default 7.3.2 image of the `cdp-default` image catalog: RHEL 9.6, kernel `5.14.0-570.154.1.el9_6`, kernel-devel and gcc preinstalled, no package repositories, no NVIDIA driver, no CUDA |
| Cloudera Manager | 7.13.2 |
| Cloudera Runtime | 7.3.2 (`7.3.2-1.cdh7.3.2.p20000.83281140`) |
| Hadoop | `3.4.2.7.3.2.20000-258` |
| Spark 3 | `3.5.8.7.3.2.20000-258`, Scala 2.12.19, OpenJDK 17.0.21. Spark 3.5.8, where every other setup in this repo has 3.5.4 |
| `MapPartitionsRDD` constructor | 8 arguments, as on Base 7.3.2 and CDE 7.3.2, so the published RAPIDS jar won't link without the patch |
| Iceberg in the Spark 3 config | 1.10.1 (`iceberg-spark-runtime-3.5_2.12-1.10.1`) |
| NVIDIA RAPIDS Accelerator | 26.10.0 planned. Its release notes list Spark 3.5.8 (shim `spark358`) and Iceberg 1.6 and later as supported |
| NVIDIA driver | 615.71.10 (`.run` installer, open kernel modules) and Fabric Manager 615.71.10, from the recipe in §5. Not yet installed on any node |
| Hosts | master `m5.4xlarge` × 1; CPU workers `r5d.2xlarge` × 3; GPU compute group `p4d.24xlarge` (96 vCPU, 1152 GiB, 8 × A100 40 GB) at 0 nodes |

## 2. Prerequisites

- A CDP environment on AWS, and a CDP user with the EnvironmentAdmin role on it (to create the
  Data Hub and its recipe).
- **Service quota for the p4d:** 96 vCPUs of "Running On-Demand P instances" in the region.
- **Cloudera Manager Full Administrator on the Data Hub,** or someone who has it (§6). This is
  the step that doesn't work yet.
- **A workload password** for your CDP user. The Cloudera Manager API is reached through the Data
  Hub's Knox gateway (`cdp-proxy-api`), which authenticates with your workload user and password.
- Budget: a `p4d.24xlarge` costs about $22 an hour on demand. Scale the compute group up only once
  §6 is done, and back to 0 when you finish.

## 3. Create the Data Hub (run)

```bash
ENV_NAME=<environment> CLUSTER_NAME=<name> datahub/create-datahub.sh
```

Notes:

- **No `--image`.** The Data Hub uses the default 7.3.2 image, which is published in every
  region. You don't need an entitlement for it.
- **Both p4d groups, `compute` and `gateway`, start at 0 nodes.** The driver recipe must be
  attached before the first GPU node starts, because recipes run only when a node is created.
- **Keep the `gateway` group at 0.** Its host template has no NodeManager, so a GPU node there
  would never run executors.
- `recoveryMode=MANUAL` on every group means Data Hub won't try to repair a node while you reboot it
  in §7.

Wait for `cdp datahub describe-cluster --cluster-name <cluster>` to report `AVAILABLE`.

## 4. Find the Cloudera Manager API (run)

Set two variables for `datahub/cm-yarn-gpu.sh`:

```bash
export DH_GATEWAY=https://<cluster>-master0.<environment domain>/<cluster>/cdp-proxy-api   # Data Hub > Endpoints
export CDP_WORKLOAD_USER=<your workload user>
```

The script asks for your workload password when it runs in a terminal. Without a terminal, it
reads the password from `~/.dh-workload-pw`. To create that file, run this yourself, then type the
password:

```bash
read -rs P && printf %s "$P" > ~/.dh-workload-pw && chmod 600 ~/.dh-workload-pw; unset P
```

Delete the file when you're done.

Then look at the YARN settings. These commands are read-only:

```bash
datahub/cm-yarn-gpu.sh show                    # cgroup, LCE and GPU settings; host templates; rm_enabled per host
datahub/cm-yarn-gpu.sh raw /externalUserMappings   # which Cloudera Manager role your groups map to
```

Three things about this API differ from a Base cluster's:

- **The cluster doesn't appear in a plain `/clusters` listing.** The Data Hub is a compute cluster,
  so you have to ask for `/clusters?clusterType=any`.
- **The API version can't be discovered.** Knox returns an empty body for `cm-api/version`, so the
  script uses `v51`, which Cloudera Manager 7.13.2 serves.
- **Your Cloudera Manager role comes from group mappings.** In a Data Hub, `_c_cm_admins_<id>` maps
  to Limited Cluster Administrator plus Dashboard User, and `ipausers` maps to Read-Only. No group
  maps to Full Administrator.

## 5. Register the driver recipe and attach it to the compute group (run; recipe not yet run)

[`datahub/nvidia-gpu-recipe.sh`](datahub/nvidia-gpu-recipe.sh) runs as root before services start
(`PRE_SERVICE_DEPLOYMENT`). It installs:

- **The NVIDIA driver 615.71.10 from NVIDIA's `.run` installer,** with open kernel modules, built
  against the kernel-devel the image already has. The RPM route doesn't work: the image has no
  package repositories, so NVIDIA's RPMs can't resolve their dependencies.
- **Fabric Manager 615.71.10 from NVIDIA's tarball.** A p4d's eight A100s are connected through
  NVSwitch, and CUDA won't initialise without a running Fabric Manager of the same version as the
  driver. Fabric Manager is published as a tarball for 615.71.10 but not for 620.x, which is why the
  driver isn't 620.x as on Base.
- **The persistence daemon, and a nouveau blacklist.**
- **Kernel arguments for cgroup v1.** YARN GPU isolation doesn't support cgroup v2, so every GPU
  NodeManager host must boot with cgroup v1. These take effect only after a reboot (§7).

On a host with no NVIDIA PCI device the recipe exits without changing anything, so it is safe to
attach to any group. It logs to `/var/log/nvidia-gpu-recipe.log`.

```bash
cdp datahub create-recipe --recipe-name dh732-nvidia-gpu --type PRE_SERVICE_DEPLOYMENT \
  --recipe-content file://datahub/nvidia-gpu-recipe.sh
cdp datahub replace-recipes --datahub <cluster> \
  --instance-group-recipes instanceGroupName=compute,recipeNames=dh732-nvidia-gpu
```

`create-recipe` echoes the content HTML-escaped. That's only how it's displayed:
`cdp datahub describe-recipe` returns the script unchanged. To change the recipe later, register it
under a new name and run `replace-recipes` again.

Not yet verified: whether the installer accepts the flags the recipe passes (`--silent
--kernel-module-type=open --no-opengl-files --no-x-check --no-nouveau-check`). The first scale-up
will show it in the recipe log.

## 6. Turn on YARN cgroups (run; refused by Cloudera Manager)

GPU isolation in YARN needs cgroups. The settings are the ones on Base
([`README-719-base.md` §2](README-719-base.md#2-prerequisites)):

- on the YARN service: "Use CGroups for Resource Management" (`yarn_service_cgroups`) and
  "Always Use Linux Container Executor" (`yarn_service_lce_always`);
- on every NodeManager host: "Enable Cgroup-based Resource Management" (`rm_enabled`).

```bash
datahub/cm-yarn-gpu.sh cgroups
```

This sets the service settings, then `rm_enabled` on every NodeManager host, then restarts YARN.
Run it before any GPU node exists. The restart then also shows whether the CPU workers'
NodeManagers start with cgroups on while they are still on cgroup v2.

**What happened:**

1. Setting only the service settings is accepted, but the restart fails Cloudera Manager's
   validation: *"Yarn is configured to use cgroups, but 3 hosts do not have 'Enable Cgroup-based
   Resource Management' enabled"*.
2. Setting `rm_enabled` on the hosts fails with **HTTP 403**: `PUT /api/v51/hosts/<id>/config`.
   Limited Cluster Administrator can change service and role settings, but not host settings.
3. That validation has no "Suppress Configuration Validator" switch at the service, NodeManager or
   ResourceManager level. `datahub/cm-yarn-gpu.sh find 'cgroup|rm_enabled|gpu'` lists every
   candidate. Only the GPU validators have one.
4. Owning the environment, or changing your CDP role, doesn't change the Cloudera Manager role:
   the group mapping does. Changing the mapping itself needs Full Administrator.

If step 1 has left the service settings saved but not applied, YARN shows an invalid
configuration. Undo it without a restart:

```bash
datahub/cm-yarn-gpu.sh revert
```

**Open question to Cloudera:** what is the supported way for a customer to get Full Administrator in
a Data Hub's Cloudera Manager, or to turn on YARN GPU scheduling without it? Until that's answered,
someone with Full Administrator has to set `rm_enabled` on the NodeManager hosts: the workers, and
the compute host once it exists. Then rerun `cgroups`.

There may be a fallback that needs only Limited Cluster Administrator. It would turn on GPU only
for the compute group's NodeManagers, without service-wide cgroups. It needs NodeManager overrides
that point YARN at a cgroup v1 hierarchy, and a recipe step that creates that hierarchy at boot. It
hasn't been tried: nothing confirms that Cloudera Manager writes the GPU section of
`container-executor.cfg` without service-wide cgroups.

## 7. Add a GPU node (not run)

Once §6 succeeds:

```bash
cdp datahub scale-cluster --cluster-name <cluster> --instance-group-name compute --instance-group-desired-count 1
```

The recipe runs while the node is created. Then reboot the node so cgroup v1 takes effect. Connect
with your environment's SSH key, as `cloudbreak`, to the node's public IP, and run
`sudo systemctl reboot`. Then check on the node:

| Check | Command | Expected |
|---|---|---|
| Recipe finished | `sudo tail /var/log/nvidia-gpu-recipe.log` | ends with `=== done; reboot required for cgroup v1` |
| Driver sees the GPUs | `nvidia-smi -L` | 8 lines, `NVIDIA A100-SXM4-40GB` |
| Fabric Manager runs | `systemctl is-active nvidia-fabricmanager` | `active` |
| cgroup v1 after the reboot | `stat -fc %T /sys/fs/cgroup` | `tmpfs` (cgroup v2 prints `cgroup2fs`) |

## 8. Turn on GPU for the compute NodeManagers (not run)

```bash
datahub/cm-yarn-gpu.sh gpu     # GPU settings on the compute host template's NodeManager group, rm_enabled on the compute host, restart YARN
datahub/cm-yarn-gpu.sh nodes   # yarn.io/gpu per NodeManager, from the ResourceManager REST API
```

The GPU settings are the Base ones: "Enable GPU Usage" (`gpu_enabled`), "NodeManager GPU Devices
Allowed" (`gpu_plugin_allowed_devices`, `auto`) and "NodeManager GPU Detection Executable"
(`gpu_plugin_detector_path`, `/usr/bin/nvidia-smi`). They go on the compute group's NodeManager
role config group only (`yarn-NODEMANAGER-COMPUTE`), so the CPU workers keep running as before.
Expected: the compute NodeManager reports `yarn.io/gpu` = 8, and the workers report 0.

## 9. RAPIDS on this Spark build (not run)

The job settings follow [`README-732-base.md`](README-732-base.md) §6–§7, with these differences:

- **The shim is `spark358`, not `spark354`.**
  `spark.rapids.shims-provider-override=com.nvidia.spark.rapids.shims.spark358.SparkShimServiceProvider`.
- **The jar needs the constructor patch,** but `base/patch_rapids_jar.sh` currently patches only the
  `spark354/` classes. It has to be generalised to `spark358/`, and so do `base/plugin_load.sh`,
  `base/benchmark.sh` and `base/iceberg_check.sh`, which name the `spark354` shim.
- **Iceberg is 1.10.1,** inside the range RAPIDS 26.10 supports (1.6 and later). On Base,
  `suppressPlanningFailure=true` was needed because Cloudera's Iceberg was older than that. Here,
  Iceberg reads and writes may run on the GPU. `base/iceberg_check.sh` will show it.
- **The user needs an HDFS home directory** and a Kerberos ticket on the master (Spark 3 gateway),
  as on Base.

## 10. Next steps

| Step | State |
|---|---|
| Create the Data Hub with the default 7.3.2 image and a p4d compute group at 0 nodes | Done |
| Write the driver and Fabric Manager recipe, register it, attach it to the compute group | Done; recipe not run yet |
| Turn on YARN cgroups and `rm_enabled` on every NodeManager host | **Blocked**: needs Full Administrator in Cloudera Manager (§6) |
| Scale compute to 1, reboot into cgroup v1, run the §7 checks | To do |
| Turn on GPU for the compute NodeManagers; YARN reports `yarn.io/gpu` = 8 | To do |
| Generalise the `base/` scripts to the `spark358` shim, and patch the 26.10.0 jar | To do |
| Run `base/benchmark.sh` and `base/iceberg_check.sh`, GPU against CPU | To do |
| Add the result as a fifth column in the [overview](README.md#results-by-version) | To do |
| Scale compute back to 0 | After each GPU session |
