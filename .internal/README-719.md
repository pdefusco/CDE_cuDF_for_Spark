# CDE cuDF for Spark on Runtime 7.1.9 / CDE 1.27

← [Overview](../README.md) · Customer-facing version: [`README-719.md`](../README-719.md) · Runtime 7.3.2: [`README-732.md`](../README-732.md)

> **Internal engineering write-up.** How the 7.1.9 result was reached and how it compares to 7.3.2.
> For the customer-facing install guide, see [`../README-719.md`](../README-719.md).

The same work as [`README-732.md`](../README-732.md), repeated on a Private Cloud built on Runtime 7.1.9
instead of 7.3.2. The question was whether the custom runtime **and** the `MapPartitionsRDD` patch
carry over.

**Outcome: working, with no patch.** The custom runtime carries over with the same structure, but the
patch does not apply. Cloudera's 7.1.9 Spark keeps Apache's 7-arg `MapPartitionsRDD` constructor, so
the **OSS RAPIDS jar links as published**. `runtime/Dockerfile.719` is the 7.3.2 image minus the
patcher stage. The smoke test passed on the first run that reached the executors. It ran on an A10G,
with 8 `Gpu*` operators in the final plan and no `NoSuchMethodError`.

Internal technical prototype, not a supported configuration. Unlike the 7.3.2 image, this one ships
the third-party RAPIDS jar unmodified.

## 1. Environment

| | |
|---|---|
| Private Cloud | Data Services 1.5.5 SP4 (ECS) |
| Cloudera Manager | 7.13.2 SP2 |
| Runtime | 7.1.9 SP2 CHF4 (`7.1.9.20400`), Kafka on ZooKeeper |
| CDE | 1.27.0-b693 |
| CDE Spark runtime base | `dex-spark-runtime-3.5.4-7.1.9.1078:1.27.0-b693` |
| `spark-core` | `spark-core_2.12-3.5.4.127.354.7191000.0-30.jar` |
| Scala / JDK / uid | 2.12.18 / 17.0.20.1 / 1345 (`sparkuser`) |
| GPU workers | 3 × `g5.8xlarge`, 1× NVIDIA A10G each |
| Worker OS / arch | RHEL 9.6, **amd64** |
| RAPIDS | `rapids-4-spark_2.12-26.02.0-cuda12.jar`, `spark354` shim, **unmodified** |
| Image | `pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-7.1.9` |
| CDE resource | `cde-rapids-runtime-719` |
| Virtual cluster | `cuDFforSpark` |

Read the base image off the live VC rather than guessing it. It is the `image` key in the VC's
`dex.yaml`:

```bash
kubectl get cm <vcId>-api-cm -n <vcId> -o jsonpath='{.data.dex\.yaml}' | grep -E '^\s*(image|gpuImage):'
```

Note that the Scala version differs from the 7.3.2 base (2.12.19). That matters only if you ever need
the patcher here.

## 2. The finding: the ctor is upstream's

`javap` inside the 7.1.9 base:

```
public org.apache.spark.rdd.MapPartitionsRDD(org.apache.spark.rdd.RDD<T>,
    scala.Function3<org.apache.spark.TaskContext, java.lang.Object, scala.collection.Iterator<T>, scala.collection.Iterator<U>>,
    boolean, boolean, boolean, scala.reflect.ClassTag<U>, scala.reflect.ClassTag<T>);
```

| Spark build | Runtime / CDE | ctor |
|---|---|---|
| Cloudera `3.5.4.127.354.7191000.0-30` | **7.1.9 SP2 CHF4 / 1.27** | **7 args, links** |
| Cloudera `3.5.4.1.26.732.0-45` | 7.3.2 / 1.26 | 8 args (`scala.Option` isDeterministic), fails |

So the extra `isDeterministic` parameter belongs to the 7.3.2 line. It is not in 7.1.9's Spark 3.5.4
build. This is the same build-window conclusion as
[`docs/oss-rapids-vs-cloudera-spark.md`](../docs/oss-rapids-vs-cloudera-spark.md), with one more data
point.

**Do not reuse the 7.3.2 `Dockerfile` here.** Recompiled against this `spark-core`, the two patched
classes emit the 7-arg call again. The patch would be a no-op, and the build's own `javap -c`
assertion fails by design. `Dockerfile.719` carries the mirror-image assertion instead. It fails the
build if a later 7.1.9 base ever picks up the 8-arg constructor, which is the signal to switch back to
the patched build.

The linkage scan in [`docs/linkage-scan.md`](../docs/linkage-scan.md) was run against the 7.3.2 jars.
It has **not** been repeated against 7.1.9. The smoke test only proves the `MapPartitionsRDD` path.

## 3. Steps

### 3.1 GPU virtual cluster (same as 7.3.2)

These steps are unchanged from [`README-732.md`](../README-732.md) §3, which explains each one:

```bash
cdp de update-service --cluster-id <clusterId> --gpu-requests 3
python scripts/create_vc_gpu.py --cluster-id <clusterId> --name cuDFforSpark
scripts/verify_gpu_vc.sh <vcId>
```

The rules that cost a recreate if they are missed apply here too: GPU settings are create-only, a
console-built VC gets `gpuAccelerationEnabled: false`, and `guaranteedGpuRequests` must be 0. See
[`README-732.md`](../README-732.md) §5.

### 3.2 Build, push, register, submit

```bash
cd runtime
curl -fL --retry 3 -O \
  https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.0/rapids-4-spark_2.12-26.02.0-cuda12.jar

docker build --platform linux/amd64 -f Dockerfile.719 \
  -t pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-7.1.9 .
docker push pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-7.1.9
cd ..

cde --config-profile <profile> resource create \
  --name cde-rapids-runtime-719 --type custom-runtime-image --image-engine spark3 \
  --image pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-7.1.9

CDE_PROFILE=<profile> RUNTIME_RESOURCE=cde-rapids-runtime-719 ./scripts/submit_rapids_smoke_test.sh
cde --config-profile <profile> run logs --id <runId> --type driver/stdout --follow=false
```

The build prints `upstream MapPartitionsRDD ctor OK` from the assertion. The jar keeps the same
in-image path and filename as the 7.3.2 image, so `submit_rapids_smoke_test.sh` needs no edits; only
the resource name changes. Sizing goes through CDE's native flags, not `--conf`, for the reason given
in [`README-732.md`](../README-732.md) §4. The script already does this.

## 4. Verified output

Run 2, `succeeded`, runtime resource `cde-rapids-runtime-719`:

```
  host=rapids-smoke-test-…-exec-1  gpu=0, NVIDIA A10G

AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   GpuColumnarToRow false
   +- GpuTopN(limit=5, …)
      +- GpuHashAggregate (keys=[k#2], functions=[gpucount(1, false), gpubasicsum(…), avg(…)])
         +- GpuShuffleCoalesce 1073741824
            +- GpuCustomShuffleReader coalesced
               +- ShuffleQueryStage 0
                  +- GpuColumnarExchange gpuhashpartitioning(k#2, 200, Murmur3Mode), ENSURE_REQUIREMENTS
                     +- GpuHashAggregate (keys=[k#2], functions=[partial_gpucount(1, false), …])
                        +- GpuProject [cast((id#0L % 1000) as int) AS k#2, …]
                           +- GpuRange (0, 20000000, step=1, splits=24)

VERDICT
  plugin configured : True
  plan finalized    : True
  Gpu* operators    : 8  ['GpuColumnarExchange', 'GpuColumnarToRow', 'GpuCustomShuffleReader',
                          'GpuHashAggregate', 'GpuProject', 'GpuRange', 'GpuShuffleCoalesce', 'GpuTopN']
  PASS: RAPIDS executed operators on the GPU.
```

This is the same plan shape as run 6 on 7.3.2, with no CPU operator below `GpuColumnarToRow`. Run 1
never reached Spark; it failed the keytab pre-flight (§5).

## 5. What got in the way (none of it GPU-related)

Each of these blocked **all** jobs, not just GPU ones. Each is written up in full in
[`docs/cde-pvc-prerequisites.md`](../docs/cde-pvc-prerequisites.md).

- **`421 Misdirected Request` on every VC endpoint.** This is a fault in the deployment's Caddy reverse
  proxy, not in CDE. Caddy sent the bare CDE service domain as the upstream TLS SNI while forwarding
  the VC hostname as `Host`, and ingress-nginx rejects any request where the two differ. The fix is
  `tls_server_name {http.request.host}` in the wildcard site's `transport http` block. It is fixed in
  the deployment repo's `cloudera_cde.caddyfile.j2`.
- **`authorization policy denies`.** The DE resource role was missing, the same as on 7.3.2. Two
  things were different this time:
  - `cdp iam` went to the public control plane, so the grant went through `cdpcurl` instead.
  - The grant took about 5 minutes to take effect, because `dex-authz` caches roles.
- **`keytab is not present for user admin`.** Same as on 7.3.2. On CDE 1.27 the upload is under the
  virtual cluster's **Actions** menu. Generating the keytab on FreeIPA has a salt pitfall: `ktutil`
  needs `-f`.

## 6. Still open on 7.1.9

- **Is Cloudera's own GPU image pullable here?** The VC's `dex.yaml` names
  `container.repository.cloudera.com/cdp-private/cloudera/dex/dex-spark-runtime-gpu-3.5.4-7.1.9.1078:1.27.0-b693`
  as `gpuImage`, but its pullability was **not** probed on this cluster. The custom runtime was used
  regardless, and it is the only route verified here. To check, run `probes/probe_images.sh` against
  this VC. The script is hardcoded to the 7.3.2 cluster's worker and build tag, so edit those first.
  If the image pulls, try it before maintaining a custom image.
- **Linkage beyond `MapPartitionsRDD`**: not re-scanned; see §2.
- **The real workload.** As on 7.3.2, the smoke test proves linkage and plan conversion, not a
  250M-row Hive ETL ([`README-732.md`](../README-732.md) §8).
