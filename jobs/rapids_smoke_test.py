#!/usr/bin/env python3
"""Smallest job that PROVES Spark RAPIDS is running on the GPU in CDE.

The point of this job is not the computation — it is the assertion. RAPIDS falls back to CPU
**silently**: a job with a broken plugin, a missing jar, a wrong shim or zero visible GPUs still
finishes green and still prints a plausible wall-clock number. So a green run means nothing on its
own, and neither does "it got faster".

Three independent checks, in increasing strength:

  1. The plugin loaded            -> `spark.rapids.sql.enabled` is readable and the SQLPlugin is in
                                     `spark.plugins`.
  2. The executors can see a GPU  -> each executor reports its CUDA device via the resource
                                     allocation Spark handed it, gathered with a tiny mapPartitions.
  3. The query actually ran on it -> the physical plan contains `Gpu*` operators.

Check 3 is the only one that cannot be faked by configuration alone, so the exit code keys off it.

Submit with:
    cde spark submit jobs/rapids_smoke_test.py \
      --runtime-image-resource-name cde-rapids-runtime \
      ... (the conf block from docs/rapids-spark-conf.md)
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def executor_gpu_report(spark, n_parts: int):
    """Ask every partition which GPU it can see. Runs on the executors, not the driver.

    The driver is NOT a GPU process in this setup, so checking `nvidia-smi` locally would prove
    nothing about where the executors ran.
    """

    def probe(_):
        import os
        import subprocess

        try:
            out = subprocess.run(
                ["nvidia-smi", "--query-gpu=index,name", "--format=csv,noheader"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            smi = out.stdout.strip().replace("\n", " | ") or f"rc={out.returncode}"
        except FileNotFoundError:
            # The decisive negative result: no NVIDIA container runtime in the container.
            smi = "NO nvidia-smi IN CONTAINER"
        except Exception as exc:  # noqa: BLE001 - want the reason, whatever it is
            smi = f"error: {type(exc).__name__}: {exc}"
        yield f"host={os.uname().nodename}  gpu={smi}"

    return spark.sparkContext.parallelize(range(n_parts), n_parts).mapPartitions(probe).collect()


def main() -> int:
    spark = SparkSession.builder.appName("rapids-smoke-test").getOrCreate()
    conf = spark.sparkContext.getConf()

    print("=" * 78)
    print("1. PLUGIN CONFIGURATION")
    print("=" * 78)
    for key in (
        "spark.plugins",
        "spark.rapids.sql.enabled",
        "spark.rapids.sql.explain",
        "spark.executor.resource.gpu.amount",
        "spark.task.resource.gpu.amount",
        "spark.executor.resource.gpu.discoveryScript",
        "spark.shuffle.manager",
        "spark.rapids.shims-provider-override",
    ):
        print(f"  {key:52s} = {conf.get(key, '<unset>')}")

    plugin_configured = "com.nvidia.spark.SQLPlugin" in (conf.get("spark.plugins") or "")
    print(f"\n  SQLPlugin present in spark.plugins: {plugin_configured}")

    print()
    print("=" * 78)
    print("2. WHAT THE EXECUTORS CAN SEE")
    print("=" * 78)
    for line in executor_gpu_report(spark, 3):
        print(f"  {line}")

    print()
    print("=" * 78)
    print("3. DID THE QUERY RUN ON THE GPU?")
    print("=" * 78)

    # Deliberately trivial, but shaped so RAPIDS has something it *wants* to accelerate: a hash
    # aggregate over a wide-ish shuffle. A pure `spark.range().count()` can be answered without
    # any operator RAPIDS cares about, which would make the plan check ambiguous.
    df = (
        spark.range(0, 20_000_000, 1, 24)
        .withColumn("k", (F.col("id") % F.lit(1000)).cast("int"))
        .withColumn("v", (F.col("id") * F.lit(7) % F.lit(99991)).cast("double"))
    )
    agg = df.groupBy("k").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum("v").alias("sum_v"),
        F.avg("v").alias("avg_v"),
    )

    # Read the plan AFTER the action, never before, and from the SAME DataFrame the action ran on.
    # With AQE on (the CDE default) the plan is an `AdaptiveSparkPlan isFinalPlan=false` wrapper
    # until a query stage actually materializes, and in that state toString() reports the
    # *pre-conversion* CPU operator names — HashAggregate, Exchange — even on a run that executed
    # entirely on the GPU. Two separate bugs come out of that, and both have bitten:
    #
    #   run 4: read the plan before the action          -> false CPU verdict
    #   run 5: read `agg` but collect `agg.orderBy(...)` -> false CPU verdict again
    #
    # The second is the subtle one. `agg.orderBy("k").limit(5)` is a *different* DataFrame with its
    # own QueryExecution, so collecting it leaves `agg`'s own AdaptiveSparkPlan unexecuted and stuck
    # at isFinalPlan=false. Run 5's driver log showed GpuOverrides converting the plan five times and
    # a GpuOpTimeTrackingRDD running all 24 tasks, while this check printed HashAggregate. Bind the
    # query to one object and use that object for both.
    probe = agg.orderBy("k").limit(5)
    rows = probe.collect()

    plan = probe._jdf.queryExecution().executedPlan().toString()
    print(plan[:3000])

    # Fail loudly rather than silently mis-verdicting if AQE still has not finalized: in that state
    # the operator names below are not evidence of anything, either way.
    plan_is_final = "isFinalPlan=false" not in plan
    if not plan_is_final:
        print(
            "\n  WARNING: AdaptiveSparkPlan is still isFinalPlan=false after collect(). The operator\n"
            "  names above are pre-conversion and this check cannot be trusted. Cross-check the\n"
            "  driver log for 'GpuOverrides: Plan conversion to the GPU' and Gpu*RDD in the stage\n"
            "  descriptions before believing either verdict."
        )

    print("\n  first 5 rows:")
    for r in rows:
        print(f"    {r}")

    gpu_ops = sorted(
        {
            tok.strip("(),*+ ")
            for tok in plan.replace("\n", " ").split()
            if tok.strip("(),*+ ").startswith("Gpu")
        }
    )

    print()
    print("=" * 78)
    print("VERDICT")
    print("=" * 78)
    print(f"  plugin configured : {plugin_configured}")
    print(f"  plan finalized    : {plan_is_final}")
    print(f"  Gpu* operators    : {len(gpu_ops)}  {gpu_ops[:12]}")

    spark.stop()

    if not plan_is_final:
        print(
            "\n  INCONCLUSIVE: the plan never finalized, so this run proves nothing either way.\n"
            "  This is a defect in the test, not in the cluster — see the comment at the probe."
        )
        return 2

    if not gpu_ops:
        print(
            "\n  FAIL: the plan contains no Gpu* operators, so this ran on the CPU.\n"
            "  Look for the reason in the executor log — with spark.rapids.sql.explain=NOT_ON_GPU\n"
            "  RAPIDS prints one line per operator it declined, naming the cause. A missing jar,\n"
            "  a shim mismatch, or zero discovered GPUs all land here."
        )
        return 1

    print("\n  PASS: RAPIDS executed operators on the GPU.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
