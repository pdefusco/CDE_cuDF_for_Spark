# Slides: Spark RAPIDS on CDE

Two slides for a mixed / leadership audience. Bullets go on the slide; speaker notes carry the detail.
Every figure comes from `README.md` §§6–9, `runtime/README-patch.md` and `docs/linkage-scan.md`.

---

## Slide 1 — Spark RAPIDS on CDE: GPU acceleration working on-prem

*A real Spark query now runs end-to-end on an NVIDIA GPU on CDE 1.26 Private Cloud.*

- **Result:** the whole query ran on the GPU (NVIDIA A10G), with no fallback to the CPU
  - Projection, aggregation, a real GPU shuffle and sort: the building blocks of ETL
- **Blocker 1: no GPU runtime image available.** Cloudera's GPU images can't be reached from this cluster
  - Fix: a CDE custom runtime built on the standard CPU image, adding just the RAPIDS jar and a GPU
    discovery script. No CUDA install, no cluster rebuild.
- **Blocker 2: the open-source RAPIDS jar is incompatible with Cloudera's Spark**
  - Cloudera's 7.3.2 / CDE 1.26 Spark added an optional parameter to one internal Spark class
  - Invisible to code built against Cloudera's Spark, but it breaks pre-built plugins like RAPIDS.
    Every GPU query failed on its first action.
  - Limited to this build: CDE 1.25 (what CAI runs) runs the same jar fine
- **Fix: recompile 2 of 17,833 classes with no code changes.** NVIDIA's source and Cloudera's Spark
  are both untouched
- **Risk measured, not guessed:** all 4,473 Spark calls RAPIDS makes were checked
  - 99.6% match; 16 don't, and only **1** is reachable by default (Hive-format `CREATE TABLE AS SELECT`)

> **Speaker notes**
> - *Why a recompile is enough:* the new parameter has a default value. NVIDIA's unchanged source
>   therefore compiles against Cloudera's Spark as-is, and the compiler fills in Cloudera's own default.
>   The only thing that changes is the method signature recorded in the compiled bytecode.
> - *Why nothing else fixes it:* the new constructor exists in no Apache Spark release, so no
>   open-source RAPIDS version can call it. Shim overrides don't help either, because all of them
>   carry the old call.
> - *Why the scan matters:* it answers "is this the first of many?" with a measurement. The 16
>   mismatches were traced one by one. Most are dead code or sit behind features that are off by
>   default.

---

## Slide 2 — Proposed: a repeatable "Cloudera-compatible RAPIDS" build

*Turn a one-off fix into a small, automated step for each CDE release.*

- **What it is:** a build step that recompiles the affected RAPIDS classes against the exact Spark in
  each CDE runtime image, then publishes a ready-to-use GPU runtime
  - The CDE image already ships the compiler and the Spark jars, so no extra toolchain is needed
- **Why this approach:** no fork of RAPIDS and no change to Spark
  - No logic changes means no new code to review and no drift from NVIDIA's behaviour
- **Built-in safety checks**
  - The build fails if the patch silently stops applying, so a no-op never ships
  - The compatibility scan re-runs on every CDE patch release and catches new mismatches before they
    reach users
- **Cost of ownership:** each build is pinned to one RAPIDS version and one Cloudera Spark build, so it
  is rebuilt on every upgrade. Rebuilds are mechanical, and the safety check shows whether the patch is
  still needed.
- **Known gaps / next steps**
  - Hive-format CTAS needs a one-line source edit, not just a recompile. We fix it if and when it's needed
  - Prove it on the real 250M-row Hive ETL, not just the smoke test
  - Confirm that recovery behaves correctly after a node is lost under Cloudera's new default
- **Decision needed: ownership and support.** Neither Cloudera nor NVIDIA supports this today. If
  Cloudera ships its own GPU runtime, we retire this build.

> **Speaker notes**
> - *Where it plugs in:* this is already a working two-stage Docker build (`runtime/Dockerfile`).
>   Productizing it means wiring that build into the CDE release pipeline and running the scan in CI.
> - *If a new mismatch appears:* it fails loudly and names the Spark class (`NoSuchMethodError`). The
>   fix loop is short: read the error, inspect that class in the runtime image, add the matching
>   RAPIDS source to the patch, rebuild. It takes minutes, not days.
> - *Why this is a build-window issue, not a Cloudera-wide one:* CAI on CDE 1.25 runs the identical
>   RAPIDS setup successfully. The only difference is the Spark build underneath.
