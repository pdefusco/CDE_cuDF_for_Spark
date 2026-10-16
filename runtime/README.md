# Custom CDE runtime with Spark RAPIDS

A **CDE custom runtime** is a per-job image: `cde resource create --type custom-runtime-image`,
then `cde spark submit --runtime-image-resource-name=...`. That matters because the alternative —
the `dexapp.api.sparkRuntime.gpuImage.override` chart value — is **create-time only** and would cost
a teardown and rebuild of the virtual cluster. A custom runtime needs no VC recreate.

## Which Dockerfile

Choose by the `spark-core` jar in the CDE base image. Both builds call themselves Spark 3.5.4, so the
Spark version alone does not tell you which one you have.

| Dockerfile | Base | `spark-core` | RAPIDS jar | Tag |
|---|---|---|---|---|
| `Dockerfile` | `dex-spark-runtime-3.5.4-7.3.2.0:1.26.0-b557` | `3.5.4.1.26.732.0-45` (8-arg ctor) | patched, 2 classes recompiled | `26.02.0-p1` |
| `Dockerfile.719` | `dex-spark-runtime-3.5.4-7.1.9.1078:1.27.0-b693` | `3.5.4.127.354.7191000.0-30` (7-arg ctor) | OSS, unmodified | `26.02.0-7.1.9` |

Each one asserts its own constructor shape at build time and fails on the wrong base. So picking the
wrong one breaks the build rather than the first GPU job. Details: [`README-patch.md`](README-patch.md)
for 7.3.2, [`../.internal/README-719.md`](../.internal/README-719.md) for 7.1.9.

## Why there is no CUDA base image here

The OSS `rapids-4-spark` jar **self-contains the CUDA libraries**. That is why it is ~887 MB, and it
is why this image is `FROM` the ordinary CPU `dex-spark-runtime` with a file copied in, rather than
`FROM` an NVIDIA CUDA image. Keeping the base unchanged is what makes the image a legal CDE runtime.

## Fetch the jar (not committed — see `.gitignore`)

```bash
cd runtime
curl -fL --retry 3 -O \
  https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.0/rapids-4-spark_2.12-26.02.0-cuda12.jar
```

`26.02.0` is the version locked to Spark 3.5.4 — see `../docs/rapids-spark-conf.md` for the
four-way version lock and why the `spark351` shim throws `NoSuchMethodError` on 3.5.4. The `cuda12`
classifier is deliberate: the workers run a CUDA 13.4 driver, which runs CUDA 12 binaries fine, and
`26.02.0-cuda12` is the combination already proven on the CAI side of this work.

## Build

The workers are **amd64 RHEL 9.6**; a Mac build host is arm64. `--platform linux/amd64` is therefore
mandatory, and the build is `COPY`-only precisely so that cross-building needs no emulation.

```bash
docker build --platform linux/amd64 -t pauldefusco/cde-rapids-runtime-3.5.4:latest -f Dockerfile .
docker push pauldefusco/cde-rapids-runtime-3.5.4:latest
```

Runtime 7.1.9 / CDE 1.27:

```bash
docker build --platform linux/amd64 -t pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-7.1.9 -f Dockerfile.719 .
docker push pauldefusco/cde-rapids-runtime-3.5.4:26.02.0-7.1.9
```

The base is overridable (`--build-arg BASE=...`) if the VC's `dex.yaml` names a different build. Read
it from there instead of assuming the default.
