# Custom CDE runtime with Spark RAPIDS

A **CDE custom runtime** is a per-job image: `cde resource create --type custom-runtime-image`,
then `cde spark submit --runtime-image-resource-name=...`. That matters because the alternative —
the `dexapp.api.sparkRuntime.gpuImage.override` chart value — is **create-time only** and would cost
a teardown and rebuild of the virtual cluster. A custom runtime needs no VC recreate.

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
