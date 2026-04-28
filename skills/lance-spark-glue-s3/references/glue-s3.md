# Lance Spark With AWS Glue And S3

This reference is for agents configuring, testing, or explaining Lance Spark with AWS Glue Data Catalog as the Lance namespace and S3 as table storage.

## Agent Discovery

The canonical skill lives at `skills/lance-spark-glue-s3`.

- OpenAI/Codex agents use `skills/lance-spark-glue-s3/SKILL.md` and `skills/lance-spark-glue-s3/agents/openai.yaml`.
- Claude agents discover the same content through `.claude/skills`, which is a symlink to `../skills`.
- Keep detailed instructions in this reference so both agent types read the same source of truth.

## Quick Start

Use the latest supported Spark/Scala pair unless the task explicitly targets another version:

```bash
export SPARK_VERSION=4.1
export SCALA_VERSION=2.13
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_S3_BUCKET_NAME=<bucket>
export AWS_GLUE_ROOT="s3://${AWS_S3_BUCKET_NAME}/lance_spark_glue_test_manual"

make docker-build-test-base SPARK_VERSION="$SPARK_VERSION" SCALA_VERSION="$SCALA_VERSION"
make bundle SPARK_VERSION="$SPARK_VERSION" SCALA_VERSION="$SCALA_VERSION"
make docker-build-test SPARK_VERSION="$SPARK_VERSION" SCALA_VERSION="$SCALA_VERSION"
make docker-test SPARK_VERSION="$SPARK_VERSION" SCALA_VERSION="$SCALA_VERSION" TEST_BACKENDS=glue
```

If credentials come from an AWS profile, set `AWS_PROFILE=<profile>` before `make docker-test`; the Makefile mounts `~/.aws` into the container when `AWS_PROFILE` is present.

For real AWS tests, leave `AWS_GLUE_ENDPOINT` unset. Do not set `storage.endpoint` or `storage.aws_allow_http`; those are for MinIO or endpoint-override testing.

## Spark Catalog Configuration

Minimal Spark configuration:

```properties
spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.lance.impl=glue
spark.sql.catalog.lance.root=s3://your-bucket/lance_glue
spark.sql.catalog.lance.region=us-east-1
spark.sql.catalog.lance.storage.region=us-east-1
spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions
spark.sql.defaultCatalog=lance
```

Optional settings:

```properties
spark.sql.catalog.lance.catalog_id=123456789012
spark.sql.catalog.lance.access_key_id=<access-key>
spark.sql.catalog.lance.secret_access_key=<secret-key>
spark.sql.catalog.lance.session_token=<session-token>
spark.sql.catalog.lance.storage.access_key_id=<access-key>
spark.sql.catalog.lance.storage.secret_access_key=<secret-key>
spark.sql.catalog.lance.storage.session_token=<session-token>
```

Use `catalog_id` when the target Glue catalog is not the caller's default account catalog. Static credentials are usually unnecessary if the AWS default provider chain is already configured, but when static credentials are used in Spark config, pass them to both the Glue namespace client and the S3 storage client as shown above.

## Docker Test Configuration

The Docker integration test image contains two key jars:

- The local Lance Spark bundle built by `make bundle`.
- The published `lance-namespace-glue-<version>-bundle.jar` downloaded from Maven Central.

Code references:

- [docker/Dockerfile.test](../../../docker/Dockerfile.test#L22) copies the Lance Spark bundle and downloads the Glue namespace bundle.
- [Makefile](../../../Makefile#L150) prints resolved Docker build args, including `lance-namespace-impl-version`.
- [Makefile](../../../Makefile#L170) builds the test image after the bundle exists.
- [Makefile](../../../Makefile#L183) runs Docker tests and forwards Glue/S3 environment variables.

Useful environment variables:

| Variable | Required | Purpose |
| --- | --- | --- |
| `TEST_BACKENDS=glue` | Yes | Restricts pytest to the Glue backend. |
| `AWS_S3_BUCKET_NAME` | Yes | S3 bucket used for Lance table storage. |
| `AWS_GLUE_ROOT` | Recommended | S3 prefix for test data. Defaults to a generated prefix if the bucket is set. |
| `AWS_REGION` or `AWS_DEFAULT_REGION` | Yes | AWS region for Glue and S3. |
| `AWS_GLUE_CATALOG_ID` | Optional | Glue catalog account ID. |
| `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` | Optional | Static credentials when not using profile or instance credentials. |
| `AWS_SESSION_TOKEN` | Optional | Session token for temporary credentials. |
| `AWS_PROFILE` | Optional | Profile from mounted `~/.aws` credentials. |
| `AWS_GLUE_ENDPOINT` | Endpoint testing only | Glue endpoint override. Leave unset for real AWS Glue. |

## CI Workflow

The real AWS workflow is [`.github/workflows/spark-aws.yml`](../../../.github/workflows/spark-aws.yml#L13).

Current behavior:

- Runs the latest Spark/Scala pair: Spark 4.1 with Scala 2.13.
- Uses `TEST_BACKENDS=glue`.
- Skips pull requests from forks because repository secrets are unavailable there.
- Builds the Lance Spark bundle, builds the Docker image, verifies AWS targets, then runs the Docker pytest suite.

To confirm the workflow is using real AWS Glue/S3:

1. Check the `Verify AWS Glue/S3 targets` step. It must run `aws sts get-caller-identity`, `aws s3api head-bucket`, and `aws glue get-databases`.
2. Check the Docker test step environment. `AWS_GLUE_ROOT` should be an `s3://` URI under the real bucket secret.
3. Check the pytest header for `lance spark backends: glue`, `aws glue root: s3://...`, and the expected AWS region.
4. Confirm there is no `AWS_GLUE_ENDPOINT`, MinIO endpoint, `storage.endpoint`, or `storage.aws_allow_http` in the real AWS job.

Workflow references:

- [`.github/workflows/spark-aws.yml`](../../../.github/workflows/spark-aws.yml#L51) defines the Glue/S3 Docker test job.
- [`.github/workflows/spark-aws.yml`](../../../.github/workflows/spark-aws.yml#L102) verifies real AWS targets before running pytest.
- [`.github/workflows/spark-aws.yml`](../../../.github/workflows/spark-aws.yml#L127) runs the Glue/S3 Docker integration tests.
- [`.github/workflows/spark-aws.yml`](../../../.github/workflows/spark-aws.yml#L142) cleans up S3 test data.

## Python Integration Tests

The Docker test runs `pytest /home/lance/tests/ -v --timeout=180`. The test backend selection and Spark session configuration live in [integration-tests/conftest.py](../../../integration-tests/conftest.py#L173).

Glue-specific behavior:

- [integration-tests/conftest.py](../../../integration-tests/conftest.py#L180) reads AWS env vars.
- [integration-tests/conftest.py](../../../integration-tests/conftest.py#L196) adds `glue` when `AWS_S3_BUCKET_NAME` is present.
- [integration-tests/conftest.py](../../../integration-tests/conftest.py#L201) prints the pytest backend, root, and region.
- [integration-tests/conftest.py](../../../integration-tests/conftest.py#L261) configures `spark.sql.catalog.lance.impl=glue`.
- [integration-tests/conftest.py](../../../integration-tests/conftest.py#L271) applies `AWS_GLUE_ENDPOINT` only when explicitly provided.
- [integration-tests/conftest.py](../../../integration-tests/conftest.py#L308) shows MinIO-only endpoint settings that should not appear in a real AWS Glue/S3 run.

Use [integration-tests/test_lance_spark.py](../../../integration-tests/test_lance_spark.py#L120) as the main feature showcase. The suite covers:

- Namespace DDL: catalogs and namespaces.
- Table DDL: create, show, describe, drop, CTAS, replace, properties, compression, indexes, optimize, vacuum, and primary keys.
- DQL: select, filters, grouping, ordering, limits, data types, and time travel.
- DML: insert, update, delete, merge, add column, update column, and insert overwrite.
- Stable row IDs and CDC-style incremental ingestion.

Some rename-table tests are marked `requires_rest` and are not expected to run against Glue.

## Runtime Code Map

Catalog and namespace setup:

- [pom.xml](../../../pom.xml#L55) defines the Lance namespace and namespace implementation versions used by Spark.
- [lance-spark-base_2.12/src/main/java/org/lance/spark/BaseLanceNamespaceSparkCatalog.java](../../../lance-spark-base_2.12/src/main/java/org/lance/spark/BaseLanceNamespaceSparkCatalog.java#L202) initializes the Spark catalog, parses options, registers the namespace implementation, and connects with `LanceNamespace.connect`.
- [lance-spark-base_2.12/src/main/java/org/lance/spark/LanceRuntime.java](../../../lance-spark-base_2.12/src/main/java/org/lance/spark/LanceRuntime.java#L81) maps `impl=glue` to `org.lance.namespace.glue.GlueNamespace`.
- [lance-spark-base_2.12/src/main/java/org/lance/spark/LanceRuntime.java](../../../lance-spark-base_2.12/src/main/java/org/lance/spark/LanceRuntime.java#L92) disables reconnecting Glue namespace clients on workers; driver-resolved table URI and storage options are used for worker reads.

Storage options and dataset opens:

- [lance-spark-base_2.12/src/main/java/org/lance/spark/LanceSparkCatalogConfig.java](../../../lance-spark-base_2.12/src/main/java/org/lance/spark/LanceSparkCatalogConfig.java#L72) maps `storage.*` Spark options into Lance storage options.
- [lance-spark-base_2.12/src/main/java/org/lance/spark/utils/Utils.java](../../../lance-spark-base_2.12/src/main/java/org/lance/spark/utils/Utils.java#L124) merges catalog storage options with `describeTable` storage options and opens datasets with namespace/table IDs when available.

Config examples:

- [docker/spark-defaults.conf](../../../docker/spark-defaults.conf#L12) contains commented Glue catalog defaults for Docker users.

## Manual AWS Preflight

Run these before launching Docker when debugging local credentials or permissions:

```bash
aws sts get-caller-identity
aws s3api head-bucket --bucket "$AWS_S3_BUCKET_NAME"
aws glue get-databases --region "$AWS_REGION" --max-results 1
```

If `AWS_GLUE_CATALOG_ID` is set:

```bash
aws glue get-databases \
  --region "$AWS_REGION" \
  --catalog-id "$AWS_GLUE_CATALOG_ID" \
  --max-results 1
```

The test role needs permissions for STS identity checks, bucket access under the selected S3 prefix, and Glue database/table create, read, update, and delete operations.

## Troubleshooting

`Glue/S3 Docker Test` is skipped in CI:

- For pull requests, the job only runs when the PR branch is in the same repository because forked PRs cannot access AWS secrets.
- Confirm the workflow trigger did not ignore the changed paths. Documentation-only changes under `docs/**` or `README.md` are ignored by this workflow.

Pytest skips the Glue backend:

- Ensure `TEST_BACKENDS=glue` and `AWS_S3_BUCKET_NAME` are set.
- Check the pytest header from [integration-tests/conftest.py](../../../integration-tests/conftest.py#L201).

The Docker build cannot find the Lance Spark bundle:

- Run `make bundle SPARK_VERSION=4.1 SCALA_VERSION=2.13` before `make docker-build-test`.
- The Makefile checks for the bundle at [Makefile](../../../Makefile#L172).

`ClassNotFoundException` for Glue namespace:

- Confirm `lance-namespace-glue-<version>-bundle.jar` was downloaded into Spark jars by [docker/Dockerfile.test](../../../docker/Dockerfile.test#L23).
- Confirm `LANCE_NAMESPACE_IMPL_VERSION` resolves to the published version in [pom.xml](../../../pom.xml#L56).

The run is accidentally using MinIO or another S3-compatible endpoint:

- Remove `AWS_GLUE_ENDPOINT`.
- Remove any Spark options ending in `storage.endpoint` or `storage.aws_allow_http`.
- Compare the Glue branch and MinIO branch in [integration-tests/conftest.py](../../../integration-tests/conftest.py#L255) and [integration-tests/conftest.py](../../../integration-tests/conftest.py#L308).

Cleanup after local runs:

```bash
aws s3 rm "$AWS_GLUE_ROOT" --recursive
```

If tests created Glue databases or tables that were not dropped due to interruption, remove them with the AWS CLI or AWS console for the configured catalog and region.
