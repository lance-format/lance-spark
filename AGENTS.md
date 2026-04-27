# AGENTS.md

Lance Spark is an Apache Spark connector for the Lance columnar data format, enabling Spark to read and write Lance datasets. It supports multiple Spark versions (3.4, 3.5, 4.0, 4.1) and Scala versions (2.12, 2.13).

## Architecture

Multi-module Maven project with shared base code and version-specific modules:

- `lance-spark-base_2.12/` - Shared source code (Scala 2.12 sources, cross-compiled for 2.13)
- `lance-spark-{3.4,3.5}_2.{12,13}/` - Spark version-specific modules
- `lance-spark-{4.0,4.1}_2.13/` - Spark 4.x modules (Scala 2.13 only)
- `lance-spark-bundle-*/` - Shaded JARs for distribution
- `integration-tests/` - PySpark integration tests (pytest, runs against local/Azurite/MinIO/LanceDB Cloud)
- `docs/` - MkDocs documentation (hosted at lance.org/integrations/spark/)
- `docker/` - Docker environment for running integration tests and notebooks
- `benchmark/` - Performance benchmarks

Source lives in `lance-spark-base_2.12/src/main/{scala,java}/`. Tests live alongside in `src/test/{scala,java}/`. The `build-helper-maven-plugin` copies both source and test directories across version modules so they don't need to be duplicated — add code once in the base module and it builds/tests against all supported Spark/Scala versions.

Key dependency: `lance-core` (Java bindings to the Lance Rust library via JNI).

## Commands

Use `./mvnw` instead of `mvn` to ensure the correct Maven version.

* Format: `./mvnw spotless:apply`
* Format (check only): `./mvnw spotless:check`
* Compile: `./mvnw compile -pl lance-spark-3.5_2.13 -am`
* Test: `./mvnw test -pl lance-spark-3.5_2.13`
* Full build: `make build SPARK_VERSION=3.5 SCALA_VERSION=2.13`

Use `-pl <module> -am` to target a specific Spark/Scala version and build its dependencies.

## Coding Standards

- Use Scala for new code unless Java is required for Spark API compatibility.
- Follow existing code style — enforced by Spotless (`checkstyle.xml`) and scalafmt (`.scalafmt.conf`).
- Use top-level imports — do not use fully qualified class names.
- Keep the connector logic thin — delegate to `lance-core` where possible.
- **Never open a Lance dataset directly.** Use `Utils.openDatasetBuilder()` to construct an `OpenDatasetBuilder` from `LanceSparkReadOptions` or `LanceSparkWriteOptions`. The builder handles storage option merging and runtime session management.

## Documentation

- **Configuration changes and new operations must update the docs.** New Spark options go in `docs/src/config.md`. New DDL/DML operations go in `docs/src/operations/`. New supported versions go in `docs/src/install.md`.
- Keep docs concise and example-driven — show the Spark SQL or DataFrame API usage.

## Testing

- **New functionality must include integration tests.** Tests should cover the happy path and key error cases — keep them concise rather than exhaustive over every edge case.
- Add tests to existing test files/suites when possible rather than creating new ones.
- Tests run against real Lance datasets (no mocking the storage layer).
- For Spark version-specific behavior, add tests in the appropriate version module.

## Git Workflow

- Never force-push commits. Append new commits instead to keep the log clean for incremental review.

## Review Guidelines

- Be concise. Focus on correctness, API consistency with Spark conventions, and test coverage.
- Verify that documentation is updated for any user-facing changes.
