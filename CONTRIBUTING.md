# Contributing to Spark Lance Connector

The Spark Lance connector codebase is at [lance-format/lance-spark](https://github.com/lance-format/lance-spark).

## Contribution Workflow

This section describes how contributors pick up and coordinate on work. It reflects the consensus from [discussion #583](https://github.com/lance-format/lance-spark/discussions/583). The goal is visibility: the clearer we are about what we are working on, the less effort we duplicate and the less we miss.

- **Claim an issue first.** Before starting non-trivial work, comment on the issue (or self-assign if you have triage rights) so others see it is taken. A claim can go stale: with no linked PR or activity after about two weeks, anyone may pick the issue up.
- **Link every PR to its issue.** Use `Closes #NNN` or `Part of #NNN` in the PR description so related work stays visible under one issue, which is the root construct we use to reconcile differences. Trivial fixes without an issue are exempt.
- **Write a design proposal for large features.** For a new SQL extension, a new public API, or anything cross-cutting, open a [GitHub Discussion](https://github.com/lance-format/lance-spark/discussions) (the `Design Proposals` category) describing the approach before opening implementation issues or PRs: what you are solving, how it works today, the proposed change, who is affected, the risks, and an incremental/MVP plan. Authoring proposals as Discussions keeps them searchable and separates the design phase from implementation tracking, mirroring how the core [lance-format/lance](https://github.com/lance-format/lance/discussions) repo runs design proposals. A maintainer sponsors the proposal and the approach is agreed before code is written; expect initial feedback within about a week. Once the design is agreed, open the implementation issue(s) and link back to the discussion. Normal issues stay no-ceremony.
- **Keep PRs small and reviewable.** Split large efforts into an umbrella issue with a checklist, each sub-task delivered as a small PR marked `Part of #NNN`. Small PRs surface direction problems early.
- **Resolving duplicate PRs.** By default the earlier-submitted PR is reviewed first when it has no critical design flaws (first in, first served). The exception is a new contributor's PR competing with a maintainer's, where the newcomer's is prioritized absent a fundamental design problem. The maintainer records the reasoning on the issue so the decision is transparent to everyone.
- **Maintainer responsibilities.** Maintainers groom issues and PRs to limit duplication, point duplicate PRs back to the shared issue so authors can consolidate, and keep the workflow visible so contributors always know where to jump in.

## Build Commands

This connector is built using Maven. You can run the following make commands:

```shell
# Install a specific Spark/Scala version (defaults: SPARK_VERSION=3.5, SCALA_VERSION=2.12)
make install
make install SPARK_VERSION=3.4 SCALA_VERSION=2.13

# Run tests for a specific Spark/Scala version
make test
make test SPARK_VERSION=4.0 SCALA_VERSION=2.13

# Build (lint + install) for a specific version
make build

# Build the runtime bundle for a specific version (incremental)
make bundle
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.13

# Build the runtime bundle with a clean first (use when source changes are not picked up)
make clean-bundle
make clean-bundle SPARK_VERSION=3.5 SCALA_VERSION=2.13

# Install all modules (without tests)
make install-all

# Run all tests
make test-all

# Clean a specific module
make clean-module

# Clean all modules
make clean

# Show all available commands
make help
```

## Styling Guide

We use checkstyle and spotless to lint the code.

To verify style, run:

```shell
make lint
```

To auto-format the code, run:

```shell
make format
```

## Docker Integration Tests

Build the Spark bundle and Docker integration-test image before running Docker tests:

```shell
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.13
make docker-build-test SPARK_VERSION=3.5 SCALA_VERSION=2.13
make docker-test SPARK_VERSION=3.5 SCALA_VERSION=2.13
```

Use `PYTEST_CMD` to run a targeted pytest path in the Docker image. For example, run only the SQL search table-function tests against the directory namespace:

```shell
make docker-test SPARK_VERSION=3.5 SCALA_VERSION=2.13 \
  TEST_BACKENDS=local \
  PYTEST_CMD="pytest /home/lance/tests/test_lance_spark.py::TestDQLSearchTableFunctions -v --timeout=180"
```

To also validate a REST namespace backed by a directory namespace, let the Docker test container start the OSS Lance REST adapter:

```shell
make docker-test SPARK_VERSION=3.5 SCALA_VERSION=2.13 \
  TEST_BACKENDS=local,rest-dir \
  LANCE_SPARK_START_REST_DIR=true \
  LANCE_SPARK_REST_URI=http://127.0.0.1:10024 \
  PYTEST_CMD="pytest /home/lance/tests/test_lance_spark.py::TestDQLSearchTableFunctions -v --timeout=180"
```

To run against an already-running compatible REST namespace server instead, omit `LANCE_SPARK_START_REST_DIR` and pass that server's URI with `LANCE_SPARK_REST_URI`.

The `Spark Search Docker` GitHub Actions workflow runs the same targeted Docker tests. Pull requests run directory namespace and REST-directory namespace coverage automatically. Use workflow dispatch with `rest-uri` only when validating against an external REST namespace server.

## Documentation

### Setup

The documentation website is built using [mkdocs-material](https://pypi.org/project/mkdocs-material).
The build system require [uv](https://docs.astral.sh/uv/).

Start the server with:

```shell
make serve-docs
```

### Understanding the Build Process

The contents in `lance-spark/docs` are for the ease of contributors to edit and preview.
After code merge, the contents are added to the 
[main Lance documentation](https://github.com/lance-format/lance/tree/main/docs) 
during the Lance doc CI build time, and is presented in the Lance website under 
[Apache Spark integration](https://lance.org/integrations/spark).

The CONTRIBUTING.md document is auto-built to the [Lance Contributing Guide](https://lance.org/community/contributing/)

## Release Process

This section describes the CI/CD workflows for automated version management, releases, and publishing.

### Version Scheme

- **Stable releases:** `X.Y.Z` (e.g., 1.2.3)
- **Preview releases:** `X.Y.Z-beta.N` (e.g., 1.2.3-beta.1)

### Creating a Release

1. **Create Release Draft**
   - Go to Actions → "Create Release"
   - Select parameters:
     - Release type (major/minor/patch)
     - Release channel (stable/preview)
     - Dry run (test without pushing)
   - Run workflow (creates a draft release)

2. **Review and Publish**
   - Go to the [Releases page](../../releases) to review the draft
   - Edit release notes if needed
   - Click "Publish release" to:
     - For stable releases: Trigger automatic Maven Central publishing
     - For preview releases: Create a beta release (not published to Maven Central)
