# Contributing to Spark Lance Connector

The Spark Lance connector codebase is at [lancedb/lance-spark](https://github.com/lance-format/lance-spark).

## Build Commands

This connector is built using Maven. You can run the following make commands:

```shell
# Build all
make build

# Clean all
make clean

# Build Spark 3.5 Scala 2.12
make build-35-212

# Clean build of Spark 3.5 Scala 2.12
make clean-35-212

# Build the runtime bundle of Spark 3.5 Scala 2.12
make bundle-35-212
```

## Styling Guide

We use checkstyle and spotless to lint the code.

All the `make build` commands automatically perform `spotless:apply` to the code.

To verify style, run:

```shell
make lint
```

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
