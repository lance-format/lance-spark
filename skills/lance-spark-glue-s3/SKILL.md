---
name: lance-spark-glue-s3
description: Use this skill when configuring, testing, debugging, or documenting lance-spark with AWS Glue Data Catalog and S3, including Spark catalog options, Docker integration tests, CI Glue/S3 workflows, and code references for namespace-backed Lance tables.
---

# Lance Spark Glue/S3

## Overview

Use this skill when a task involves Lance Spark running against AWS Glue Data Catalog with S3 storage. It is intended for agents changing or validating this repository, and for agents helping users reproduce the Glue/S3 setup.

## Workflow

1. Read `references/glue-s3.md` before making configuration, CI, Docker, or integration-test changes for Glue/S3.
2. Prefer the repository's current Makefile and workflow commands over ad hoc Spark invocations.
3. For real AWS validation, confirm the setup uses AWS Glue and S3 directly, not MinIO or a local S3-compatible endpoint.
4. When updating instructions, keep detailed content in `references/glue-s3.md`; keep this `SKILL.md` short.

## References

- `references/glue-s3.md`: Glue/S3 configuration, Docker test flow, CI checks, code map, Python integration-test use cases, and troubleshooting.
