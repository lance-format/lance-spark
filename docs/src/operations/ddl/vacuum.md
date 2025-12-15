# VACUUM

Remove old versions and unreferenced data files from Lance tables to reclaim storage space.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

Lance maintains multiple versions of data for time travel and transactional consistency. Over time, old versions accumulate and can consume significant storage space. The `VACUUM` command removes old versions and their associated data files that are no longer needed.

## Basic Usage

=== "SQL"
    ```sql
    VACUUM lance.db.users;
    ```

## Options

The `VACUUM` command supports several options to control cleanup behavior:

| Option | Type | Description |
|--------|------|-------------|
| `before_version` | Long | Remove versions older than this version number |
| `before_timestamp_millis` | Long | Remove versions older than this timestamp (milliseconds since epoch) |
| `delete_unverified` | Boolean | Whether to delete unverified files (default: false) |
| `error_if_tagged_old_versions` | Boolean | Error if old versions have tags (default: false) |

### Examples

Remove versions older than a specific version:

=== "SQL"
    ```sql
    VACUUM lance.db.users WITH (before_version = 10);
    ```

Remove versions older than a specific timestamp:

=== "SQL"
    ```sql
    VACUUM lance.db.users WITH (before_timestamp_millis = 1704067200000);
    ```

Use multiple options:

=== "SQL"
    ```sql
    VACUUM lance.db.users WITH (
        before_version = 10,
        delete_unverified = TRUE
    );
    ```

## Output

The `VACUUM` command returns statistics about the cleanup operation:

| Column | Type | Description |
|--------|------|-------------|
| `bytes_removed` | Long | Total bytes removed from storage |
| `old_versions` | Long | Number of old versions removed |

## When to Vacuum

Consider running VACUUM when:

- Storage costs are increasing due to accumulated versions
- You no longer need historical versions for time travel
- After running many updates or deletes that created new versions

## Important Notes

- **Irreversible**: VACUUM permanently deletes data. Removed versions cannot be recovered.
- **Time travel**: After vacuuming, you cannot time travel to removed versions.
- **Tags**: If you have tagged versions, use `error_if_tagged_old_versions` to prevent accidental deletion of important snapshots.
- **Concurrent operations**: VACUUM is safe to run alongside read operations but should not run concurrently with writes.
