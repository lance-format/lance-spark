# OPTIMIZE

Compact table fragments to improve query performance and reduce storage overhead.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

Over time, as data is appended to Lance tables, the number of fragments (data files) can grow, which may impact query performance. The `OPTIMIZE` command compacts these fragments into larger, more efficient files.

## Basic Usage

=== "SQL"
    ```sql
    OPTIMIZE lance.db.users;
    ```

## Options

The `OPTIMIZE` command supports several options to control compaction behavior:

| Option | Type | Description |
|--------|------|-------------|
| `target_rows_per_fragment` | Long | Target number of rows per fragment after compaction |
| `max_rows_per_group` | Long | Maximum rows per row group within a fragment |
| `max_bytes_per_file` | Long | Maximum bytes per data file |
| `materialize_deletions` | Boolean | Whether to materialize soft deletes during compaction |
| `materialize_deletions_threshold` | Float | Threshold ratio for materializing deletions |
| `num_threads` | Long | Number of threads for compaction |
| `batch_size` | Long | Batch size for processing |
| `defer_index_remap` | Boolean | Whether to defer index remapping |
| `max_source_fragments` | Long | Maximum number of source fragments to compact in a single task |
| `compaction_mode` | String | How data is rewritten: `reencode` (default), `try_binary_copy`, or `force_binary_copy` |
| `binary_copy_read_batch_bytes` | Long | Read batch size in bytes used during binary-copy compaction |

### Compaction modes

The `compaction_mode` option controls how fragments are rewritten:

- `reencode` (default) — decode and re-encode the data.
- `try_binary_copy` — binary-copy compatible fragments (skipping decode/re-encode), falling back to `reencode` otherwise. Much cheaper when fragments are compatible.
- `force_binary_copy` — use binary copy, or fail if fragments are not compatible.

### Examples

Optimize with a specific target rows per fragment:

=== "SQL"
    ```sql
    OPTIMIZE lance.db.users WITH (target_rows_per_fragment = 1000000);
    ```

Optimize with multiple options:

=== "SQL"
    ```sql
    OPTIMIZE lance.db.users WITH (
        target_rows_per_fragment = 1000000,
        max_rows_per_group = 10000,
        materialize_deletions = TRUE,
        num_threads = 4
    );
    ```

Optimize using binary copy when fragments are compatible:

=== "SQL"
    ```sql
    OPTIMIZE lance.db.users WITH (compaction_mode = 'try_binary_copy');
    ```

## Output

The `OPTIMIZE` command returns statistics about the compaction operation:

| Column | Type | Description |
|--------|------|-------------|
| `fragments_removed` | Long | Number of fragments removed |
| `fragments_added` | Long | Number of new fragments created |
| `files_removed` | Long | Number of data files removed |
| `files_added` | Long | Number of new data files created |

## When to Optimize

Consider running OPTIMIZE when:

- Many small appends have created numerous fragments
- Query performance has degraded over time
- You want to reduce the number of files in storage
- After running many deletes (to materialize soft deletes)

## How It Works

Lance stores data in fragments. Each write operation (INSERT, append) creates new fragments. The OPTIMIZE command:

1. Identifies small fragments that can be combined
2. Merges them into larger, more efficient fragments
3. Updates the table manifest to point to the new fragments
4. Removes the old fragments

This process is safe and does not block concurrent reads.
