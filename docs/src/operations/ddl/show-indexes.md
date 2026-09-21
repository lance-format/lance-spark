# SHOW INDEXES

List all indexes defined on a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `SHOW INDEXES` command returns one row for each index on a Lance table. The information is retrieved using the `Dataset.describeIndices` method, and the output columns align with the attributes of `org.lance.index.IndexDescription`. Per-segment metadata is not listed individually; `num_segments` and `size_bytes` summarise it.

This command is useful for inspecting existing indexes, verifying index creation, and understanding the high-level properties of each index.

## Syntax

=== "SQL"
    ```sql
    SHOW INDEXES FROM multipartIdentifier;
    SHOW INDEXES IN multipartIdentifier;
    SHOW INDEX FROM multipartIdentifier;
    SHOW INDEX IN multipartIdentifier;
    ```

`multipartIdentifier` can be a fully qualified table name (for example `lance.db.users`) or a shorter form depending on the current catalog and namespace configuration.

## Examples

### List indexes on a table

List all indexes on a Lance table `lance.db.users`:

=== "SQL"
    ```sql
    SHOW INDEXES FROM lance.db.users;
    ```

You can also use the `IN` keyword or the singular `INDEX` spelling:

=== "SQL"
    ```sql
    SHOW INDEX IN lance.db.users;
    ```

## Output

The `SHOW INDEXES` command returns the following columns:

| Column                  | Type          | Description                                                        |
|-------------------------|---------------|--------------------------------------------------------------------|
| `name`                  | string        | Logical name of the index.                                         |
| `fields`                | array<string> | List of column names included in the index.                        |
| `index_type`            | string        | Human-readable index type (for example `btree`).                   |
| `num_indexed_fragments` | long          | Number of fragments fully or partially covered by the index.       |
| `num_indexed_rows`      | long          | Approximate number of rows covered by the index.                   |
| `num_unindexed_fragments` | long        | Number of fragments that are not yet indexed.                      |
| `num_unindexed_rows`    | long          | Approximate number of rows that are not yet covered by the index.  |
| `indexed_percent`       | double        | Share of rows the index covers, as a percentage truncated to two decimals, so it never overstates coverage. Null for an empty table. |
| `num_segments`          | long          | Number of physical index segments backing this logical index.       |
| `size_bytes`            | long          | Total size of all index files across the segments. Null if any segment predates index file size tracking. |

## Interpreting the Output

An `indexed_percent` below 100 means part of the table is not covered — either rows were appended
since the index was last built, or [OPTIMIZE](./optimize.md) rewrote fragments an index had
covered. Rebuild with [CREATE INDEX](./create-index.md) to restore full coverage.

`num_segments` reflects how the index was built: a distributed build produces one segment per
parallel task. Queries search every segment, and Lance can only compact fragments covered by the
identical set of segments, so a high count costs both query time and `OPTIMIZE`'s ability to
coalesce. Rebuilding with [CREATE INDEX](./create-index.md) consolidates them.

## Notes

- The `fields` column returns the logical column names from the Lance schema, ordered according to the index definition.
- Lance-maintained system indexes, including fragment-reuse and MemWAL indexes, are excluded from the output.
- Row counts are approximate, so `indexed_percent` is a guide rather than an exact figure.

## See Also

- [CREATE INDEX](./create-index.md)
