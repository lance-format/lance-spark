# SHOW INDEXES

List all indexes defined on a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `SHOW INDEXES` command returns one row for each index on a Lance table. The information is retrieved using the `Dataset.describeIndices` method, and the output columns align with the attributes of `org.lance.index.IndexDescription`, excluding the per-segment metadata list.

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

## Notes

- The `fields` column returns the logical column names from the Lance schema, ordered according to the index definition.

## See Also

- [CREATE INDEX](./create-index.md)
