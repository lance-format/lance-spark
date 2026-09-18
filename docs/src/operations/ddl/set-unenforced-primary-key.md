# SET UNENFORCED PRIMARY KEY

Declare primary key columns on a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `SET UNENFORCED PRIMARY KEY` command records the primary key columns of a table as schema field metadata. Uniqueness is **not** enforced: no write path validates the declared columns, so duplicate values are accepted.

## Syntax

=== "SQL"
    ```sql
    ALTER TABLE <table> SET UNENFORCED PRIMARY KEY (<column> [, <column> ...]);
    ```

Column order is significant and is recorded alongside the declaration.

## Examples

### Single-column primary key

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users SET UNENFORCED PRIMARY KEY (id);
    ```

### Composite primary key

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users SET UNENFORCED PRIMARY KEY (id, name);
    ```

## Output

The `SET UNENFORCED PRIMARY KEY` command returns the following information:

| Column                | Type   | Description                                                  |
|-----------------------|--------|--------------------------------------------------------------|
| `status`              | String | The result status (`OK`).                                    |
| `primary_key_columns` | String | The declared columns, comma-separated, in the order given.    |

## How It Works

The command commits a new table version that attaches `lance-schema:unenforced-primary-key` field metadata to each declared column, together with its one-based position in the key. Because the change is a normal commit, earlier versions keep their original schema and remain available for time travel.

## Notes and Limitations

- The declaration is **write-once**. Setting a primary key on a table that already has one fails; it cannot be changed or removed afterwards.
- Columns must be non-nullable. Declare them `NOT NULL` when running [`CREATE TABLE`](create-table.md).
- Columns must be primitive leaf fields. Struct and other nested columns are rejected.
- The same column may not be listed twice.
- Branch and tag identifiers are read-only, so the command is rejected against them.
