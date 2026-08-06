# SHOW TAGS

List tags defined on a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `SHOW TAGS` command returns one row for each tag on a Lance table. It is useful for inspecting table snapshots and stable named references.

## Syntax

=== "SQL"
    ```sql
    SHOW TAGS FROM <table>;
    SHOW TAGS IN <table>;
    SHOW TAG FROM <table>;
    SHOW TAG IN <table>;
    ```

Both plural and singular forms are accepted. Both `FROM` and `IN` are accepted.

## Examples

### List all tags on a table

=== "SQL"
    ```sql
    SHOW TAGS FROM lance.db.users;
    ```

### Use the singular alias

=== "SQL"
    ```sql
    SHOW TAG IN lance.db.users;
    ```

## Output

The `SHOW TAGS` command returns the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `name` | String | Tag name |
| `branch` | String | Source branch name if the tag points to a branch version; otherwise NULL |
| `version` | Long | Version used as the tag target |
| `created_at` | Long | Tag creation timestamp as Unix epoch seconds |
| `updated_at` | Long | Tag update timestamp as Unix epoch seconds |
| `manifest_size` | Integer | Manifest size recorded for the tag |

## Notes

- `SHOW TAGS` is implemented as a Spark SQL extension command.
- The target table must be a Lance table.
- The result includes one row per tag currently registered in the table metadata.
- `created_at` and `updated_at` are returned as Unix epoch seconds.
- Use [CREATE TAG](./create-tag.md) to add a tag and [DROP TAG](./drop-tag.md) to remove one.

## See Also

- [CREATE TAG](./create-tag.md)
- [DROP TAG](./drop-tag.md)