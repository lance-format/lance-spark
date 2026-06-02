# SHOW BRANCHES

List branches defined on a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `SHOW BRANCHES` command returns one row for each branch on a Lance table. It is useful for inspecting branch lineage, verifying branch creation, and understanding which version each branch points to.

## Syntax

=== "SQL"
    ```sql
    SHOW BRANCHES FROM <table>;
    SHOW BRANCHES IN <table>;
    SHOW BRANCH FROM <table>;
    SHOW BRANCH IN <table>;
    ```

Both plural and singular forms are accepted. Both `FROM` and `IN` are accepted.

## Examples

### List all branches on a table

=== "SQL"
    ```sql
    SHOW BRANCHES FROM lance.db.users;
    ```

### Use the singular alias

=== "SQL"
    ```sql
    SHOW BRANCH IN lance.db.users;
    ```

## Output

The `SHOW BRANCHES` command returns the following columns:

| Column | Type | Description                                                                      |
|--------|------|----------------------------------------------------------------------------------|
| `name` | String | Branch name                                                                      |
| `parent_branch` | String | Source branch name if the branch was created from another branch; otherwise NULL |
| `parent_version` | Long | Version used as the branch starting point                                        |
| `created_at` | Long | Branch creation timestamp as returned by Lance metadata                          |
| `manifest_size` | Integer | Manifest size recorded for the branch                                            |

## Notes

- `SHOW BRANCHES` is implemented as a Spark SQL extension command.
- The target table must be a Lance table.
- The result includes one row per branch currently registered in the table metadata.
- Use [CREATE BRANCH](./create-branch.md) to add a branch and [DROP BRANCH](./drop-branch.md) to remove one.

## See Also

- [CREATE BRANCH](./create-branch.md)
- [DROP BRANCH](./drop-branch.md)