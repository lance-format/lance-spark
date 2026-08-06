# DROP BRANCH

Remove a branch from a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `DROP BRANCH` command deletes a named branch reference from a Lance table. This removes the branch metadata entry, but does not rewrite data on `main` or on any other existing branch.

## Syntax

=== "SQL"
    ```sql
    ALTER TABLE <table> DROP BRANCH [IF EXISTS] <branch_name>;
    ```

## Example

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users DROP BRANCH IF EXISTS feature_x;
    ```

## Output

The `DROP BRANCH` command returns:

| Column | Type   | Description                    |
|--------|--------|--------------------------------|
| `name` | String | The name of the dropped branch |

## Notes and Limitations

- `DROP BRANCH` is implemented as a Spark SQL extension command.
- The target table must be a Lance table.
- `IF EXISTS` can be used when dropping a branch conditionally.
- Use [SHOW BRANCHES](./show-branches.md) to inspect existing branches before deleting one.

## See Also

- [CREATE BRANCH](./create-branch.md)
- [SHOW BRANCHES](./show-branches.md)