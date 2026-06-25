# CREATE BRANCH

Create a named branch for a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `CREATE BRANCH` command creates a new branch that points to a specific table state. A branch can be created from:

- the latest version of `main`
- a specific version on `main`
- the head of another branch
- a specific version on another branch
- a tag

Creating a branch records a new reference in table metadata. It does not duplicate the table's data files.

## Syntax

=== "SQL"

    ```sql
    ALTER TABLE <table> CREATE BRANCH [IF NOT EXISTS] <branch_name>;

    ALTER TABLE <table> CREATE BRANCH [IF NOT EXISTS] <branch_name>
    AS OF VERSION <version>;

    ALTER TABLE <table> CREATE BRANCH [IF NOT EXISTS] <branch_name>
    AS OF BRANCH <source_branch>;

    ALTER TABLE <table> CREATE BRANCH [IF NOT EXISTS] <branch_name>
    AS OF BRANCH <source_branch> VERSION <version>;

    ALTER TABLE <table> CREATE BRANCH [IF NOT EXISTS] <branch_name>
    AS OF TAG <tag_name>;
    ```

If the `AS OF` clause is omitted, the new branch is created from the latest version of `main`.

## Examples

### Create a branch from the latest `main`

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE BRANCH feature_x;
    ```

### Create a branch from a specific `main` version

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE BRANCH IF NOT EXISTS snapshot_v5
    AS OF VERSION 5;
    ```

### Create a branch from another branch head

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE BRANCH experiment_b
    AS OF BRANCH experiment_a;
    ```

### Create a branch from a specific version on another branch

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE BRANCH experiment_b_v3
    AS OF BRANCH experiment_a VERSION 3;
    ```

### Create a branch from a tag

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE BRANCH release_fix
    AS OF TAG release_candidate;
    ```

## Output

The `CREATE BRANCH` command returns:

| Column | Type   | Description                |
|--------|--------|----------------------------|
| `name` | String | The name of the new branch |

## Notes and Limitations

- `CREATE BRANCH` is implemented as a Spark SQL extension command.
- The referenced table must be a Lance table.
- Creating a branch from a non-existent branch, tag, or version returns an error.

## See Also

- [SHOW BRANCHES](./show-branches.md)
- [DROP BRANCH](./drop-branch.md)