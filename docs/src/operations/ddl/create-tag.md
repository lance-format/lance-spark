# CREATE TAG

Create a named tag for a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `CREATE TAG` command creates a stable named reference to a specific table state. A tag can be created from:

- the latest version of `main`
- a specific version on `main`
- the head of another branch
- a specific version on another branch
- another tag

## Syntax

=== "SQL"

    ```sql
    ALTER TABLE <table> CREATE TAG [IF NOT EXISTS] <tag_name>;

    ALTER TABLE <table> CREATE TAG [IF NOT EXISTS] <tag_name>
    AS OF VERSION <version>;

    ALTER TABLE <table> CREATE TAG [IF NOT EXISTS] <tag_name>
    AS OF BRANCH <source_branch>;

    ALTER TABLE <table> CREATE TAG [IF NOT EXISTS] <tag_name>
    AS OF BRANCH <source_branch> VERSION <version>;

    ALTER TABLE <table> CREATE TAG [IF NOT EXISTS] <tag_name>
    AS OF TAG <source_tag>;
    ```

If the `AS OF` clause is omitted, the tag is created from the latest version of `main`.

## Examples

### Create a tag from the latest `main`

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE TAG release_candidate;
    ```

### Create a tag from a specific `main` version

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE TAG IF NOT EXISTS release_candidate_v5
    AS OF VERSION 5;
    ```

### Create a tag from another branch head

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE TAG experiment_snapshot
    AS OF BRANCH experiment_a;
    ```

### Create a tag from a specific version on another branch

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE TAG experiment_snapshot_v3
    AS OF BRANCH experiment_a VERSION 3;
    ```

### Create a tag from another tag

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE TAG release_copy
    AS OF TAG release_candidate;
    ```

## Output

The `CREATE TAG` command returns:

| Column | Type   | Description            |
|--------|--------|------------------------|
| `name` | String | The name of the new tag |

## Notes and Limitations

- `CREATE TAG` is implemented as a Spark SQL extension command.
- The referenced table must be a Lance table.
- Creating a tag from a non-existent branch, tag, or version returns an error.

## See Also

- [SHOW TAGS](./show-tags.md)
- [DROP TAG](./drop-tag.md)
