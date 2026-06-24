# DROP TAG

Remove a tag from a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `DROP TAG` command deletes a named tag reference from a Lance table. This removes the tag metadata entry, but does not rewrite data on `main` or any branch.

## Syntax

=== "SQL"
    ```sql
    ALTER TABLE <table> DROP TAG [IF EXISTS] <tag_name>;
    ```

## Example

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users DROP TAG IF EXISTS release_candidate;
    ```

## Output

The `DROP TAG` command returns:

| Column | Type   | Description                |
|--------|--------|----------------------------|
| `name` | String | The name of the dropped tag |

## Notes and Limitations

- `DROP TAG` is implemented as a Spark SQL extension command.
- The target table must be a Lance table.
- `IF EXISTS` can be used when dropping a tag conditionally.
- Use [SHOW TAGS](./show-tags.md) to inspect existing tags before deleting one.

## See Also

- [CREATE TAG](./create-tag.md)
- [SHOW TAGS](./show-tags.md)
