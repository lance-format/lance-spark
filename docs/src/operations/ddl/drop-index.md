# DROP INDEX

Remove an index from a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Usage

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users DROP INDEX idx_id;
    ```

## Output

The `DROP INDEX` command returns the following information:

| Column       | Type   | Description                     |
|--------------|--------|---------------------------------|
| `index_name` | String | The name of the dropped index.  |
| `status`     | String | The result status (`dropped`).  |

## How It Works

The `DROP INDEX` command removes the named index from the dataset manifest. Physical index files are **not** deleted immediately; they are cleaned up by the [`VACUUM`](vacuum.md) command during garbage collection. This allows older dataset versions to continue referencing the index for time travel.

## Notes and Limitations

- Dropping a non-existent index results in an error.
- Index names are case-insensitive (stored as lowercase).
- To recreate an index after dropping, use [`CREATE INDEX`](create-index.md).
- To list existing indexes before dropping, use [`SHOW INDEXES`](show-indexes.md).
