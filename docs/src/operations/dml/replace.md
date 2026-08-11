# REPLACE ... WHERE

`REPLACE` atomically replaces the rows of a table that match a predicate with the result of a
query, in a single table version. It is the predicate-scoped analogue of `INSERT OVERWRITE`: rather
than replacing the whole table, it replaces only the rows selected by `WHERE`.

The delete of the matching rows and the append of the new rows are committed as one atomic Lance
`Update` operation, so readers never observe a state where the old rows are gone but the new rows
are not yet present, and a failure cannot leave the region half-written.

## Syntax

```sql
REPLACE <table> WHERE <predicate> AS <query>
```

- `<predicate>` is any SQL boolean expression over the table's columns. It selects the existing rows
  to delete.
- `<query>` is any `SELECT` producing rows with the table's schema. Its result becomes the new rows.

## Examples

=== "Spark SQL"
    ```sql
    -- Replace one day's data with freshly computed rows
    REPLACE lance.db.events
      WHERE dt = '2026-08-01'
      AS SELECT id, dt, value FROM staging_events WHERE dt = '2026-08-01';

    -- Predicates may span a range or combine conditions
    REPLACE lance.db.events
      WHERE dt >= '2026-08-01' AND dt < '2026-08-08'
      AS SELECT id, dt, value FROM staging_events;
    ```

=== "PySpark"
    ```python
    spark.sql(
        "REPLACE lance.db.events "
        "WHERE dt = '2026-08-01' "
        "AS SELECT id, dt, value FROM staging_events WHERE dt = '2026-08-01'"
    )
    ```

## Notes

- The `spark.sql.extensions` entry `org.lance.spark.extensions.LanceSparkSessionExtensions` must be
  configured, as `REPLACE` is a Lance SQL extension.
- A row filter that matches no existing rows makes `REPLACE` behave as a plain append of the query
  result.
- When a fragment contains both matching and non-matching rows, only the matching rows are removed
  (via a deletion vector); the rest are preserved. A fragment whose rows all match is dropped
  outright.
