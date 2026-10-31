# ALTER TABLE

Changes the schema or properties of a table.

## SET TBLPROPERTIES

Add or update key-value properties on a table:

```sql
ALTER TABLE users SET TBLPROPERTIES ('team' = 'data-eng', 'env' = 'production');
```

If a property already exists, its value is overwritten:

```sql
ALTER TABLE users SET TBLPROPERTIES ('env' = 'staging');
```

## UNSET TBLPROPERTIES

Remove properties from a table:

```sql
ALTER TABLE users UNSET TBLPROPERTIES ('env');
```

Remove multiple properties at once:

```sql
ALTER TABLE users UNSET TBLPROPERTIES ('team', 'env');
```

Unsetting a property that does not exist is a no-op (no error is raised).

## DROP COLUMN

Remove one or more columns. The Lance field is dropped, and schema or encoding properties for that
column are removed. Other properties stay. Nested names use the same quoted field path as other
Lance commands. Quote a name that contains a dot. The last example drops that literal child and
leaves the nested path in place.

```sql
ALTER TABLE users DROP COLUMN name;

ALTER TABLE users DROP COLUMNS name, city;

ALTER TABLE users DROP COLUMN IF EXISTS missing_column;

ALTER TABLE users DROP COLUMN address.city;

ALTER TABLE users DROP COLUMN s.`a.b`;
```

`DROP COLUMN IF EXISTS` does nothing when the column is absent, including leaving a property that
was stored for a column that has not been added yet.

Dropping every remaining column fails before the table changes. The SQL error is:

```text
Cannot drop all columns from a Lance table. At least one column must remain.
```

A column that belongs to the unenforced primary key cannot be dropped. That declaration is
write-once and stays in place.

A column that still has an index cannot be dropped. Drop the index first:

```sql
ALTER TABLE users DROP INDEX name_idx;
ALTER TABLE users DROP COLUMN name;
```

This command updates metadata only. The removed column's bytes stay in existing data files until
optimize and vacuum.

## Limitations

The `enable_stable_row_ids` property controls stable row ID tracking in the Lance format and can only be set at table creation time via `TBLPROPERTIES` in `CREATE TABLE`. Changing it via `ALTER TABLE` updates the stored config value but does **not** change the actual row ID tracking behavior.

```sql
-- Correct: set at creation time
CREATE TABLE users (id BIGINT, name STRING)
    TBLPROPERTIES ('enable_stable_row_ids' = 'true');

-- Has no behavioral effect after creation
ALTER TABLE users SET TBLPROPERTIES ('enable_stable_row_ids' = 'true');
```

## Rename Table

Rename a table within the same namespace:

```sql
ALTER TABLE users RENAME TO new_users;
```

Rename a table to a different namespace:

```sql
ALTER TABLE ns1.users RENAME TO ns2.new_users;
```

!!! note
Rename is only supported when using a namespace-based catalog (`impl=rest`).
Directory-based catalogs do not support table renames.

## Error Behavior

| Scenario | Error |
|----------|-------|
| Source table does not exist | `TABLE_OR_VIEW_NOT_FOUND` |
| Target table name already exists | `TABLE_ALREADY_EXISTS` |
| Directory-based catalog | `UnsupportedOperationException` |
