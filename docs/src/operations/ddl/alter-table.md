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

## Limitations

The `enable_stable_row_ids` property controls stable row ID tracking in the Lance format and can only be set at table creation time via `TBLPROPERTIES` in `CREATE TABLE`. Changing it via `ALTER TABLE` updates the stored config value but does **not** change the actual row ID tracking behavior.

```sql
-- Correct: set at creation time
CREATE TABLE users (id BIGINT, name STRING)
    TBLPROPERTIES ('enable_stable_row_ids' = 'true');

-- Has no behavioral effect after creation
ALTER TABLE users SET TBLPROPERTIES ('enable_stable_row_ids' = 'true');
```

## ADD COLUMN

Add one or more top-level columns. New columns are appended and filled with `NULL` for existing rows:

```sql
ALTER TABLE users ADD COLUMN age INT;
ALTER TABLE users ADD COLUMNS (age INT, email STRING);
```

## DROP COLUMN

Remove a column from the table:

```sql
ALTER TABLE users DROP COLUMN age;
ALTER TABLE users DROP COLUMN IF EXISTS age;
```

## RENAME COLUMN

Rename a column:

```sql
ALTER TABLE users RENAME COLUMN name TO full_name;
```

## ALTER COLUMN

Change a column's nullability:

```sql
ALTER TABLE users ALTER COLUMN id DROP NOT NULL;
```

!!! note
Column schema evolution operates on top-level columns. Each `ALTER TABLE` is committed as a single
atomic Lance operation against the current schema, so one statement may batch changes of the same
kind that target distinct columns (e.g. adding several columns), but it may not mix column
additions, drops, and alterations, combine a column change with `TBLPROPERTIES` changes, target the
same column more than once, or apply a change that depends on an earlier change in the same
statement (such as altering a column by its just-assigned new name) — issue those as separate
statements. The whole request is validated first, so if any change is unsupported, none is applied.
The following are **not** currently supported and are rejected before any change is written:

- Adding a column at a specific position (`FIRST`/`AFTER`) — columns are always appended.
- Adding a column with a `DEFAULT` value — new columns are filled with `NULL`.
- Adding a column to a legacy-format (`file_format_version='LEGACY'`) table.
- Changing a column's data type (`ALTER COLUMN ... TYPE`) or updating a column comment.

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
