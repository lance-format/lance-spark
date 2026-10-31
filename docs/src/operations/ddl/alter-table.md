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

Schema and encoding properties must be configured before the column exists: in `CREATE TABLE`
for initial columns, or with `SET TBLPROPERTIES` before `ADD COLUMNS` for future columns.
Once the column exists, SET rejects these keys, even when setting the current value:

- `<column>.arrow.fixed-size-list.size`, `<column>.arrow.float16`,
  `<column>.arrow.fixed-size-binary.byte-width`, and `<column>.arrow.large_var_char`.
- `<column>.lance.encoding` and the supported `<column>.lance.*` encoding keys:
  `compression`, `compression-level`, `structural-encoding`, `rle-threshold`, and `bss`.

For directory-backed tables, use `ALTER TABLE` to persist future-column properties because
`CREATE TABLE` does not persist user properties. See [ADD COLUMNS](../dml/add-columns.md)
for a blob column example.

`UNSET TBLPROPERTIES` can remove legacy config without changing the stored schema or encoding.
If a SET names any protected existing column, the entire statement is rejected.
Concurrent column additions can cause a commit conflict. Retry only if the columns named by
the schema or encoding properties still do not exist; otherwise, SET is rejected.

The `enable_stable_row_ids` property also can only be set at table creation;
both SET and UNSET through `ALTER TABLE` are rejected.

```sql
-- Correct: set at creation time
CREATE TABLE users (id BIGINT, name STRING)
    TBLPROPERTIES ('enable_stable_row_ids' = 'true');

-- Rejected: this property is creation-only
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
