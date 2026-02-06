# DROP TABLE

Remove Lance tables from the catalog.

## DROP TABLE (Deregister Only)

The standard `DROP TABLE` command removes the table from the catalog metadata but **preserves the underlying data files**. This allows the table to be re-registered later if needed.

```sql
DROP TABLE users;
```

Drop table if it exists (no error if table doesn't exist):

```sql
DROP TABLE IF EXISTS users;
```

## DROP TABLE PURGE (Delete Data)

Use `DROP TABLE ... PURGE` to completely remove the table, including both metadata and underlying data files. This operation is irreversible.

```sql
DROP TABLE users PURGE;
```

Drop and purge if exists:

```sql
DROP TABLE IF EXISTS users PURGE;
```

## Comparison

| Command | Catalog Metadata | Data Files | Recoverable |
|---------|-----------------|------------|-------------|
| `DROP TABLE` | Removed | Preserved | Yes (re-register) |
| `DROP TABLE PURGE` | Removed | Deleted | No |