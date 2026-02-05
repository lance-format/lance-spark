# UPDATE COLUMNS FROM

Lance supports updating existing columns in a dataset using data from a source,
matching rows by `_rowaddr`. Only matching rows are updated; unmatched rows in source are ignored.

This is useful for large-scale batch updates where both target and source tables are very large.
Unlike `MERGE INTO`, this operation uses Lance's `fragment.updateColumns()` API,
which preserves `_rowaddr` and `_fragid` (no row rewrite) and has lower write amplification.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled.
    See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Syntax

```sql
ALTER TABLE <table> UPDATE COLUMNS <col1>, <col2>, ... FROM <source_view>;
```

## Usage

First, create a temporary view containing the updated column values along with `_rowaddr` and `_fragid`:

```sql
CREATE TEMPORARY VIEW update_source AS
SELECT _rowaddr, _fragid, 'new_value' as name
FROM users
WHERE id = 2;

ALTER TABLE users UPDATE COLUMNS name FROM update_source;
```

Update multiple columns at once:

```sql
CREATE TEMPORARY VIEW update_source AS
SELECT _rowaddr, _fragid, 200 as value, 'updated' as name
FROM users
WHERE id = 2;

ALTER TABLE users UPDATE COLUMNS value, name FROM update_source;
```

!!! note
    The source must contain `_rowaddr` and `_fragid` columns to address the target dataset's rows.
    The columns specified in the `UPDATE COLUMNS` clause must already exist in the target table.

## Comparison with Other Operations

| Feature | UPDATE SET | UPDATE COLUMNS FROM | MERGE INTO |
|---|---|---|---|
| Matching | WHERE clause | `_rowaddr` | JOIN condition |
| Scope | All matching rows | Selected columns only | Full row operations |
| Write amplification | Rewrites affected rows | Column-level update only | Rewrites affected rows |
| Preserves `_rowaddr` | No | Yes | No |
| Batch update support | No | Yes | No |
