# ADD COLUMNS FROM

Similar to most table formats, Lance supports traditional schema evolution: 
adding, removing, and altering columns in a dataset. 
Most of these operations can be performed without rewriting the data files in the dataset, 
making them very efficient operations. 

In addition, Lance supports data evolution, 
which allows you to also backfill existing rows with the new column data without rewriting the data files in the dataset, 
making it highly suitable for use cases like ML feature engineering.
This feature is implemented in Spark as `ALTER TABLE ADD COLUMNS FROM`

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. 
    See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

Example:

```sql
CREATE TEMPORARY VIEW tmp_view
AS
SELECT _rowaddr, _fragid, hash(name) as name_hash
FROM users;

ALTER TABLE users ADD COLUMNS name_hash FROM tmp_view;
```

No table rewrite, no data movement—just a new column that is instantly queryable.

## Blob v2 columns

To add a blob v2 column, store the target blob encoding property on the table and
use a Lance file format version that supports blob v2 before running
`ADD COLUMNS FROM`:

```sql
CREATE TABLE media (
  id INT
) USING lance
TBLPROPERTIES (
  'file_format_version' = '2.2'
);

INSERT INTO media VALUES (1), (2);

ALTER TABLE media SET TBLPROPERTIES ('content.lance.encoding' = 'blob');

CREATE TEMPORARY VIEW media_with_bytes AS
SELECT 1 AS id, X'68656C6C6F' AS image_bytes
UNION ALL
SELECT 2 AS id, X'776F726C64' AS image_bytes;

CREATE TEMPORARY VIEW content_backfill AS
SELECT _rowaddr, _fragid, image_bytes AS content
FROM media
JOIN media_with_bytes USING (id);

ALTER TABLE media ADD COLUMNS content FROM content_backfill;
```

The source expression for the new column should be `BINARY`. When adding a new
column from an existing blob v2 Lance table, a direct column select also works:

```sql
CREATE TEMPORARY VIEW copied_content AS
SELECT target._rowaddr, target._fragid, source.content AS content
FROM media target
JOIN source_media source ON target.id = source.id;

ALTER TABLE media ADD COLUMNS content FROM copied_content;
```

For an existing table that was already created with `file_format_version = '2.2'`
or newer, set the new column's blob property before adding the column:

```sql
ALTER TABLE media SET TBLPROPERTIES ('content.lance.encoding' = 'blob');
ALTER TABLE media ADD COLUMNS content FROM content_backfill;
```

!!! note
    Because we use `_rowaddr` and `_fragid` to address the target dataset's rows for the new column's data, 
    the temporary view should contain `_rowaddr` and `_fragid`.
