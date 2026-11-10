# DESCRIBE TABLE

Inspect table structure and metadata.

Describe table structure:

```sql
DESCRIBE TABLE users;
```

Show detailed table information:

```sql
DESCRIBE EXTENDED users;
```

Spark displays fixed-size vectors as `array<float>` or `array<double>`.
Use `DESCRIBE EXTENDED` or `SHOW TBLPROPERTIES` to inspect their dimensions,
including after reloading the table:

```sql
CREATE TABLE vectors (id BIGINT, embedding ARRAY<FLOAT>)
USING lance
TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '756');

-- Reload the table so dimensions are read from the stored Lance schema.
REFRESH TABLE vectors;

DESCRIBE EXTENDED vectors;
-- Table Properties includes: embedding.arrow.fixed-size-list.size=756

SHOW TBLPROPERTIES vectors ('embedding.arrow.fixed-size-list.size');
-- value: 756
```

Vector dimensions come from the Lance schema and take precedence over table config.
Removing a config entry with `UNSET TBLPROPERTIES` leaves the vector size and
the dimension reported by `DESCRIBE EXTENDED` and `SHOW TBLPROPERTIES` unchanged.
