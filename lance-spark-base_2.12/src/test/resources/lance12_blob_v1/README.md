# Lance 12 blob compatibility fixtures

These immutable datasets were produced with Lance **12.0.0**, originally for
[PR #885](https://github.com/lance-format/lance-spark/pull/885).
Do not regenerate them using the dependency version under test: that would remove
the legacy encoding that these tests must exercise.

| Dataset | Producer | Contents |
| --- | --- | --- |
| `top_level.lance` | pylance 12.0.0 | `id`, blob-v1 `data`; rows `(1, one)`, `(2, two)` in a 2.1 file, `(3, three)` in a 2.2 file. BTree `id_idx` covers only the first fragment. |
| `nested.lance` | pylance 12.0.0 | `id`, struct `payload.data` with blob-v1 metadata; `(1, one)` in 2.1, `(2, two)` in 2.2; no index. |
| `spark_append_22.lance` | lance-spark 0.9.0-beta.2 / Lance 12.0.0 | Created at 2.1; two rows `(1, 0x01)`, `(2, 0x02)` appended at 2.2. The manifest still reports 2.1. |

`datagen.py` reproduces the first two datasets with `pylance==12.0.0` and PyArrow.
The third can be generated with PySpark and a Lance 12 connector bundle:

```python
from pathlib import Path
from pyspark.sql import SparkSession

root = Path("/tmp/lance12-fixtures").resolve()
uri = root / "spark_append_22.lance"
spark = (SparkSession.builder.master("local[2]")
    .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
    .config("spark.sql.catalog.lance.impl", "dir")
    .config("spark.sql.catalog.lance.root", str(root)).getOrCreate())
table = f"lance.`{uri}`"
spark.sql(f"""CREATE TABLE {table} (id INT NOT NULL, data BINARY) USING lance
    TBLPROPERTIES ('data.lance.encoding'='blob', 'file_format_version'='2.1')""")
(spark.sql("SELECT 1 AS id, X'01' AS data UNION ALL SELECT 2 AS id, X'02' AS data")
    .writeTo(table).option("file_format_version", "2.2").append())
spark.stop()
```

Use a fresh output directory and a matching Spark/Scala bundle. The byte layout and
manifest/index UUIDs need not be identical when regenerated; the rows, blob
metadata, file versions and partial index coverage must be identical.

The JUnit suites copy each fixture into a fresh temporary directory per case,
execute a real Spark operation, reopen the dataset, and compare IDs and every
blob's bytes. Compaction and index refresh must perform actual work. Spark 3.5+
also exercises UPDATE and MERGE. Nested stored bytes are checked through Lance's
blob API; Spark's nested blob materialization limitation is tracked in
[#852](https://github.com/lance-format/lance-spark/issues/852).

Run with the repository's JDK 17 toolchain:

```sh
./mvnw -pl lance-spark-3.5_2.13 -am test \
  -Dtest=Lance12BlobCompatibilityTest,Lance12BlobRowLevelTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The normal Spark CI matrix runs the shared compatibility suite on all supported
Spark/Scala modules. The tests intentionally do not select expectations based on
the Lance dependency version: an upgrade must retain the same compatibility.
