# Apache Spark Connector for Lance

The Apache Spark Connector for Lance allows Apache Spark to efficiently read datasets stored in Lance format.
By using the Apache Spark Connector for Lance, you can leverage Apache Spark's powerful data processing, SQL querying, 
and machine learning training capabilities on the AI data lake powered by Lance.

## Features

- **Read & write** Lance datasets via Spark DataSourceV2
- **Catalog integration** with `LanceNamespaceSparkCatalog` (path-based or namespace-backed via `dir` / `rest` / etc.)
- **SQL extensions**: `VACUUM`, `OPTIMIZE`, `ADD/SHOW/DROP INDEX`, `ADD/UPDATE COLUMNS ... FROM`
- **Distributed scalar index build**: `BTREE`, `ZONEMAP`, `FTS` (inverted)
- **Distributed vector index build**: `IVF_FLAT`, `IVF_PQ`, `IVF_SQ`, `IVF_HNSW_PQ`, `IVF_HNSW_SQ` — driver-side IVF centroid and PQ codebook training, executor-parallel segment build, atomic `commitExistingIndexSegments`
- **Vector search**: `VECTOR_SEARCH(<table>, <query_vector>, <k>)` table function with optional named arguments

For more details, please visit the [documentation website](https://lance.org/integrations/spark).

For development setup and contribution guidelines, please see [CONTRIBUTING.md](CONTRIBUTING.md).
