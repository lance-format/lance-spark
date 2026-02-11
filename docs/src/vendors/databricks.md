# Databricks

## Supported Environments

| Environment     | Catalog          | Support Status      | Notes                                                       |
|-----------------|------------------|---------------------|-------------------------------------------------------------|
| Classic Compute | Unity Catalog    | ❌ Not Supported     | Databricks uses a proprietary implementation not compatible with Spark extensions |
| Classic Compute | Hive Metastore   | ⚠️ Not Recommended  | Vended storage credentials through Databricks are not supported |
| Classic Compute | Lance Namespace  | ✅ Supported         | Recommended approach; bypasses Databricks catalog integration |
| SQL Warehouse   | —                | ❌ Not Supported     | Does not support custom Spark datasources or SQL Extensions |

!!! warning "Databricks Catalogs"
    Using Databricks catalogs is not officially supported.

    - **Unity Catalog** &mdash; Databricks uses a proprietary Unity Catalog implementation that is not compatible with OSS Unity Catalog. We are working with Databricks to add support.
    - **Hive Metastore** &mdash; Lance Spark can read and write to the Databricks legacy Hive Metastore, but vended storage credentials through Databricks are not currently supported, resulting in a cumbersome authentication process.

    The recommended approach is to use the Lance namespace catalog (`LanceNamespaceSparkCatalog`) directly, which bypasses Databricks catalog integration entirely. See [Configure Spark](#3-configure-spark) below.

## Classic Compute Setup

### 1. Create a Cluster

Create a new cluster using **Databricks Runtime 16.4 LTS**. Other runtimes may work but have not been tested.

The cluster **Access Mode** must be set to **No isolation shared**. This is required for using custom Spark extensions on Databricks. You can set this by using `Policy: Unrestricted` and configuring the access mode under **Advanced** cluster configuration.

### 2. Install the Lance Spark Library

The Lance Spark **bundled JAR** is the recommended artifact for Databricks. It includes all dependencies, which avoids dependency conflicts and eliminates the need to manually install additional libraries.

Navigate to **Classic Compute &rarr; \<cluster\> &rarr; Libraries &rarr; Install New** and choose one of the following methods:

=== "Maven Central"
    Search for the Lance Spark bundle artifact on Maven Central (e.g., `org.lance:lance-spark-bundle-3.5_2.12`).

=== "Upload JAR"
    Upload the bundled JAR directly to Databricks. You can download a release from Maven Central, or build from source:

    ```shell
    SPARK_VERSION=<version> SCALA_VERSION=<version> make bundle
    ```

    See the [lance-spark repository](https://github.com/lance-format/lance-spark) for more details.

### 3. Configure Spark

Configure the catalog to use `LanceNamespaceSparkCatalog`, then set catalog-specific and namespace-specific properties as needed. Refer to the [Lance Spark Config docs](https://lance.org/integrations/spark/config/#example-namespace-implementations) for all available namespace implementations.

!!! note
    Some namespace implementations may require additional libraries from the [lance-namespace repository](https://github.com/lance-format/lance-namespace). These are also published on Maven Central and can be installed alongside the Lance Spark bundle using the same process described in [Install the Lance Spark Library](#2-install-the-lance-spark-library).

Navigate to **Classic Compute &rarr; \<cluster\> &rarr; Advanced Configuration &rarr; Spark Config** to populate namespace configuration options. To use the `LanceNamespaceSparkCatalog` with S3 storage, you can set the following properties, replacing values as needed:

```
spark.sql.catalog.<catalog_name> org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.<catalog_name>.impl dir
spark.sql.catalog.<catalog_name>.root s3://my-bucket/my-tables
spark.sql.catalog.<catalog_name>.storage.access_key_id <ACCESS_KEY>
spark.sql.catalog.<catalog_name>.storage.secret_access_key <SECRET_KEY>
spark.sql.catalog.<catalog_name>.storage.region us-east-1
spark.sql.defaultCatalog <catalog_name>
```

## Example Usage
Interacting with Lance Spark on Databricks is the same as using any Spark datasource. You can use either SQL or the DataFrame API to create, read, and manipulate Lance tables.

### SQL

```python
# Create a table
spark.sql("""
CREATE TABLE lancedb.default.my_table (
    id INT,
    name STRING
) USING lance
""")

# List tables
spark.sql("SHOW TABLES IN lancedb.default").show()

# Describe a table
spark.sql("DESCRIBE TABLE lancedb.default.my_table").show()

# Insert data
spark.sql("INSERT INTO lancedb.default.my_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'David')")

# Query data
spark.sql("SELECT * FROM lancedb.default.my_table").show()

# Drop a table
spark.sql("DROP TABLE lancedb.default.my_table")
```

### DataFrame API

```python
import pyarrow as pa

table = pa.Table.from_pylist([
    {"name": "Alice", "age": 20},
    {"name": "Bob", "age": 30},
    {"name": "David", "age": 42},
])

df = spark.createDataFrame(table)

# Write a table
df.write \
    .format("lance") \
    .saveAsTable("lancedb.default.my_table")

# Read a table
new_df = spark.read \
    .table("lancedb.default.my_table") \
    .collect()
```

## Known Issues

### SQL Extensions Not Available

Lance SQL Extensions cannot currently be loaded in Databricks Classic Compute. The following features are unavailable:

- Creating indices
- `OPTIMIZE` / `VACUUM` commands
- `ADD COLUMNS FROM` / `UPDATE COLUMNS FROM` commands

When this is resolved, you will be able to enable extensions by adding the following to your Spark Config:

```
spark.sql.extensions org.lance.spark.extensions.LanceSparkSessionExtensions
```

### Authentication

Databricks-native authentication mechanisms that rely on Unity Catalog (e.g., External Volumes, vended credentials) are not compatible with Lance Spark. Storage credentials must be managed explicitly using the available [Lance Spark storage configuration options](https://lance.org/integrations/spark/config/#example-namespace-implementations) (e.g., AWS access key and secret key for S3).
