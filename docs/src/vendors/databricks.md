# Databricks

## Supported Environments

| Environment   | Support Status  | Notes                                                              |
|---------------|-----------------|--------------------------------------------------------------------|
| Classic Compute | ✅ Supported    | Supports custom Spark datasources                                  |
| SQL Warehouse | ❌ Not Supported | Does not support custom Spark datasources or SQL Extensions        |

## Catalog Support

| Catalog           | Support Status    | Notes                                                                                                    |
|-------------------|-------------------|----------------------------------------------------------------------------------------------------------|
| Hive Metastore    | ✅ Supported       | Available via Databricks legacy Hive Metastore support                                                   |
| Unity Catalog     | ❌ Not Yet Supported | Databricks uses a custom Unity Catalog implementation that is not compatible with OSS Unity Catalog. We are working with Databricks to add support. |

## Classic Compute Setup

### 1. Create a Cluster

Create a new cluster using **Databricks Runtime 16.4 LTS**. Other runtimes may work but have not been tested.

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

Navigate to **Classic Compute &rarr; \<cluster\> &rarr; Configuration &rarr; Spark Config** and add the following to register the Lance catalog:

```
spark.sql.catalog.<catalog_name> = org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.defaultCatalog = <catalog_name>
```

Replace `<catalog_name>` with your desired catalog name.

#### Storage Credentials

Because Unity Catalog is not supported, storage credentials cannot be inherited from the Databricks environment. You must provide them explicitly, either at the catalog level in Spark Config or per read/write operation.

To configure credentials at the catalog level (S3 example):

```
spark.sql.catalog.<catalog_name>.storage.access_key_id=YOUR_ACCESS_KEY
spark.sql.catalog.<catalog_name>.storage.secret_access_key=YOUR_SECRET_KEY
spark.sql.catalog.<catalog_name>.storage.session_token=YOUR_SESSION_TOKEN   # optional, for temporary credentials
spark.sql.catalog.<catalog_name>.storage.region=us-east-1
spark.sql.catalog.<catalog_name>.storage.endpoint=https://s3.amazonaws.com  # or custom S3-compatible endpoint
spark.sql.catalog.<catalog_name>.storage.aws_allow_http=false               # set true for local MinIO etc.
```

You can configure credentials for any [supported storage system](https://lance.org/guide/object_store) in a similar way using the appropriate options.

## Example Usage

### DataFrame API

```python
import pyarrow as pa

# Create a table
table = pa.Table.from_pylist([
    {"name": "Alice", "age": 20},
    {"name": "Bob", "age": 30},
    {"name": "David", "age": 42},
])

df = spark.createDataFrame(table)

# Write a table
df.write \
    .format("lance") \
    .option("access_key_id", "YOUR_ACCESS_KEY") \
    .option("secret_access_key", "YOUR_SECRET_KEY") \
    .option("region", "us-east-1") \
    .option("endpoint", "https://s3.amazonaws.com") \
    .saveAsTable("hive_metastore.default.my_table", path="s3://my-bucket/my-table")

# Read a table
new_df = spark.read \
    .option("access_key_id", "YOUR_ACCESS_KEY") \
    .option("secret_access_key", "YOUR_SECRET_KEY") \
    .option("region", "us-east-1") \
    .option("endpoint", "https://s3.amazonaws.com") \
    .table("hive_metastore.default.my_table") \
    .collect()
```

!!! tip
    If you configured storage credentials at the catalog level in Spark Config, you can omit the `.option(...)` calls for credentials in each read/write operation.

### SQL

```python
# Create a table
spark.sql("""
CREATE TABLE hive_metastore.default.my_table (
    id INT,
    name STRING
) USING lance
LOCATION 's3://my-bucket/my-table';
""")

# List tables
spark.sql("SHOW TABLES IN hive_metastore.default").show()

# Describe a table
spark.sql("DESCRIBE TABLE hive_metastore.default.my_table").show()

# Drop a table
spark.sql("DROP TABLE hive_metastore.default.my_table")
```

## Known Issues

### Authentication

The common Databricks pattern where storage credentials are inherited from Unity Catalog does not work with Lance Spark, because Unity Catalog is not currently supported. Users must provide storage credentials explicitly (e.g., AWS access key and secret key) as options when reading and writing tables, or configure them at the catalog level in Spark Config.

### SQL Extensions Not Available

Lance SQL Extensions cannot currently be loaded in Databricks Classic Compute. The following features are unavailable:

- Creating indices
- `OPTIMIZE` / `VACUUM` commands
- `ADD COLUMNS FROM` / `UPDATE COLUMNS FROM` commands

When this is resolved, you will be able to enable extensions by adding the following to your Spark Config:

```
spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions
```
