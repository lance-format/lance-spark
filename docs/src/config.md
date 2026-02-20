# Configuration

Spark DSV2 catalog integrates with Lance through [Lance Namespace](https://github.com/lance-format/lance-namespace).

## Spark SQL Extensions

Lance provides SQL extensions that add additional functionality beyond standard Spark SQL. To enable these extensions, configure your Spark application with:

=== "PySpark"
    ```python
    spark = SparkSession.builder \
        .appName("lance-example") \
        .config("spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions") \
        .getOrCreate()
    ```

=== "Scala"
    ```scala
    val spark = SparkSession.builder()
        .appName("lance-example")
        .config("spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
        .getOrCreate()
    ```

=== "Java"
    ```java
    SparkSession spark = SparkSession.builder()
        .appName("lance-example")
        .config("spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
        .getOrCreate();
    ```

=== "Spark Shell"
    ```shell
    spark-shell \
      --packages org.lance:lance-spark-bundle-3.5_2.12:0.0.7 \
      --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions
    ```

=== "Spark Submit"
    ```shell
    spark-submit \
      --packages org.lance:lance-spark-bundle-3.5_2.12:0.0.7 \
      --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
      your-application.jar
    ```

### Features Requiring Extensions

The following features require the Lance Spark SQL extension to be enabled:

- [ADD COLUMNS with backfill](operations/dml/add-columns.md) - Add new columns and backfill existing rows with data
- [UPDATE COLUMNS with backfill](operations/dml/update-columns.md) - Update existing columns using data from a source
- [OPTIMIZE](operations/ddl/optimize.md) - Compact table fragments for improved query performance
- [VACUUM](operations/ddl/vacuum.md) - Remove old versions and reclaim storage space

## Basic Setup

Configure Spark with the `LanceNamespaceSparkCatalog` by setting the appropriate Spark catalog implementation
and namespace-specific options:

| Parameter                                   | Type   | Required | Description                                                                                                                |
|---------------------------------------------|--------|----------|----------------------------------------------------------------------------------------------------------------------------|
| `spark.sql.catalog.{name}`                  | String | ✓        | Set to `org.lance.spark.LanceNamespaceSparkCatalog`                                                                        |
| `spark.sql.catalog.{name}.impl`             | String | ✓        | Namespace implementation, short name like `dir`, `rest`, `hive3`, `glue` or full Java implementation class                 |
| `spark.sql.catalog.{name}.storage.*`        | -      | ✗        | Lance IO storage options. See [Lance Object Store Guide](https://lance.org/guide/object_store) for all available options.  |
| `spark.sql.catalog.{name}.single_level_ns`  | Boolean | ✗       | Enable single-level mode with virtual "default" namespace. Default: `false`. See [Note on Namespace Levels](#note-on-namespace-levels). |
| `spark.sql.catalog.{name}.parent`           | String | ✗        | Parent prefix for multi-level namespaces. See [Note on Namespace Levels](#note-on-namespace-levels).                             |
| `spark.sql.catalog.{name}.parent_delimiter` | String | ✗        | Delimiter for parent prefix (default: `.`). See [Note on Namespace Levels](#note-on-namespace-levels).                           |

### Arrow Allocator

The Arrow buffer allocator size can be configured via environment variable:

| Environment Variable    | Default          | Description                             |
|-------------------------|------------------|-----------------------------------------|
| `LANCE_ALLOCATOR_SIZE`  | `Long.MAX_VALUE` | Arrow allocator size in bytes (global). |

## Example Namespace Implementations

### Directory Namespace

=== "Scala"
    ```scala
    import org.apache.spark.sql.SparkSession
    
    val spark = SparkSession.builder()
        .appName("lance-dir-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "dir")
        .config("spark.sql.catalog.lance.root", "/path/to/lance/database")
        .getOrCreate()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.SparkSession;
    
    SparkSession spark = SparkSession.builder()
        .appName("lance-dir-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "dir")
        .config("spark.sql.catalog.lance.root", "/path/to/lance/database")
        .getOrCreate();
    ```

=== "Spark Shell"
    ```shell
    spark-shell \
      --packages org.lance:lance-spark-bundle-3.5_2.12:0.0.7 \
      --conf spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=dir \
      --conf spark.sql.catalog.lance.root=/path/to/lance/database
    ```

=== "Spark Submit"
    ```shell
    spark-submit \
      --packages org.lance:lance-spark-bundle-3.5_2.12:0.0.7 \
      --conf spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=dir \
      --conf spark.sql.catalog.lance.root=/path/to/lance/database \
      your-application.jar
    ```

#### Directory Configuration Parameters

| Parameter                       | Required | Description                                        |
|---------------------------------|----------|----------------------------------------------------|
| `spark.sql.catalog.{name}.root` | ✗        | Storage root location (default: current directory) |

Example settings:

=== "Local Storage"
    ```python
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("lance-dir-local-example") \
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "dir") \
        .config("spark.sql.catalog.lance.root", "/path/to/lance/database") \
        .getOrCreate()
    ```

=== "S3"
    ```python
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("lance-dir-minio-example") \
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "dir") \
        .config("spark.sql.catalog.lance.root", "s3://bucket-name/lance-data") \
        .config("spark.sql.catalog.lance.storage.access_key_id", "abc") \
        .config("spark.sql.catalog.lance.storage.secret_access_key", "def")
        .config("spark.sql.catalog.lance.storage.session_token", "ghi") \
        .getOrCreate()
    ```    

=== "MinIO"
    ```python
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("lance-dir-minio-example") \
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "dir") \
        .config("spark.sql.catalog.lance.root", "s3://bucket-name/lance-data") \
        .config("spark.sql.catalog.lance.storage.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.lance.storage.aws_allow_http", "true") \
        .config("spark.sql.catalog.lance.storage.access_key_id", "admin") \
        .config("spark.sql.catalog.lance.storage.secret_access_key", "password") \
        .getOrCreate()
    ```

### REST Namespace

Here we use LanceDB Cloud as an example of the REST namespace:

=== "PySpark"
    ```python
    spark = SparkSession.builder \
        .appName("lance-rest-example") \
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "rest") \
        .config("spark.sql.catalog.lance.headers.x-api-key", "your-api-key") \
        .config("spark.sql.catalog.lance.headers.x-lancedb-database", "your-database") \
        .config("spark.sql.catalog.lance.uri", "https://your-database.us-east-1.api.lancedb.com") \
        .getOrCreate()
    ```

=== "Scala"
    ```scala
    val spark = SparkSession.builder()
        .appName("lance-rest-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "rest")
        .config("spark.sql.catalog.lance.headers.x-api-key", "your-api-key")
        .config("spark.sql.catalog.lance.headers.x-lancedb-database", "your-database")
        .config("spark.sql.catalog.lance.uri", "https://your-database.us-east-1.api.lancedb.com")
        .getOrCreate()
    ```

=== "Java"
    ```java
    SparkSession spark = SparkSession.builder()
        .appName("lance-rest-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "rest")
        .config("spark.sql.catalog.lance.headers.x-api-key", "your-api-key")
        .config("spark.sql.catalog.lance.headers.x-lancedb-database", "your-database")
        .config("spark.sql.catalog.lance.uri", "https://your-database.us-east-1.api.lancedb.com")
        .getOrCreate();
    ```

=== "Spark Shell"
    ```shell
    spark-shell \
      --packages org.lance:lance-spark-bundle-3.5_2.12:0.0.7 \
      --conf spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=rest \
      --conf spark.sql.catalog.lance.headers.x-api-key=your-api-key \
      --conf spark.sql.catalog.lance.headers.x-lancedb-database=your-database \
      --conf spark.sql.catalog.lance.uri=https://your-database.us-east-1.api.lancedb.com
    ```

=== "Spark Submit"
    ```shell
    spark-submit \
      --packages org.lance:lance-spark-bundle-3.5_2.12:0.0.7 \
      --conf spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=rest \
      --conf spark.sql.catalog.lance.headers.x-api-key=your-api-key \
      --conf spark.sql.catalog.lance.headers.x-lancedb-database=your-database \
      --conf spark.sql.catalog.lance.uri=https://your-database.us-east-1.api.lancedb.com \
      your-application.jar
    ```

#### REST Configuration Parameters

| Parameter                            | Required | Description                                                 |
|--------------------------------------|----------|-------------------------------------------------------------|
| `spark.sql.catalog.{name}.uri`       | ✓        | REST API endpoint URL (e.g., `https://api.lancedb.com`)     |
| `spark.sql.catalog.{name}.headers.*` | ✗        | HTTP headers for authentication (e.g., `headers.x-api-key`) |

### AWS Glue Namespace

AWS Glue is Amazon's managed metastore service that provides a centralized catalog for your data assets.

=== "PySpark"
    ```python
    spark = SparkSession.builder \
        .appName("lance-glue-example") \
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "glue") \
        .config("spark.sql.catalog.lance.region", "us-east-1") \
        .config("spark.sql.catalog.lance.catalog_id", "123456789012") \
        .config("spark.sql.catalog.lance.access_key_id", "your-access-key") \
        .config("spark.sql.catalog.lance.secret_access_key", "your-secret-key") \
        .config("spark.sql.catalog.lance.root", "s3://your-bucket/lance") \
        .getOrCreate()
    ```

=== "Scala"
    ```scala
    val spark = SparkSession.builder()
        .appName("lance-glue-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "glue")
        .config("spark.sql.catalog.lance.region", "us-east-1")
        .config("spark.sql.catalog.lance.catalog_id", "123456789012")
        .config("spark.sql.catalog.lance.access_key_id", "your-access-key")
        .config("spark.sql.catalog.lance.secret_access_key", "your-secret-key")
        .config("spark.sql.catalog.lance.root", "s3://your-bucket/lance")
        .getOrCreate()
    ```

=== "Java"
    ```java
    SparkSession spark = SparkSession.builder()
        .appName("lance-glue-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "glue")
        .config("spark.sql.catalog.lance.region", "us-east-1")
        .config("spark.sql.catalog.lance.catalog_id", "123456789012")
        .config("spark.sql.catalog.lance.access_key_id", "your-access-key")
        .config("spark.sql.catalog.lance.secret_access_key", "your-secret-key")
        .config("spark.sql.catalog.lance.root", "s3://your-bucket/lance")
        .getOrCreate();
    ```

#### Additional Dependencies

Using the Glue namespace requires additional dependencies beyond the main Lance Spark bundle:
- `lance-namespace-glue`: Lance Glue namespace implementation
- AWS Glue related dependencies: The easiest way is to use `software.amazon.awssdk:bundle` which includes all necessary AWS SDK components, though you can specify individual dependencies if preferred

Example with Spark Shell:
```shell
spark-shell \
  --packages org.lance:lance-spark-bundle-3.5_2.12:0.0.7,org.lance:lance-namespace-glue:0.0.7,software.amazon.awssdk:bundle:2.20.0 \
  --conf spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.sql.catalog.lance.impl=glue \
  --conf spark.sql.catalog.lance.root=s3://your-bucket/lance
```

#### Glue Configuration Parameters

| Parameter                                    | Required | Description                                                                                                     |
|----------------------------------------------|----------|-----------------------------------------------------------------------------------------------------------------|
| `spark.sql.catalog.{name}.region`            | ✗        | AWS region for Glue operations (e.g., `us-east-1`). If not specified, derives from the default AWS region chain |
| `spark.sql.catalog.{name}.catalog_id`        | ✗        | Glue catalog ID, defaults to the AWS account ID of the caller                                                   |
| `spark.sql.catalog.{name}.endpoint`          | ✗        | Custom Glue service endpoint for connecting to compatible metastores                                            |
| `spark.sql.catalog.{name}.access_key_id`     | ✗        | AWS access key ID for static credentials                                                                        |
| `spark.sql.catalog.{name}.secret_access_key` | ✗        | AWS secret access key for static credentials                                                                    |
| `spark.sql.catalog.{name}.session_token`     | ✗        | AWS session token for temporary credentials                                                                     |
| `spark.sql.catalog.{name}.root`              | ✗        | Storage root location (e.g., `s3://bucket/path`), defaults to current directory                                 |

### Apache Hive Namespace

Lance supports both Hive 2.x and Hive 3.x metastores for metadata management.

#### Hive 3.x Namespace

=== "PySpark"
    ```python
    spark = SparkSession.builder \
        .appName("lance-hive3-example") \
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "hive3") \
        .config("spark.sql.catalog.lance.parent", "hive") \
        .config("spark.sql.catalog.lance.hadoop.hive.metastore.uris", "thrift://metastore:9083") \
        .config("spark.sql.catalog.lance.client.pool-size", "5") \
        .config("spark.sql.catalog.lance.root", "hdfs://namenode:8020/lance") \
        .getOrCreate()
    ```

=== "Scala"
    ```scala
    val spark = SparkSession.builder()
        .appName("lance-hive3-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "hive3")
        .config("spark.sql.catalog.lance.parent", "hive")
        .config("spark.sql.catalog.lance.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        .config("spark.sql.catalog.lance.client.pool-size", "5")
        .config("spark.sql.catalog.lance.root", "hdfs://namenode:8020/lance")
        .getOrCreate()
    ```

=== "Java"
    ```java
    SparkSession spark = SparkSession.builder()
        .appName("lance-hive3-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "hive3")
        .config("spark.sql.catalog.lance.parent", "hive")
        .config("spark.sql.catalog.lance.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        .config("spark.sql.catalog.lance.client.pool-size", "5")
        .config("spark.sql.catalog.lance.root", "hdfs://namenode:8020/lance")
        .getOrCreate();
    ```

#### Hive 2.x Namespace

=== "PySpark"
    ```python
    spark = SparkSession.builder \
        .appName("lance-hive2-example") \
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "hive2") \
        .config("spark.sql.catalog.lance.hadoop.hive.metastore.uris", "thrift://metastore:9083") \
        .config("spark.sql.catalog.lance.client.pool-size", "3") \
        .config("spark.sql.catalog.lance.root", "hdfs://namenode:8020/lance") \
        .getOrCreate()
    ```

=== "Scala"
    ```scala
    val spark = SparkSession.builder()
        .appName("lance-hive2-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "hive2")
        .config("spark.sql.catalog.lance.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        .config("spark.sql.catalog.lance.client.pool-size", "3")
        .config("spark.sql.catalog.lance.root", "hdfs://namenode:8020/lance")
        .getOrCreate()
    ```

=== "Java"
    ```java
    SparkSession spark = SparkSession.builder()
        .appName("lance-hive2-example")
        .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "hive2")
        .config("spark.sql.catalog.lance.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        .config("spark.sql.catalog.lance.client.pool-size", "3")
        .config("spark.sql.catalog.lance.root", "hdfs://namenode:8020/lance")
        .getOrCreate();
    ```

#### Additional Dependencies

Using Hive namespaces requires additional JARs beyond the main Lance Spark bundle:
- For Hive 2.x: `lance-namespace-hive2`
- For Hive 3.x: `lance-namespace-hive3`

Example with Spark Shell for Hive 3.x:
```shell
spark-shell \
  --packages org.lance:lance-spark-bundle-3.5_2.12:0.0.7,org.lance:lance-namespace-hive3:0.0.7 \
  --conf spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.sql.catalog.lance.impl=hive3 \
  --conf spark.sql.catalog.lance.hadoop.hive.metastore.uris=thrift://metastore:9083 \
  --conf spark.sql.catalog.lance.root=hdfs://namenode:8020/lance
```

#### Hive Configuration Parameters

| Parameter                                   | Required | Description                                                                             |
|---------------------------------------------|----------|-----------------------------------------------------------------------------------------|
| `spark.sql.catalog.{name}.hadoop.*`         | ✗        | Additional Hadoop configuration options, will override the default Hadoop configuration |
| `spark.sql.catalog.{name}.client.pool-size` | ✗        | Connection pool size for metastore clients (default: 3)                                 |
| `spark.sql.catalog.{name}.root`             | ✗        | Storage root location for Lance tables (default: current directory)                     |

## Note on Namespace Levels

Spark provides at least a 3 level hierarchy of **catalog → multi-level namespace → table**.
Most users treat Spark as a 3 level hierarchy with 1 level namespace.

### For Namespaces with Less Than 3 Levels

Some namespace implementations have a flat 2-level hierarchy of **root namespace → table**. The `LanceNamespaceSparkCatalog` provides a configuration `single_level_ns` to enable single-level mode with a virtual "default" namespace.

**DirectoryNamespace**: By default, uses multi-level namespace mode with manifest-based storage. Tables are stored with hash-prefixed paths for better scalability.

=== "Multi-level namespace (default, recommended)"
    ```python
    # Default: multi-level namespace mode with manifest-based storage
    spark = SparkSession.builder \
        .config("spark.sql.catalog.lance.impl", "dir") \
        .config("spark.sql.catalog.lance.root", "s3://bucket/lance") \
        .getOrCreate()

    # Create namespaces explicitly, then create tables
    spark.sql("CREATE NAMESPACE lance.mydb")
    spark.sql("CREATE TABLE lance.mydb.users (id INT, name STRING)")
    ```

=== "Single-level with virtual namespace"
    ```python
    # Enable single-level mode for backward compatibility
    spark = SparkSession.builder \
        .config("spark.sql.catalog.lance.impl", "dir") \
        .config("spark.sql.catalog.lance.single_level_ns", "true") \
        .getOrCreate()

    # Use the virtual "default" namespace (no CREATE NAMESPACE needed)
    spark.sql("CREATE TABLE lance.default.users (id INT, name STRING)")
    ```

**RestNamespace**: If `ListNamespaces` returns an error, `single_level_ns=true` is automatically enabled.

### For Namespaces with More Than 3 Levels

Some namespace implementations like Hive3 support more than 3 levels of hierarchy.
For example, Hive3 has a 4 level hierarchy: **root metastore → catalog → database → table**.

To handle this, the `LanceNamespaceSparkCatalog` provides `parent` and `parent_delimiter` configurations which
allow you to specify a parent prefix that gets prepended to all namespace operations.

For example, with Hive3:

- Setting `parent=hive` (using default `parent_delimiter=.`)
- When Spark requests namespace `["database1"]`, it gets transformed to `["hive.database1"]` for the API call
- This allows the 4-level Hive3 structure to work within Spark's 3-level model

The parent configuration effectively "anchors" your Spark catalog at a specific level within the deeper namespace
hierarchy, making the extra levels transparent to Spark users while maintaining compatibility with the underlying
namespace implementation.