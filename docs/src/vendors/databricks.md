# Databricks

## Classic Compute Setup

### 1. Create a Cluster

Create a new Compute Cluster. The cluster **Access Mode** must be set to **No isolation shared**. This is required for using custom Spark extensions on Databricks. You can set this by using `Policy: Unrestricted` and configuring the access mode under **Advanced** cluster configuration.

!!!note
    This guide is tested with Databricks Runtime 16.4 LTS. Other runtimes may work but have not been tested.

### 2. Install the Lance Spark Library

The Lance Spark **bundled JAR** is the recommended artifact for Databricks. It includes all dependencies, which avoids dependency conflicts and eliminates the need to manually install additional libraries.

Navigate to **Classic Compute &rarr; \<cluster\> &rarr; Libraries &rarr; Install New** and upload the Lance Spark JAR file:

=== "Maven Central"
    Search for the Lance Spark bundle artifact on Maven Central (e.g., `org.lance:lance-spark-bundle-3.5_2.12`).

!!! note
    Some namespace implementations (e.g., to interface with external catalogs) may require additional libraries from the [lance-namespace repository](https://github.com/lance-format/lance-namespace). These are also published on Maven Central and can be installed alongside the Lance Spark bundle using the same process.

### 3. Configure Spark

Navigate to **Classic Compute &rarr; \<cluster\> &rarr; Advanced Configuration &rarr; Spark Config** to populate namespace configuration options. The catalog must use `LanceNamespaceSparkCatalog`, other catalog-specific and namespace-specific properties should be set as needed. Refer to the [Lance Spark Config docs](https://lance.org/integrations/spark/config/#example-namespace-implementations) for all available namespace implementations.

## Known Limitations

### Supported Environments

| Environment     | Catalog          | Support Status      | Notes                                                       |
|-----------------|------------------|---------------------|-------------------------------------------------------------|
| Classic Compute | Unity Catalog    | ❌ Not Supported     | Databricks uses a proprietary implementation not compatible with Spark extensions |
| Classic Compute | Hive Metastore   | ❌ Not Supported     | Vended storage credentials through Databricks are not supported |
| Classic Compute | Lance Namespace  | ✅ Supported         | Recommended approach; bypasses Databricks catalog integration |
| SQL Warehouse   | —                | ❌ Not Supported     | Does not support custom Spark datasources or SQL Extensions |

!!! warning "Databricks Catalogs"
    Using Databricks catalogs is not officially supported.

    - **Unity Catalog** &mdash; Databricks uses a proprietary Unity Catalog implementation that is not compatible with OSS Unity Catalog. Please contact Databricks for support.
    - **Hive Metastore** &mdash; Lance Spark can read and write to the Databricks legacy Hive Metastore, but vended storage credentials through Databricks are not currently supported, resulting in a cumbersome authentication process.

    The recommended approach is to use the Lance namespace catalog (`LanceNamespaceSparkCatalog`) directly, which bypasses Databricks catalog integration entirely. See [Configure Spark](#3-configure-spark) above.

### SQL Extensions Not Available

Lance SQL Extensions cannot currently be loaded in Databricks Classic Compute. The following features are unavailable:

- Creating indices
- `OPTIMIZE` / `VACUUM` commands
- `ADD COLUMNS FROM` / `UPDATE COLUMNS FROM` commands

When this is resolved, you will be able to enable extensions by adding the following to your Spark Config:

```
spark.sql.extensions org.lance.spark.extensions.LanceSparkSessionExtensions
```
