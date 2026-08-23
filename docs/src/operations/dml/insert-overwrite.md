# INSERT OVERWRITE

Replace all existing data in a table with new data. This operation removes all existing rows and inserts the new data atomically.

## Schema Preservation Semantics

Full-table overwrite operations that use Spark's truncate capability—such as `INSERT OVERWRITE`, `writeTo(...).overwrite(lit(true))`, and `mode("overwrite").save(...)`—write data with the existing table's Arrow schema instead of rebuilding it solely from Spark-expressible types.

This preserves schema-level semantics such as unsigned types, `FixedSizeList`, `FixedSizeBinary`, field nullability, and field metadata. If the existing schema cannot be loaded or is structurally incompatible with the incoming DataFrame schema, the overwrite fails before executor writers start.

When `use_large_var_types=true`, variable-width string and binary fields are intentionally promoted to `LargeUtf8` and `LargeBinary`; executor fragments and the committed manifest use the same promoted schema.

!!! note
    Lance-Spark supports full-table overwrite, but not dynamic partition overwrite. `writeTo(...).overwritePartitions()` fails during Spark analysis. Use `writeTo(...).overwrite(lit(true))` to replace the entire table.

!!! note
    `mode("overwrite").saveAsTable(...)`, `REPLACE TABLE`, and `CREATE OR REPLACE TABLE` use Spark's staged table-replacement path. They may rebuild the schema from the incoming DataFrame and are not governed by the schema-preserving overwrite semantics above.

!!! warning
    Existing nullability is enforced. If a nullable struct contains non-nullable children, a row whose parent struct is null cannot be written because Arrow must place nulls in the child vectors. The write fails without relaxing the declared `NOT NULL` constraint.

## Basic Overwrite

=== "SQL"
    ```sql
    INSERT OVERWRITE users VALUES
        (1, 'Alice', 'alice@newdomain.com'),
        (2, 'Bob', 'bob@newdomain.com');
    ```

=== "Python"
    ```python
    from pyspark.sql.functions import lit

    # Full-table overwrite of an existing catalog table
    new_df.writeTo("users").overwrite(lit(True))

    # Full-table overwrite of a path-based Lance dataset
    new_df.write \
        .format("lance") \
        .mode("overwrite") \
        .save(users_uri)
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.functions.lit

    // Full-table overwrite of an existing catalog table
    newDF.writeTo("users").overwrite(lit(true))

    // Full-table overwrite of a path-based Lance dataset
    newDF.write
        .format("lance")
        .mode("overwrite")
        .save(usersUri)
    ```

=== "Java"
    ```java
    import static org.apache.spark.sql.functions.lit;

    // Full-table overwrite of an existing catalog table
    newDF.writeTo("users").overwrite(lit(true));

    // Full-table overwrite of a path-based Lance dataset
    newDF.write()
        .format("lance")
        .mode("overwrite")
        .save(usersUri);
    ```

## Overwrite from SELECT

Replace table data using a query result:

=== "SQL"
    ```sql
    INSERT OVERWRITE users
    SELECT id, name, email FROM staging_users;
    ```

=== "Python"
    ```python
    from pyspark.sql.functions import lit

    staging_df = spark.table("staging_users")
    staging_df.writeTo("users").overwrite(lit(True))
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.functions.lit

    val stagingDF = spark.table("staging_users")
    stagingDF.writeTo("users").overwrite(lit(true))
    ```

=== "Java"
    ```java
    import static org.apache.spark.sql.functions.lit;

    Dataset<Row> stagingDF = spark.table("staging_users");
    stagingDF.writeTo("users").overwrite(lit(true));
    ```

## Overwrite with Transformation

Replace data after applying transformations:

=== "SQL"
    ```sql
    INSERT OVERWRITE users
    SELECT id, UPPER(name) as name, LOWER(email) as email
    FROM staging_users
    WHERE active = true;
    ```

=== "Python"
    ```python
    from pyspark.sql.functions import lit, lower, upper

    staging_df = spark.table("staging_users") \
        .filter("active = true") \
        .select("id", upper("name").alias("name"), lower("email").alias("email"))

    staging_df.writeTo("users").overwrite(lit(True))
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.functions.{lit, lower, upper}

    val stagingDF = spark.table("staging_users")
        .filter("active = true")
        .select($"id", upper($"name").alias("name"), lower($"email").alias("email"))

    stagingDF.writeTo("users").overwrite(lit(true))
    ```

=== "Java"
    ```java
    import static org.apache.spark.sql.functions.*;

    Dataset<Row> stagingDF = spark.table("staging_users")
        .filter("active = true")
        .select(col("id"), upper(col("name")).alias("name"), lower(col("email")).alias("email"));

    stagingDF.writeTo("users").overwrite(lit(true));
    ```
