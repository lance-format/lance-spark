# INSERT OVERWRITE

Replace all existing data in a table with new data. This operation removes all existing rows and inserts the new data atomically.

## Basic Overwrite

=== "SQL"
    ```sql
    INSERT OVERWRITE users VALUES
        (1, 'Alice', 'alice@newdomain.com'),
        (2, 'Bob', 'bob@newdomain.com');
    ```

=== "Python"
    ```python
    # Replace all data using overwrite mode
    new_df.writeTo("users").overwritePartitions()

    # Alternative: use traditional write API with overwrite mode
    new_df.write.mode("overwrite").saveAsTable("users")
    ```

=== "Scala"
    ```scala
    // Replace all data using overwrite mode
    newDF.writeTo("users").overwritePartitions()

    // Alternative: use traditional write API with overwrite mode
    newDF.write.mode("overwrite").saveAsTable("users")
    ```

=== "Java"
    ```java
    // Replace all data using overwrite mode
    newDF.writeTo("users").overwritePartitions();

    // Alternative: use traditional write API with overwrite mode
    newDF.write().mode("overwrite").saveAsTable("users");
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
    # Query source data and overwrite target
    staging_df = spark.table("staging_users")
    staging_df.writeTo("users").overwritePartitions()
    ```

=== "Scala"
    ```scala
    // Query source data and overwrite target
    val stagingDF = spark.table("staging_users")
    stagingDF.writeTo("users").overwritePartitions()
    ```

=== "Java"
    ```java
    // Query source data and overwrite target
    Dataset<Row> stagingDF = spark.table("staging_users");
    stagingDF.writeTo("users").overwritePartitions();
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
    from pyspark.sql.functions import upper, lower

    staging_df = spark.table("staging_users") \
        .filter("active = true") \
        .select("id", upper("name").alias("name"), lower("email").alias("email"))

    staging_df.writeTo("users").overwritePartitions()
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.functions.{upper, lower}

    val stagingDF = spark.table("staging_users")
        .filter("active = true")
        .select($"id", upper($"name").alias("name"), lower($"email").alias("email"))

    stagingDF.writeTo("users").overwritePartitions()
    ```

=== "Java"
    ```java
    import static org.apache.spark.sql.functions.*;

    Dataset<Row> stagingDF = spark.table("staging_users")
        .filter("active = true")
        .select(col("id"), upper(col("name")).alias("name"), lower(col("email")).alias("email"));

    stagingDF.writeTo("users").overwritePartitions();
    ```
