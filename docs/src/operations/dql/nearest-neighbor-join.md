# APPROX NEAREST Join

Join a query (left) table against a Lance (right) table so that each left row is matched with its
approximate _k_ nearest neighbors in the Lance table, using the Lance vector index instead of a
brute-force cross product.

This uses the `APPROX NEAREST ... BY DISTANCE` / `BY SIMILARITY` join syntax added to Spark SQL in
Spark 4.2 ([SPARK-56395](https://issues.apache.org/jira/browse/SPARK-56395)). When the right side of
the join is a Lance table and the ranking expression is a recognized vector-distance function, the
Lance Spark KNN extension rewrites the join into a single no-shuffle operator that probes the Lance
vector index directly.

!!! warning "Spark 4.2 Required"
    The `APPROX NEAREST` join syntax is only available in Spark 4.2 or later. The indexed rewrite is
    packaged in the `lance-spark-knn-4.2` module.

!!! warning "KNN Extension Required"
    The indexed rewrite requires the Lance Spark KNN SQL extension to be enabled. This is a separate
    extension from the connector's `LanceSparkSessionExtensions` — see
    [Enabling the Extension](#enabling-the-extension) below. Both can be enabled together in a
    comma-separated `spark.sql.extensions` value.

!!! note "Opt-in"
    The rewrite is off by default. It fires only when
    `spark.lance.knn.indexedNearestByJoin.enabled` is set to `true`. When it is off (or when the join
    shape is not supported), the query falls through to Spark's built-in brute-force
    `APPROX NEAREST` rewrite, so results are unchanged either way.

## Installation

The indexed rewrite ships in its own module, **separate** from the base connector artifacts — it is
built only for Spark 4.2 / Scala 2.13 (the Spark release where `APPROX NEAREST` exists). Add it
alongside the connector.

| Artifact                          | Coordinate                                      |
|-----------------------------------|-------------------------------------------------|
| KNN SQL extension (Spark 4.2)     | `org.lance:lance-spark-knn-4.2_2.13:<version>`  |
| Lance connector (Spark 4.2)       | `org.lance:lance-spark-bundle-4.2_2.13:<version>` |

Use the same `<version>` as the connector release.

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.lance</groupId>
        <artifactId>lance-spark-knn-4.2_2.13</artifactId>
        <version>VERSION</version>
    </dependency>
    ```

=== "Gradle"
    ```gradle
    dependencies {
        implementation 'org.lance:lance-spark-knn-4.2_2.13:VERSION'
    }
    ```

=== "sbt"
    ```scala
    libraryDependencies += "org.lance" % "lance-spark-knn-4.2_2.13" % "VERSION"
    ```

To supply it to a running cluster, add the coordinate to `--packages` (comma-separated, together
with the Lance connector bundle):

```shell
spark-submit \
  --packages org.lance:lance-spark-bundle-4.2_2.13:VERSION,org.lance:lance-spark-knn-4.2_2.13:VERSION \
  --conf spark.sql.extensions=org.lance.spark.knn.extensions.LanceKnnSparkSessionExtensions \
  --conf spark.lance.knn.indexedNearestByJoin.enabled=true \
  your-application.jar
```

## Enabling the Extension

=== "Scala"
    ```scala
    val spark = SparkSession.builder()
        .appName("lance-knn-example")
        .config("spark.sql.extensions",
                "org.lance.spark.knn.extensions.LanceKnnSparkSessionExtensions")
        .config("spark.lance.knn.indexedNearestByJoin.enabled", "true")
        .getOrCreate()
    ```

=== "PySpark"
    ```python
    spark = SparkSession.builder \
        .appName("lance-knn-example") \
        .config("spark.sql.extensions",
                "org.lance.spark.knn.extensions.LanceKnnSparkSessionExtensions") \
        .config("spark.lance.knn.indexedNearestByJoin.enabled", "true") \
        .getOrCreate()
    ```

=== "Spark Submit"
    ```shell
    spark-submit \
      --conf spark.sql.extensions=org.lance.spark.knn.extensions.LanceKnnSparkSessionExtensions \
      --conf spark.lance.knn.indexedNearestByJoin.enabled=true \
      your-application.jar
    ```

## Basic Usage

The right side of the join is a Lance table (loaded through the Lance data source or a Lance
namespace catalog table). The ranking expression takes the left query vector and the right table's
vector column.

=== "SQL"
    ```sql
    SELECT q.id, d.id, d.title
    FROM queries q INNER JOIN documents d
    APPROX NEAREST 10 BY DISTANCE vector_l2_distance(q.embedding, d.embedding);
    ```

`documents` must resolve to a Lance table, for example a temp view over
`spark.read.format("lance").load(...)` or a Lance namespace catalog table such as
`lance.default.documents`.

## Supported Ranking Functions

The rewrite fires only when the ranking function and the `BY` direction are consistent:

| Ranking function                          | Direction         | Lance metric |
|-------------------------------------------|-------------------|--------------|
| `vector_l2_distance(left, right)`         | `BY DISTANCE`     | `l2`         |
| `vector_cosine_similarity(left, right)`   | `BY SIMILARITY`   | `cosine`     |
| `vector_inner_product(left, right)`       | `BY SIMILARITY`   | `dot`        |

Each argument must resolve to a single column — the left query vector and the right table's vector
column. Mixed-side or computed arguments (for example `vector_l2_distance(q.vec, slice(d.vec, ...))`)
are not rewritten and fall through to Spark's brute-force path.

Only `APPROX` joins are rewritten. An exact (`EXACT`) nearest join is always handled by Spark's
brute-force rewrite.

## WHERE Pushdown

A `WHERE` clause on the right (Lance) side is translated into a Lance filter and applied by the
index **before** the top-_k_ search (a prefilter), so the neighbors are drawn only from rows matching
the filter:

=== "SQL"
    ```sql
    SELECT q.id, d.id
    FROM queries q
    INNER JOIN (SELECT * FROM documents WHERE category = 'news' AND score > 5) d
    APPROX NEAREST 10 BY DISTANCE vector_l2_distance(q.embedding, d.embedding);
    ```

Translation is conservative. It supports comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`), `IN`,
`IS [NOT] NULL`, and `AND` / `OR` / `NOT` over the right table's columns (top-level or nested struct
fields) compared against literals. If any part of the predicate cannot be translated (UDFs,
subqueries, computed expressions, or a reference to a left-side column), the rewrite is refused
entirely and the query falls through to the brute-force path — the predicate is never partially
applied.

## Tuning

These options tune the Lance index search. Both are optional; when unset, Lance's index defaults
apply.

| Configuration                            | Type    | Description                                                                                       |
|------------------------------------------|---------|---------------------------------------------------------------------------------------------------|
| `spark.lance.knn.indexedNearestByJoin.enabled` | Boolean | Enable the indexed rewrite. Default `false`.                                                |
| `spark.lance.knn.nprobes`                | Integer | Number of IVF partitions to probe per query. Higher improves recall at more compute.              |
| `spark.lance.knn.refineFactor`           | Integer | IVF-PQ refine factor. Lance fetches `k * refineFactor` candidates and re-ranks them with exact distance. Highest-leverage recall knob for IVF-PQ. |

## Snapshot Consistency

The Lance table version is resolved and pinned once on the driver before the join runs. Every
partition probes that same snapshot, so a concurrent write to the Lance table does not change the
result mid-query. A `version` / `branch` supplied through the Lance read options (for example
`spark.read.format("lance").option("branch", "...")`) is honored.

## Execution

The rewrite produces a single physical operator that runs as one no-shuffle `mapPartitions` over the
left input. No `Exchange` is inserted above it. Each task opens the Lance vector index once, then for
every left row probes the index, keeps the top _k_, and late-materializes the surviving right rows by
row id. Because there is no shuffle or broadcast of the right table, the right table is opened per
task rather than moved across the network.

## Validation

The `lance-spark-knn-4.2` module covers this path with unit tests for the Catalyst rewrite rule and
end-to-end SQL tests that build a Lance IVF-PQ index, run `APPROX NEAREST` through the indexed
operator, and check recall against a brute-force oracle — including `WHERE` pushdown and the
rule-disabled fallthrough.
