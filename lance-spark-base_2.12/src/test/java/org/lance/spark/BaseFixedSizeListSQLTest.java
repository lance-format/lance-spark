/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for FixedSizeList support using SQL API. Tests creating Lance tables with vector columns via
 * SQL CREATE TABLE and INSERT statements.
 */
public abstract class BaseFixedSizeListSQLTest {

  @TempDir Path tempDir;

  @Test
  public void testCreateTableWithFixedSizeListVectorSQL() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("fixedsizelist-sql-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "vector_table_sql";

      // Create table with vector column using TBLPROPERTIES
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "text STRING, "
              + "embeddings ARRAY<FLOAT> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'embeddings.arrow.fixed-size-list.size' = '128'"
              + ")");

      // Verify table was created
      spark.sql("SHOW TABLES IN " + catalogName + ".default").show();

      // Get the schema with metadata
      StructType schema = spark.table(catalogName + ".default." + tableName).schema();
      assertNotNull(schema);
      assertEquals(3, schema.fields().length);

      // Verify the embeddings field has the correct metadata
      StructField embeddingsField = schema.apply("embeddings");
      assertNotNull(embeddingsField);
      Metadata metadata = embeddingsField.metadata();

      if (metadata.contains("arrow.fixed-size-list.size")) {
        long size = metadata.getLong("arrow.fixed-size-list.size");
        assertEquals(128L, size);
      }

      // Insert data using SQL
      spark.sql(
          "INSERT INTO "
              + catalogName
              + ".default."
              + tableName
              + " VALUES "
              + "(1, 'test_text', array("
              + java.util.stream.IntStream.range(0, 128)
                  .mapToObj(i -> String.valueOf(i * 0.01f))
                  .collect(java.util.stream.Collectors.joining(", "))
              + "))");

      // Read back the data
      Dataset<Row> result = spark.sql("SELECT * FROM " + catalogName + ".default." + tableName);
      assertEquals(1, result.count(), "Should have 1 row");

      Row firstRow = result.first();
      assertEquals(1, firstRow.getInt(0));
      assertEquals("test_text", firstRow.getString(1));

      // Verify the array was read correctly
      // Use getSeq for cross-version compatibility
      scala.collection.Seq<Float> embeddings = firstRow.getSeq(2);
      assertEquals(128, embeddings.size(), "Embeddings should have 128 elements");

      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);

    } finally {
      spark.stop();
    }
  }

  @Test
  public void testCreateTableWithMultipleVectorColumnsSQL() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("multi-vector-sql-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "multi_vector_table_sql";

      // Create table with multiple vector columns of different dimensions
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "title STRING, "
              + "title_embeddings ARRAY<FLOAT> NOT NULL, "
              + "content_embeddings ARRAY<FLOAT> NOT NULL, "
              + "summary_embeddings ARRAY<FLOAT> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'title_embeddings.arrow.fixed-size-list.size' = '64', "
              + "'content_embeddings.arrow.fixed-size-list.size' = '256', "
              + "'summary_embeddings.arrow.fixed-size-list.size' = '128'"
              + ")");

      // Verify table was created
      StructType schema = spark.table(catalogName + ".default." + tableName).schema();
      assertNotNull(schema);
      assertEquals(5, schema.fields().length);

      // Insert test data using SQL
      StringBuilder insertSQL = new StringBuilder();
      insertSQL.append("INSERT INTO ").append(catalogName).append(".default.").append(tableName);
      insertSQL.append(" VALUES (1, 'doc1', ");

      // title_embeddings: 64d
      insertSQL.append("array(");
      for (int i = 0; i < 64; i++) {
        if (i > 0) insertSQL.append(", ");
        insertSQL.append(i * 0.01f);
      }
      insertSQL.append("), ");

      // content_embeddings: 256d
      insertSQL.append("array(");
      for (int i = 0; i < 256; i++) {
        if (i > 0) insertSQL.append(", ");
        insertSQL.append(i * 0.002f);
      }
      insertSQL.append("), ");

      // summary_embeddings: 128d
      insertSQL.append("array(");
      for (int i = 0; i < 128; i++) {
        if (i > 0) insertSQL.append(", ");
        insertSQL.append(i * 0.005f);
      }
      insertSQL.append("))");

      spark.sql(insertSQL.toString());

      // Verify data
      Dataset<Row> result = spark.sql("SELECT * FROM " + catalogName + ".default." + tableName);
      assertEquals(1, result.count());

      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);

    } finally {
      spark.stop();
    }
  }

  @Test
  public void testMixedPrecisionVectorsSQL() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("mixed-precision-sql-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "mixed_precision_sql";

      // Create table with float and double vector columns
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "float_vec ARRAY<FLOAT> NOT NULL, "
              + "double_vec ARRAY<DOUBLE> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'float_vec.arrow.fixed-size-list.size' = '32', "
              + "'double_vec.arrow.fixed-size-list.size' = '32'"
              + ")");

      // Insert data with different precision vectors
      spark.sql(
          "INSERT INTO "
              + catalogName
              + ".default."
              + tableName
              + " VALUES "
              + "(1, array("
              + java.util.stream.IntStream.range(0, 32)
                  .mapToObj(i -> String.valueOf(i * 0.1f))
                  .collect(java.util.stream.Collectors.joining(", "))
              + "), array("
              + java.util.stream.IntStream.range(0, 32)
                  .mapToObj(i -> String.valueOf(i * 0.01))
                  .collect(java.util.stream.Collectors.joining(", "))
              + "))");

      // Verify
      Dataset<Row> result = spark.sql("SELECT * FROM " + catalogName + ".default." + tableName);
      assertEquals(1, result.count());

      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);

    } finally {
      spark.stop();
    }
  }
}
