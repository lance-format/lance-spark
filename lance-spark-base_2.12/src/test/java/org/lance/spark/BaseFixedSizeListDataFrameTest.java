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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for FixedSizeList support using DataFrame API. Tests creating Lance tables with vector
 * columns via DataFrame write operations and validates both write and read paths.
 */
public abstract class BaseFixedSizeListDataFrameTest {

  @TempDir Path tempDir;

  @Test
  public void testDataFrameWriteAndReadWithFixedSizeList() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("dataframe-fixedsizelist-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "df_vector_table";

      // Create metadata for vector column - use Long value, not String
      Metadata vectorMetadata = Metadata.fromJson("{\"arrow.fixed-size-list.size\":128}");

      // Create schema with vector column using DataFrame API
      StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("text", DataTypes.StringType, true),
                new StructField(
                    "embeddings",
                    DataTypes.createArrayType(DataTypes.FloatType, false),
                    false,
                    vectorMetadata)
              });

      // Create test data
      List<Row> rows = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        float[] vector = new float[128];
        for (int j = 0; j < 128; j++) {
          vector[j] = i * 0.01f + j * 0.001f;
        }
        rows.add(RowFactory.create(i, "text_" + i, vector));
      }

      Dataset<Row> df = spark.createDataFrame(rows, schema);

      // Write to Lance table using DataFrame API
      df.writeTo(catalogName + ".default." + tableName).using("lance").createOrReplace();

      // Read back and verify
      Dataset<Row> result = spark.table(catalogName + ".default." + tableName);
      assertEquals(10, result.count(), "Should have 10 rows");

      // Verify the data was read correctly
      Row firstRow = result.first();
      assertEquals(0, firstRow.getInt(0));
      assertEquals("text_0", firstRow.getString(1));
      // Use getSeq for cross-version compatibility
      scala.collection.Seq<Float> embeddings = firstRow.getSeq(2);
      assertEquals(128, embeddings.size(), "Embeddings should have 128 elements");

      // Verify values
      for (int i = 0; i < 10; i++) {
        float expected = i * 0.001f;
        assertEquals(expected, embeddings.apply(i), 0.0001f, "Value mismatch at index " + i);
      }

      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);

    } finally {
      spark.stop();
    }
  }

  @Test
  public void testDataFrameMultipleVectorColumns() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("dataframe-multi-vector-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "df_multi_vector";

      // Create metadata for different vector dimensions
      Metadata vec32Metadata = Metadata.fromJson("{\"arrow.fixed-size-list.size\":32}");
      Metadata vec128Metadata = Metadata.fromJson("{\"arrow.fixed-size-list.size\":128}");
      Metadata vec256Metadata = Metadata.fromJson("{\"arrow.fixed-size-list.size\":256}");

      // Create schema with multiple vector columns
      StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                new StructField(
                    "small_embedding",
                    DataTypes.createArrayType(DataTypes.FloatType, false),
                    false,
                    vec32Metadata),
                new StructField(
                    "medium_embedding",
                    DataTypes.createArrayType(DataTypes.FloatType, false),
                    false,
                    vec128Metadata),
                new StructField(
                    "large_embedding",
                    DataTypes.createArrayType(DataTypes.FloatType, false),
                    false,
                    vec256Metadata)
              });

      // Create test data
      List<Row> rows = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        float[] smallVec = new float[32];
        float[] mediumVec = new float[128];
        float[] largeVec = new float[256];

        for (int j = 0; j < 32; j++) {
          smallVec[j] = i * 0.01f + j * 0.001f;
        }
        for (int j = 0; j < 128; j++) {
          mediumVec[j] = i * 0.005f + j * 0.0005f;
        }
        for (int j = 0; j < 256; j++) {
          largeVec[j] = i * 0.002f + j * 0.0002f;
        }

        rows.add(RowFactory.create(i, "entity_" + i, smallVec, mediumVec, largeVec));
      }

      Dataset<Row> df = spark.createDataFrame(rows, schema);

      // Write to Lance table
      df.writeTo(catalogName + ".default." + tableName).using("lance").createOrReplace();

      // Read back and verify
      Dataset<Row> result = spark.table(catalogName + ".default." + tableName);
      assertEquals(5, result.count(), "Should have 5 rows");

      // Verify dimensions
      Row firstRow = result.first();
      // Use getSeq for cross-version compatibility
      scala.collection.Seq<Float> smallEmb = firstRow.getSeq(2);
      scala.collection.Seq<Float> mediumEmb = firstRow.getSeq(3);
      scala.collection.Seq<Float> largeEmb = firstRow.getSeq(4);

      assertEquals(32, smallEmb.size(), "Small embedding should have 32 elements");
      assertEquals(128, mediumEmb.size(), "Medium embedding should have 128 elements");
      assertEquals(256, largeEmb.size(), "Large embedding should have 256 elements");

      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);

    } finally {
      spark.stop();
    }
  }

  @Test
  public void testDataFrameMixedPrecisionVectors() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("dataframe-mixed-precision-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "df_mixed_precision";

      // Create metadata
      Metadata floatVecMetadata = Metadata.fromJson("{\"arrow.fixed-size-list.size\":64}");
      Metadata doubleVecMetadata = Metadata.fromJson("{\"arrow.fixed-size-list.size\":64}");

      // Create schema with float and double vectors
      StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("label", DataTypes.StringType, true),
                new StructField(
                    "float_embedding",
                    DataTypes.createArrayType(DataTypes.FloatType, false),
                    false,
                    floatVecMetadata),
                new StructField(
                    "double_embedding",
                    DataTypes.createArrayType(DataTypes.DoubleType, false),
                    false,
                    doubleVecMetadata)
              });

      // Create test data
      List<Row> rows = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        float[] floatVec = new float[64];
        double[] doubleVec = new double[64];

        for (int j = 0; j < 64; j++) {
          floatVec[j] = i * 0.1f + j * 0.01f;
          doubleVec[j] = i * 0.1 + j * 0.01;
        }

        rows.add(RowFactory.create(i, "label_" + i, floatVec, doubleVec));
      }

      Dataset<Row> df = spark.createDataFrame(rows, schema);

      // Write to Lance table
      df.writeTo(catalogName + ".default." + tableName).using("lance").createOrReplace();

      // Read back and verify
      Dataset<Row> result = spark.table(catalogName + ".default." + tableName);
      assertEquals(5, result.count(), "Should have 5 rows");

      // Verify precision is maintained
      Row firstRow = result.first();
      // Use getSeq for cross-version compatibility
      scala.collection.Seq<Float> floatEmb = firstRow.getSeq(2);
      scala.collection.Seq<Double> doubleEmb = firstRow.getSeq(3);

      assertEquals(64, floatEmb.size());
      assertEquals(64, doubleEmb.size());

      // Check precision difference
      for (int i = 0; i < 10; i++) {
        float fVal = floatEmb.apply(i);
        double dVal = doubleEmb.apply(i);
        assertEquals(i * 0.01f, fVal, 0.0001f);
        assertEquals(i * 0.01, dVal, 0.0000001);
      }

      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);

    } finally {
      spark.stop();
    }
  }
}
