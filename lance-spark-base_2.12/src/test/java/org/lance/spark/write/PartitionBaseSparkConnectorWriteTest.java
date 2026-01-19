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
package org.lance.spark.write;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.spark.MemoryLanceNamespace;
import org.lance.spark.TestUtils;

import com.google.common.collect.Lists;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.lance.spark.partition.LancePartitionConfig.TABLE_ROOT;

/**
 * Base tests for partitioned Lance write using MemoryLanceNamespace via LanceNamespaceSparkCatalog.
 */
public abstract class PartitionBaseSparkConnectorWriteTest {
  private static SparkSession spark;
  private static Dataset<Row> data;
  private static String namespaceLoc;

  @BeforeAll
  static void setup() {
    namespaceLoc = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    LanceNamespace namespace = MemoryLanceNamespace.create(namespaceLoc, new HashMap<>());
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.id(Lists.asList("default", new String[0]));
    namespace.createNamespace(request);
    spark =
        SparkSession.builder()
            .appName("partitioned-spark-lance-connector-write-test")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.impl", MemoryLanceNamespace.SCHEME)
            .config("spark.sql.catalog.lance." + TABLE_ROOT, namespaceLoc)
            .getOrCreate();
    // Schema: numeric columns x, y, b, c plus partition columns country, city
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("x", DataTypes.LongType, false),
              DataTypes.createStructField("y", DataTypes.LongType, false),
              DataTypes.createStructField("b", DataTypes.LongType, false),
              DataTypes.createStructField("c", DataTypes.LongType, false),
              DataTypes.createStructField("country", DataTypes.StringType, false),
              DataTypes.createStructField("city", DataTypes.StringType, false),
            });
    List<List<Long>> expected = TestUtils.TestTable1Config.expectedValues;
    List<Row> rows = new ArrayList<>();
    // Attach simple country/city partitions ensuring multiple logical partitions.
    rows.add(
        RowFactory.create(
            expected.get(0).get(0),
            expected.get(0).get(1),
            expected.get(0).get(2),
            expected.get(0).get(3),
            "US",
            "NY"));
    rows.add(
        RowFactory.create(
            expected.get(1).get(0),
            expected.get(1).get(1),
            expected.get(1).get(2),
            expected.get(1).get(3),
            "US",
            "SF"));
    rows.add(
        RowFactory.create(
            expected.get(2).get(0),
            expected.get(2).get(1),
            expected.get(2).get(2),
            expected.get(2).get(3),
            "CN",
            "BJ"));
    rows.add(
        RowFactory.create(
            expected.get(3).get(0),
            expected.get(3).get(1),
            expected.get(3).get(2),
            expected.get(3).get(3),
            "CN",
            "SH"));
    data = spark.createDataFrame(rows, schema).repartition(4);
  }

  @AfterAll
  static void tearDown() throws IOException {
    namespaceLoc = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    MemoryLanceNamespace namespace = MemoryLanceNamespace.create(namespaceLoc, new HashMap<>());
    namespace.cleanup();
    if (spark != null) {
      spark.stop();
    }
  }

  private void createEmptyPartitionedTable(String tableName, String partitionTableRoot)
      throws TableAlreadyExistsException {
    Dataset<Row> emptyDf = data.limit(0);
    Column countryColumn = functions.col("country");
    Column bucketCityColumn = functions.bucket(4, functions.col("city"));

    emptyDf
        .writeTo(tableName)
        .tableProperty("location", partitionTableRoot)
        .partitionedBy(
            countryColumn,
            JavaConverters.asScalaBufferConverter(Arrays.asList(bucketCityColumn))
                .asScala()
                .toSeq())
        .create();
  }

  private void validateData(Dataset<Row> dataset, List<List<Long>> expectedValues) {
    List<Row> rows = dataset.collectAsList();
    assertEquals(expectedValues.size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      List<Long> actual = new ArrayList<>();
      for (int j = 0; j < row.size(); j++) {
        actual.add(row.getLong(j));
      }
      assertTrue(
          expectedValues.contains(actual), String.format("Row %s not in expected values", actual));
    }
  }

  @Test
  public void append() throws TableAlreadyExistsException, NoSuchTableException {
    String tableName = "lance.default.partition_write_append";
    String partitionTableRoot =
        String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    // Create empty partitioned table backed by MemoryLanceNamespace.
    createEmptyPartitionedTable(tableName, partitionTableRoot);
    // Append data and validate numeric columns.
    data.writeTo(tableName).append();
    Dataset<Row> read = spark.table(tableName).select("x", "y", "b", "c");
    validateData(read, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void overwrite() throws TableAlreadyExistsException, NoSuchTableException {
    String tableName = "lance.default.partition_write_overwrite";
    String partitionTableRoot =
        String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    // Create empty partitioned table and append initial data.
    createEmptyPartitionedTable(tableName, partitionTableRoot);
    data.writeTo(tableName).append();
    Dataset<Row> initial = spark.table(tableName).select("x", "y", "b", "c");
    validateData(initial, TestUtils.TestTable1Config.expectedValues);
    // Truncate table using V2 overwrite with an empty DataFrame.
    // Use a full-table overwrite predicate to simulate truncate semantics.
    data.limit(0).writeTo(tableName).overwrite(functions.lit(true));
    // Append again; only this append should remain.
    data.writeTo(tableName).append();
    Dataset<Row> afterOverwrite = spark.table(tableName).select("x", "y", "b", "c");
    validateData(afterOverwrite, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void appendErrorIfNotExist() {
    String tableName =
        "lance.default.partition_write_not_exist_" + UUID.randomUUID().toString().replace("-", "");
    Exception thrown = assertThrows(Exception.class, () -> data.writeTo(tableName).append());
    boolean isNoSuchTable = thrown instanceof NoSuchTableException;
    boolean isAnalysisException = thrown instanceof AnalysisException;
    assertTrue(
        isNoSuchTable || isAnalysisException,
        "Expected NoSuchTableException or AnalysisException but got: " + thrown.getClass());
  }
}
