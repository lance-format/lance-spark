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
package org.lance.spark.read;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.spark.MemoryLanceNamespace;
import org.lance.spark.TestUtils;

import com.google.common.collect.Lists;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.lance.spark.partition.LancePartitionConfig.TABLE_ROOT;

public abstract class PartitionBaseSparkConnectorReadTest {
  private static SparkSession spark;
  private static Dataset<Row> data;
  private static String namespaceLoc;

  @BeforeAll
  static void setup() throws NoSuchTableException, TableAlreadyExistsException {
    namespaceLoc = String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());
    LanceNamespace namespace = MemoryLanceNamespace.create(namespaceLoc, new HashMap<>());
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.id(Lists.asList("default", new String[0]));
    namespace.createNamespace(request);

    spark =
        SparkSession.builder()
            .appName("partitioned-spark-lance-connector-read-test")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.impl", MemoryLanceNamespace.SCHEME)
            .config("spark.sql.catalog.lance." + TABLE_ROOT, namespaceLoc)
            .getOrCreate();

    String tableName = "lance.default.partition_read_test";
    String partitionTableRoot =
        String.format("%s://%s/", MemoryLanceNamespace.SCHEME, UUID.randomUUID());

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

    Dataset<Row> df = spark.createDataFrame(rows, schema).repartition(4);

    // Create empty partitioned table backed by MemoryLanceNamespace.
    Dataset<Row> emptyDf = df.limit(0);
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

    // Append data so that leaf datasets are created and populated.
    df.writeTo(tableName).append();

    // Access numeric columns from the partitioned table.
    data = spark.table(tableName).select("x", "y", "b", "c");
    data.createOrReplaceTempView("test_dataset1");
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
  public void readAll() {
    validateData(data, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void filter() {
    validateData(
        data.filter("x > 1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .collect(Collectors.toList()));
    validateData(
        data.filter("y == 4"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(1) == 4)
            .collect(Collectors.toList()));
    validateData(
        data.filter("b >= 6"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(2) >= 6)
            .collect(Collectors.toList()));
    validateData(
        data.filter("c < -1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(3) < -1)
            .collect(Collectors.toList()));
    validateData(
        data.filter("c <= -1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(3) <= -1)
            .collect(Collectors.toList()));
    validateData(
        data.filter("c == -2"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(3) == -2)
            .collect(Collectors.toList()));
    validateData(
        data.filter("x > 1").filter("y < 6"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .filter(row -> row.get(1) < 6)
            .collect(Collectors.toList()));
    validateData(
        data.filter("x > 1 and y < 6"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .filter(row -> row.get(1) < 6)
            .collect(Collectors.toList()));
    validateData(
        data.filter("x > 1 or y < 6"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> (row.get(0) > 1) || (row.get(1) < 6))
            .collect(Collectors.toList()));
    validateData(
        data.filter("(x >= 1 and x <= 2) or (c >= -2 and c < 0)"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(
                row -> (row.get(0) >= 1 && row.get(0) <= 2) || (row.get(3) >= -2 && row.get(3) < 0))
            .collect(Collectors.toList()));
  }

  @Test
  public void select() {
    validateData(
        data.select("y", "b"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .collect(Collectors.toList()));
  }

  @Test
  public void filterSelect() {
    validateData(
        data.select("y", "b").filter("y > 3"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .filter(row -> row.get(0) > 3)
            .collect(Collectors.toList()));
  }

  @Test
  public void supportBroadcastJoin() {
    Dataset<Row> df = spark.range(0, 4).toDF("x");
    df.createOrReplaceTempView("test_dataset3");

    List<Row> desc =
        spark
            .sql("explain select t1.* from test_dataset1 t1 join test_dataset3 t3 on t1.x = t3.x")
            .collectAsList();

    assertTrue(
        desc.stream().anyMatch(row -> row.getString(0).contains("BroadcastHashJoin")),
        "Expected BroadcastHashJoin in the physical plan");
  }
}
