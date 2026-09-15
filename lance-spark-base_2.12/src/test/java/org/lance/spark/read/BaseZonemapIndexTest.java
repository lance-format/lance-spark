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

import org.lance.ipc.FragmentSlice;
import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.InputPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** Minimal reproduction for zonemap index native crash. */
public abstract class BaseZonemapIndexTest {
  protected String catalogName = "lance_zonemap_test";
  protected SparkSession spark;
  protected Path rootPath;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    rootPath = tempDir.resolve(UUID.randomUUID().toString());
    rootPath.toFile().mkdirs();
    spark =
        SparkSession.builder()
            .appName("lance-zonemap-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", rootPath.toString())
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testCreateBtreeIndex() {
    String table =
        catalogName + ".default.bt_test_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(String.format("CREATE TABLE %s (id INT, region STRING) USING lance", table));
    spark.sql(String.format("INSERT INTO %s VALUES (1,'east'),(2,'west'),(3,'north')", table));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX r_idx USING btree (region)", table));

    long count = spark.sql("SELECT * FROM " + table).count();
    assertEquals(3, count);
  }

  @Test
  public void testCreateZonemapIndex() {
    String table =
        catalogName + ".default.zm_test_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(String.format("CREATE TABLE %s (id INT, region STRING) USING lance", table));
    spark.sql(String.format("INSERT INTO %s VALUES (1,'east'),(2,'west'),(3,'north')", table));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX r_idx USING zonemap (region)", table));

    // Verify index was created
    long count = spark.sql("SELECT * FROM " + table).count();
    assertEquals(3, count);
  }

  @Test
  public void testZonemapThenSecondTable() {
    String t1 = catalogName + ".default.zm_t1_" + UUID.randomUUID().toString().replace("-", "");
    String t2 = catalogName + ".default.zm_t2_" + UUID.randomUUID().toString().replace("-", "");

    // Table 1: create, insert, zonemap index
    spark.sql(String.format("CREATE TABLE %s (id INT, region STRING) USING lance", t1));
    spark.sql(String.format("INSERT INTO %s VALUES (1,'east'),(2,'west')", t1));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX r_idx USING zonemap (region)", t1));

    // Table 2: create and insert — this is where the crash happens
    spark.sql(String.format("CREATE TABLE %s (id INT, region STRING) USING lance", t2));
    spark.sql(String.format("INSERT INTO %s VALUES (3,'north'),(4,'south')", t2));

    assertEquals(2, spark.sql("SELECT * FROM " + t1).count());
    assertEquals(2, spark.sql("SELECT * FROM " + t2).count());
  }

  @Test
  public void testZonemapPhysicalSlicesForRowsAndCount() throws Exception {
    String tableName = "zm_slice_" + UUID.randomUUID().toString().replace("-", "");
    String table = catalogName + ".default." + tableName;

    spark.sql(String.format("CREATE TABLE %s (id INT, payload STRING) USING lance", table));
    spark
        .range(12)
        .selectExpr("cast(id as int) AS id", "concat('v', id) AS payload")
        .coalesce(1)
        .writeTo(table)
        .append();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX id_zm USING zonemap (id) WITH (rows_per_zone=4)", table));

    LanceScanBuilder builder =
        new LanceScanBuilder(
            spark.table(table).schema(),
            LanceSparkReadOptions.builder()
                .datasetUri(rootPath.resolve(tableName + ".lance").toString())
                .build(),
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    builder.pushPredicates(
        new Predicate[] {TestPredicates.gte("id", 4L), TestPredicates.lt("id", 8L)});
    InputPartition[] partitions = ((LanceScan) builder.build()).planInputPartitions();
    assertEquals(1, partitions.length);
    LanceSplit split = ((LanceInputPartition) partitions[0]).getLanceSplit();
    assertEquals(1, split.getFragments().size());
    assertEquals(
        Arrays.asList(new FragmentSlice(split.getFragments().get(0), 4, 4)),
        split.getFragmentSlices());

    List<Integer> ids =
        spark
            .sql(String.format("SELECT id FROM %s WHERE id >= 4 AND id < 8", table))
            .collectAsList()
            .stream()
            .map(row -> row.getInt(0))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.asList(4, 5, 6, 7), ids);

    long count =
        spark
            .sql(String.format("SELECT count(*) FROM %s WHERE id >= 4 AND id < 8", table))
            .first()
            .getLong(0);
    assertEquals(4, count);
  }

  @Test
  public void testZonemapSliceOffsetsAreNotCompactedByDeletion() throws Exception {
    String table =
        catalogName + ".default.zm_delete_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(String.format("CREATE TABLE %s (id INT) USING lance", table));
    spark.range(12).selectExpr("cast(id as int) AS id").coalesce(1).writeTo(table).append();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX id_zm USING zonemap (id) WITH (rows_per_zone=4)", table));
    spark.sql(String.format("DELETE FROM %s WHERE id = 1", table));

    assertEquals(1, spark.sql(String.format("SELECT * FROM %s WHERE id = 4", table)).count());
  }

  @Test
  public void testZonemapPhysicalSlicesWithStableRowIds() throws Exception {
    String table =
        catalogName + ".default.zm_stable_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT) USING lance "
                + "TBLPROPERTIES ('enable_stable_row_ids'='true')",
            table));
    spark.range(12).selectExpr("cast(id as int) AS id").coalesce(1).writeTo(table).append();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX id_zm USING zonemap (id) WITH (rows_per_zone=4)", table));

    assertEquals(1, spark.sql(String.format("SELECT * FROM %s WHERE id = 8", table)).count());
  }
}
