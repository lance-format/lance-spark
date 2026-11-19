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
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
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
  public void testZonemapWithoutBoundsKeepsMatchingRows() {
    // Lance reports no zone bounds for decimals; such zones must not be treated as all-null.
    String table = catalogName + ".default.zm_dec_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(String.format("CREATE TABLE %s (id INT, x DECIMAL(10,2)) USING lance", table));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 1.23), (2, 4.56)", table));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX x_zm USING zonemap (x)", table));
    assertEquals(
        Collections.singletonList(RowFactory.create(1)),
        spark.sql(String.format("SELECT id FROM %s WHERE x = 1.23", table)).collectAsList());
  }

  @Test
  public void testZonemapKeepsRowsOfSupplementaryCharacterStrings() {
    // Lance orders string bounds by code point, so U+1F600 is this zone's max, while Java's UTF-16
    // order puts it below U+FFFD.
    String table = catalogName + ".default.zm_str_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(String.format("CREATE TABLE %s (id INT, s STRING) USING lance", table));
    // One fragment, so that one zone holds both values.
    spark.sql(
        String.format(
            "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, '\uFFFD'),"
                + " (2, '\uD83D\uDE00') AS v(id, s)",
            table));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX s_zm USING zonemap (s)", table));
    assertEquals(
        Collections.singletonList(RowFactory.create(1)),
        spark.sql(String.format("SELECT id FROM %s WHERE s = '\uFFFD'", table)).collectAsList());
    assertEquals(
        Collections.singletonList(RowFactory.create(1)),
        spark
            .sql(String.format("SELECT id FROM %s WHERE s IN ('\uFFFD', 'a')", table))
            .collectAsList());
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
    // A second fragment is fully pruned, while the first retains one physical slice.
    spark
        .range(12, 16)
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
    LanceScan scan = (LanceScan) builder.build();
    assertEquals("2", JavaConverters.mapAsJavaMap(scan.getMetaData()).get("originalFragmentCount"));
    assertEquals("2", JavaConverters.mapAsJavaMap(scan.getMetaData()).get("numPartitions"));
    InputPartition[] partitions = scan.planInputPartitions();
    assertEquals(1, partitions.length);
    assertEquals("2", JavaConverters.mapAsJavaMap(scan.getMetaData()).get("originalFragmentCount"));
    assertEquals("1", JavaConverters.mapAsJavaMap(scan.getMetaData()).get("numPartitions"));
    LanceSplit split = ((LanceInputPartition) partitions[0]).getLanceSplit();
    assertEquals(1, split.getFragments().size());
    assertEquals(
        Arrays.asList(new FragmentSlice(split.getFragments().get(0), 4, 4)),
        split.getFragmentSlices());

    // Arrow consumers copy a planned scan. Preserve its physical slices through that copy,
    // and verify both reader paths return only the selected rows.
    for (LanceScan readerScan : Arrays.asList(scan, scan.withArrowBatches())) {
      InputPartition[] readerPartitions = readerScan.planInputPartitions();
      assertEquals(1, readerPartitions.length);
      assertEquals(
          split.getFragmentSlices(),
          ((LanceInputPartition) readerPartitions[0]).getLanceSplit().getFragmentSlices());
      // Exercise the reader's slice contract independently of the residual predicate.
      LanceInputPartition sliced = (LanceInputPartition) readerPartitions[0];
      LanceInputPartition unfilteredSlice =
          new LanceInputPartition(
              sliced.getSchema(),
              sliced.getPartitionId(),
              sliced.getLanceSplit(),
              sliced.getReadOptions(),
              Optional.empty(),
              sliced.getLimit(),
              sliced.getOffset(),
              sliced.getTopNSortOrders(),
              sliced.getPushedAggregation(),
              sliced.getScanId(),
              sliced.getInitialStorageOptions(),
              sliced.getNamespaceImpl(),
              sliced.getNamespaceProperties(),
              null);
      List<Integer> readIds = new ArrayList<>();
      try (PartitionReader<ColumnarBatch> reader =
          readerScan.createReaderFactory().createColumnarReader(unfilteredSlice)) {
        while (reader.next()) {
          ColumnarBatch batch = reader.get();
          for (int row = 0; row < batch.numRows(); row++) {
            readIds.add(batch.column(0).getInt(row));
          }
        }
      }
      assertEquals(Arrays.asList(4, 5, 6, 7), readIds);
      try (LanceCountStarPartitionReader reader =
          new LanceCountStarPartitionReader(unfilteredSlice)) {
        assertTrue(reader.next());
        assertEquals(4L, reader.get().column(0).getLong(0));
        assertFalse(reader.next());
      }
    }

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
  public void testLegacyZonemapFallsBackToFullFragment() {
    String tableName = "zm_legacy_" + UUID.randomUUID().toString().replace("-", "");
    String table = catalogName + ".default." + tableName;
    String uri = rootPath.resolve(tableName + ".lance").toString();
    spark
        .range(12)
        .coalesce(1)
        .write()
        .format("lance")
        .option("file_format_version", "0.1")
        .save(uri);
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX id_zm USING zonemap (id) WITH (rows_per_zone=4)", table));

    LanceScanBuilder builder =
        new LanceScanBuilder(
            spark.table(table).schema(),
            LanceSparkReadOptions.from(uri),
            Collections.emptyMap(),
            null,
            Collections.emptyMap());
    builder.pushPredicates(
        new Predicate[] {TestPredicates.gte("id", 4L), TestPredicates.lt("id", 8L)});
    InputPartition[] partitions = ((LanceScan) builder.build()).planInputPartitions();
    assertEquals(1, partitions.length);
    assertTrue(((LanceInputPartition) partitions[0]).getLanceSplit().getFragmentSlices().isEmpty());
    assertEquals(
        Arrays.asList(
            RowFactory.create(4L),
            RowFactory.create(5L),
            RowFactory.create(6L),
            RowFactory.create(7L)),
        spark
            .sql("SELECT id FROM " + table + " WHERE id >= 4 AND id < 8 ORDER BY id")
            .collectAsList());
    assertEquals(
        4L,
        spark
            .sql("SELECT count(*) FROM " + table + " WHERE id >= 4 AND id < 8")
            .first()
            .getLong(0));
  }

  @Test
  public void testZonemapStringRangesUseUtf8Ordering() {
    String tableName = "zm_utf8_" + UUID.randomUUID().toString().replace("-", "");
    String table = catalogName + ".default." + tableName;
    spark
        .range(8)
        .selectExpr("id", "CASE WHEN id < 4 THEN '\uD800\uDC00' ELSE '\uF000' END AS s")
        .coalesce(1)
        .write()
        .format("lance")
        .save(rootPath.resolve(tableName + ".lance").toString());
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX s_zm USING zonemap (s) WITH (rows_per_zone=4)", table));
    String predicate = " WHERE s > '\uE000'";
    assertEquals(
        Arrays.asList(
            RowFactory.create(0L),
            RowFactory.create(1L),
            RowFactory.create(2L),
            RowFactory.create(3L),
            RowFactory.create(4L),
            RowFactory.create(5L),
            RowFactory.create(6L),
            RowFactory.create(7L)),
        spark.sql("SELECT id FROM " + table + predicate + " ORDER BY id").collectAsList());
    assertEquals(8L, spark.sql("SELECT count(*) FROM " + table + predicate).first().getLong(0));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testZonemapSliceOffsetsAreNotCompactedByDeletion(boolean stableRowIds)
      throws Exception {
    String tableName = "zm_delete_" + UUID.randomUUID().toString().replace("-", "");
    String table = catalogName + ".default." + tableName;

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT) USING lance "
                + "TBLPROPERTIES ('enable_stable_row_ids'='%s')",
            table, stableRowIds));
    spark.range(12).selectExpr("cast(id as int) AS id").coalesce(1).writeTo(table).append();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX id_zm USING zonemap (id) WITH (rows_per_zone=4)", table));
    // Delete before and within the selected zone. Its physical offsets must remain [4, 8).
    spark.sql(String.format("DELETE FROM %s WHERE id IN (1, 6)", table));

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
    LanceScan scan = (LanceScan) builder.build();
    InputPartition[] partitions = scan.planInputPartitions();
    assertEquals(1, partitions.length);
    LanceSplit split = ((LanceInputPartition) partitions[0]).getLanceSplit();
    assertEquals(
        Collections.singletonList(new FragmentSlice(split.getFragments().get(0), 4, 4)),
        split.getFragmentSlices());

    List<Integer> ids =
        spark
            .sql(String.format("SELECT id FROM %s WHERE id >= 4 AND id < 8", table))
            .collectAsList()
            .stream()
            .map(row -> row.getInt(0))
            .sorted()
            .collect(Collectors.toList());
    // Keeping the zone's final row catches offsets incorrectly compacted after deletion.
    assertEquals(Arrays.asList(4, 5, 7), ids);
    assertEquals(
        3L,
        spark
            .sql(String.format("SELECT count(*) FROM %s WHERE id >= 4 AND id < 8", table))
            .first()
            .getLong(0));
  }
}
