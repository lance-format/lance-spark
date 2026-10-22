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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Verifies that a declared unenforced primary key makes concurrent MERGE inserts of the same new
 * keys dedupe instead of duplicate (the fix that records inserted keys in the Update's
 * KeyExistenceFilter), and that disjoint concurrent inserts still commit without a conflict.
 */
public class MergeUnenforcedPkConcurrencyTest {

  private SparkSession spark;
  @TempDir Path tempDir;
  private String table;

  @BeforeEach
  public void setup() throws IOException {
    Path root = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(root);
    spark =
        SparkSession.builder()
            .appName("merge-unenforced-pk-concurrency")
            .master("local[4]")
            .config("spark.sql.catalog.lance_test", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog.lance_test.impl", "dir")
            .config("spark.sql.catalog.lance_test.root", root.toString())
            .config("spark.sql.catalog.lance_test.single_level_ns", "true")
            .getOrCreate();
    table = "lance_test.default.merge_pk_race_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(
        String.format(
            "CREATE TABLE %s (id1 STRING NOT NULL, id2 STRING NOT NULL, payload STRING) USING lance",
            table));
    spark.sql(String.format("ALTER TABLE %s SET UNENFORCED PRIMARY KEY (id1, id2)", table));
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  private String mergeSql(String sourceView) {
    return String.format(
        "MERGE INTO %s AS t USING %s AS s ON t.id1 = s.id1 AND t.id2 = s.id2 "
            + "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        table, sourceView);
  }

  /** Races the two statements, released together to maximise overlap; returns thrown errors. */
  private List<Throwable> race(String sqlA, String sqlB) throws InterruptedException {
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    for (String sql : Arrays.asList(sqlA, sqlB)) {
      pool.submit(
          () -> {
            try {
              start.await();
              spark.sql(sql);
            } catch (Throwable t) {
              errors.add(t);
            }
          });
    }
    start.countDown();
    pool.shutdown();
    Assertions.assertTrue(pool.awaitTermination(180, TimeUnit.SECONDS), "MERGE tasks timed out");
    return errors;
  }

  private void registerSource(String view, List<Row> rows) {
    StructType schema =
        new StructType()
            .add("id1", DataTypes.StringType, false)
            .add("id2", DataTypes.StringType, false)
            .add("payload", DataTypes.StringType, true);
    spark.createDataFrame(rows, schema).createOrReplaceTempView(view);
  }

  @Test
  public void concurrentMergesOfSameNewKeysDoNotDuplicate() throws Exception {
    registerSource(
        "src",
        Arrays.asList(
            RowFactory.create("c1", "t1", "a"),
            RowFactory.create("c1", "t2", "b"),
            RowFactory.create("c2", "t1", "c")));

    List<Throwable> errors = race(mergeSql("src"), mergeSql("src"));

    // A commit-conflict on the losing writer is acceptable (Lance serialised the writes); the
    // bug is a silent double-insert. Either way, the table must not contain duplicates.
    long count = spark.table(table).count();
    Assertions.assertEquals(
        3L, count, "concurrent MERGEs duplicated rows; thread errors=" + errors);
  }

  @Test
  public void concurrentMergesOfDisjointKeysBothSucceed() throws Exception {
    registerSource(
        "src_a",
        Arrays.asList(RowFactory.create("a1", "t1", "a"), RowFactory.create("a2", "t1", "b")));
    registerSource(
        "src_b",
        Arrays.asList(RowFactory.create("b1", "t1", "c"), RowFactory.create("b2", "t1", "d")));

    List<Throwable> errors = race(mergeSql("src_a"), mergeSql("src_b"));

    // Exact filters over disjoint keys must not intersect, so neither writer may be rejected.
    Assertions.assertTrue(errors.isEmpty(), "disjoint MERGEs conflicted: " + errors);
    Assertions.assertEquals(4L, spark.table(table).count());
  }

  @Test
  public void concurrentLargeMergesOfSameNewKeysDoNotDuplicate() throws Exception {
    // Enough rows to spill the exact set into the bloom filter on the driver.
    int rows = InsertedKeys.EXACT_LIMIT + 1000;
    spark
        .range(rows)
        .selectExpr("cast(id as string) as id1", "'k' as id2", "cast(id as string) as payload")
        .createOrReplaceTempView("src_large");

    List<Throwable> errors = race(mergeSql("src_large"), mergeSql("src_large"));

    long count = spark.table(table).count();
    Assertions.assertEquals(
        (long) rows, count, "concurrent large MERGEs duplicated rows; thread errors=" + errors);
  }
}
