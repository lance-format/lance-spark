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

import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.execution.ColumnarToRowExec;
import org.apache.spark.sql.execution.FilterExec;
import org.apache.spark.sql.execution.InputAdapter;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.adaptive.QueryStageExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests runtime filtering ({@link LanceScan#filter}): dynamic partition pruning over a broadcast
 * join prunes the fragments whose zonemap zones cannot match the join keys.
 */
public abstract class BaseRuntimeFilteringTest {
  private static final String CATALOG = "lance_rf";
  private static final int FRAGMENTS = 10;
  private static final int ROWS_PER_FRAGMENT = 100;

  private SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    Path root = tempDir.resolve(UUID.randomUUID().toString());
    root.toFile().mkdirs();
    spark =
        SparkSession.builder()
            .appName("lance-runtime-filtering-test")
            .master("local[4]")
            .config("spark.sql.catalog." + CATALOG, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG + ".root", root.toString())
            .config("spark.sql.catalog." + CATALOG + ".single_level_ns", "true")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  private static String uniqueName(String prefix) {
    return CATALOG + ".default." + prefix + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  /**
   * Creates a fact table of {@link #FRAGMENTS} fragments of {@link #ROWS_PER_FRAGMENT} rows each.
   * {@code k} runs over {@code [f * 100, f * 100 + 99]} in fragment {@code f} if {@code clustered},
   * else every fragment spans all keys. {@code u} has the same values but no zonemap index.
   */
  private String createFact(boolean clustered) {
    String fact = uniqueName("fact");
    spark.sql("CREATE TABLE " + fact + " (k INT, u INT, v STRING) USING lance");
    for (int f = 0; f < FRAGMENTS; f++) {
      String key =
          clustered
              ? String.format("CAST(id + %d AS INT)", f * ROWS_PER_FRAGMENT)
              : String.format("CAST(id * %d + %d AS INT)", FRAGMENTS, f);
      spark.sql(
          String.format(
              "INSERT INTO %s SELECT /*+ COALESCE(1) */ %s AS k, %s AS u, CONCAT('v', id) AS v"
                  + " FROM range(%d)",
              fact, key, key, ROWS_PER_FRAGMENT));
    }
    spark.sql("ALTER TABLE " + fact + " CREATE INDEX k_zm USING zonemap (k)");
    return fact;
  }

  /**
   * A small dimension view, which is not a Lance table, so Spark keeps its filter for dynamic
   * partition pruning to find; {@code attr = 'x'} selects keys 310, 320 and 330.
   */
  private String createDim() {
    String dim = "dim_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW "
            + dim
            + " AS SELECT CAST(id * 10 AS INT) AS k, CAST(id AS INT) AS w,"
            + " CASE WHEN id IN (31, 32, 33) THEN 'x' ELSE 'y' END AS attr FROM range(100)");
    return dim;
  }

  /** The BatchScanExec nodes of {@code plan}, through adaptive query stages. */
  private static List<BatchScanExec> scans(SparkPlan plan) {
    List<BatchScanExec> found = new ArrayList<>();
    collect(plan, found);
    return found;
  }

  private static void collect(SparkPlan plan, List<BatchScanExec> found) {
    if (plan instanceof AdaptiveSparkPlanExec) {
      collect(((AdaptiveSparkPlanExec) plan).executedPlan(), found);
      return;
    }
    if (plan instanceof QueryStageExec) {
      collect(((QueryStageExec) plan).plan(), found);
      return;
    }
    if (plan instanceof BatchScanExec) {
      found.add((BatchScanExec) plan);
    }
    for (int i = 0; i < plan.children().size(); i++) {
      collect((SparkPlan) plan.children().apply(i), found);
    }
  }

  /**
   * The number of input partitions {@code scan} reads: Spark applies the runtime filters to the
   * executed scan's {@link LanceScan}, which then plans only the partitions left.
   */
  private static int partitionsRead(BatchScanExec scan) {
    return ((LanceScan) scan.scan()).planInputPartitions().length;
  }

  /** The scan of {@code table} in the executed plan of {@code df}, which must have been run. */
  private static BatchScanExec scanOf(Dataset<Row> df, String table) {
    String name = table.substring(table.lastIndexOf('.') + 1);
    List<BatchScanExec> matching =
        scans(df.queryExecution().executedPlan()).stream()
            .filter(s -> s.scan() instanceof LanceScan)
            .filter(
                s -> ((LanceScan) s.scan()).readOptions().getDatasetUri().contains(name + ".lance"))
            .collect(Collectors.toList());
    assertEquals(1, matching.size(), "scans of " + table + ": " + matching);
    return matching.get(0);
  }

  /** Whether the executed plan of {@code df} has a Filter right above the scan of {@code table}. */
  private static boolean hasFilterAbove(Dataset<Row> df, String table) {
    BatchScanExec scan = scanOf(df, table);
    List<SparkPlan> nodes = new ArrayList<>();
    collectAll(df.queryExecution().executedPlan(), nodes);
    for (SparkPlan node : nodes) {
      if (node instanceof FilterExec && scanBelow(((FilterExec) node).child()) == scan) {
        return true;
      }
    }
    return false;
  }

  /** The scan under {@code plan} through row conversions and code generation, or null. */
  private static SparkPlan scanBelow(SparkPlan plan) {
    if (plan instanceof BatchScanExec) {
      return plan;
    }
    if (plan instanceof ColumnarToRowExec
        || plan instanceof InputAdapter
        || plan instanceof WholeStageCodegenExec) {
      return scanBelow((SparkPlan) plan.children().apply(0));
    }
    return null;
  }

  private static void collectAll(SparkPlan plan, List<SparkPlan> found) {
    if (plan instanceof AdaptiveSparkPlanExec) {
      collectAll(((AdaptiveSparkPlanExec) plan).executedPlan(), found);
      return;
    }
    if (plan instanceof QueryStageExec) {
      collectAll(((QueryStageExec) plan).plan(), found);
      return;
    }
    found.add(plan);
    for (int i = 0; i < plan.children().size(); i++) {
      collectAll((SparkPlan) plan.children().apply(i), found);
    }
  }

  private List<Row> sorted(Dataset<Row> df) {
    List<Row> rows = new ArrayList<>(df.collectAsList());
    rows.sort((a, b) -> a.toString().compareTo(b.toString()));
    return rows;
  }

  /** Runs {@code sql} with dynamic partition pruning on, checking its rows against it off. */
  private Dataset<Row> runWithAndWithoutPruning(String sql) {
    spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false");
    List<Row> expected = sorted(spark.sql(sql));
    spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true");
    Dataset<Row> df = spark.sql(sql);
    assertEquals(expected, sorted(df), sql);
    return df;
  }

  @Test
  public void reportsZonemapColumnsAsFilterAttributes() {
    String fact = createFact(true);
    LanceScan scan =
        (LanceScan) scanOf(runWithAndWithoutPruning("SELECT * FROM " + fact), fact).scan();
    assertEquals(
        Arrays.asList("k"),
        Arrays.stream(scan.filterAttributes())
            .map(NamedReference::describe)
            .collect(Collectors.toList()));
  }

  @Test
  public void prunesFragmentsByBroadcastJoinKeys() {
    for (String adaptive : new String[] {"true", "false"}) {
      spark.conf().set("spark.sql.adaptive.enabled", adaptive);
      String fact = createFact(true);
      String dim = createDim();
      Dataset<Row> df =
          runWithAndWithoutPruning(
              String.format(
                  "SELECT f.k, f.v FROM %s f JOIN %s d ON f.k = d.k WHERE d.attr = 'x'",
                  fact, dim));
      assertEquals(3, df.count());
      BatchScanExec factScan = scanOf(df, fact);
      assertFalse(factScan.runtimeFilters().isEmpty(), "no runtime filter on " + factScan);
      // Keys 310-330 all lie in fragment 3.
      assertEquals(1, partitionsRead(factScan), "adaptive=" + adaptive);
    }
  }

  @Test
  public void keepsAllFragmentsWhenZonesCannotPrune() {
    String dim = createDim();
    // Every fragment spans all keys, so its zones match any key.
    String unclustered = createFact(false);
    Dataset<Row> df =
        runWithAndWithoutPruning(
            String.format(
                "SELECT f.k FROM %s f JOIN %s d ON f.k = d.k WHERE d.attr = 'x'",
                unclustered, dim));
    assertEquals(3, df.count());
    assertEquals(FRAGMENTS, partitionsRead(scanOf(df, unclustered)));

    // u has no zonemap index, so it is not a filter attribute and the scan reads every fragment.
    String clustered = createFact(true);
    df =
        runWithAndWithoutPruning(
            String.format(
                "SELECT f.k FROM %s f JOIN %s d ON f.u = d.k WHERE d.attr = 'x'", clustered, dim));
    assertEquals(3, df.count());
    BatchScanExec scan = scanOf(df, clustered);
    assertTrue(scan.runtimeFilters().isEmpty(), "runtime filter on " + scan);
    assertEquals(FRAGMENTS, partitionsRead(scan));
  }

  @Test
  public void prunesEveryFragmentWhenNoKeyMatches() {
    // With adaptive execution Spark drops the join, and the scan, over an empty broadcast. The
    // filter is on a column Spark cannot fold, so the dimension is empty only at runtime.
    spark.conf().set("spark.sql.adaptive.enabled", "false");
    String fact = createFact(true);
    String dim = createDim();
    Dataset<Row> df =
        runWithAndWithoutPruning(
            String.format("SELECT f.k FROM %s f JOIN %s d ON f.k = d.k WHERE d.w < 0", fact, dim));
    assertEquals(0, df.count());
    assertEquals(0, partitionsRead(scanOf(df, fact)));
  }

  /** A sharded table keeps its key-grouped partitioning: runtime filtering only drops keys. */
  @Test
  public void prunesShardedTablesWithoutNewPartitionKeys() {
    String fact = uniqueName("sharded");
    spark.sql(
        "CREATE TABLE " + fact + " (region STRING, v INT) USING lance PARTITIONED BY (region)");
    String[] regions = {"east", "west", "north", "south"};
    for (String region : regions) {
      spark.sql(
          String.format(
              "INSERT INTO %s SELECT /*+ COALESCE(1) */ '%s', CAST(id AS INT) FROM range(10)",
              fact, region));
    }
    spark.sql("ALTER TABLE " + fact + " CREATE INDEX region_zm USING zonemap (region)");
    String dim = "regions_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW "
            + dim
            + " AS SELECT element_at(array('east', 'west', 'north', 'south'), CAST(id AS INT) + 1)"
            + " AS region, CASE WHEN id IN (0, 3) THEN 'x' ELSE 'y' END AS attr FROM range(4)");
    Dataset<Row> df =
        runWithAndWithoutPruning(
            String.format(
                "SELECT f.region, f.v FROM %s f JOIN %s d ON f.region = d.region"
                    + " WHERE d.attr = 'x'",
                fact, dim));
    assertEquals(20, df.count());
    assertEquals(2, partitionsRead(scanOf(df, fact)));
  }

  private String createLanceDim() {
    String dim = uniqueName("lancedim");
    spark.sql("CREATE TABLE " + dim + " (k INT, attr STRING) USING lance");
    spark.sql(
        "INSERT INTO "
            + dim
            + " SELECT CAST(id * 10 AS INT) AS k,"
            + " CASE WHEN id IN (31, 32, 33) THEN 'x' ELSE 'y' END AS attr FROM range(100)");
    return dim;
  }

  /**
   * A filter on a Lance dimension table is pushed into its scan, which leaves Spark's own
   * PartitionPruning no filter to find; LanceDynamicPruningRule adds the runtime filter instead.
   */
  @Test
  public void prunesFragmentsByAFilteredLanceDimensionTable() {
    String fact = createFact(true);
    String dim = createLanceDim();
    Dataset<Row> df =
        runWithAndWithoutPruning(
            String.format(
                "SELECT f.k, f.v FROM %s f JOIN %s d ON f.k = d.k WHERE d.attr = 'x'", fact, dim));
    assertEquals(3, df.count());
    BatchScanExec factScan = scanOf(df, fact);
    assertFalse(factScan.runtimeFilters().isEmpty(), "no runtime filter on " + factScan);
    assertEquals(1, partitionsRead(factScan));
    // The dimension's filter stays pushed into its scan, and Spark evaluates it again above.
    LanceScan dimScan = (LanceScan) scanOf(df, dim).scan();
    assertTrue(dimScan.getMetaData().get("whereConditions").get().contains("attr"));
    assertTrue(hasFilterAbove(df, dim));
    assertFalse(hasFilterAbove(df, fact));

    // A semi join prunes too. An IN subquery does not: Spark makes it a join only after the
    // dimension's filter was pushed into its scan.
    df =
        runWithAndWithoutPruning(
            String.format(
                "SELECT f.k, f.v FROM %s f LEFT SEMI JOIN (SELECT k FROM %s WHERE attr = 'x') d"
                    + " ON f.k = d.k",
                fact, dim));
    assertEquals(3, df.count());
    assertEquals(1, partitionsRead(scanOf(df, fact)));
  }

  /** Without a selective filter on the dimension there is nothing to prune by. */
  @Test
  public void addsNoRuntimeFilterForAnUnfilteredLanceDimensionTable() {
    String fact = createFact(true);
    String dim = createLanceDim();
    Dataset<Row> df =
        runWithAndWithoutPruning(
            String.format("SELECT f.k, f.v FROM %s f JOIN %s d ON f.k = d.k", fact, dim));
    assertEquals(100, df.count());
    BatchScanExec factScan = scanOf(df, fact);
    assertTrue(factScan.runtimeFilters().isEmpty(), "runtime filter on " + factScan);
    assertEquals(FRAGMENTS, partitionsRead(factScan));
    // Its inferred k IS NOT NULL is not selective, so it stays pushed down only.
    assertFalse(hasFilterAbove(df, dim));
  }

  /** Fragments written after the zonemap index was built are not in its stats, so they are kept. */
  @Test
  public void keepsFragmentsTheZonemapDoesNotDescribe() {
    String fact = createFact(true);
    // A fragment with key 320 that the index does not describe.
    spark.sql(
        "INSERT INTO "
            + fact
            + " SELECT /*+ COALESCE(1) */ CAST(320 AS INT) AS k, 0 AS u, 'late' AS v FROM range(1)");
    String dim = createDim();
    Dataset<Row> df =
        runWithAndWithoutPruning(
            String.format(
                "SELECT f.k, f.v FROM %s f JOIN %s d ON f.k = d.k WHERE d.attr = 'x'", fact, dim));
    assertEquals(4, df.count());
    assertEquals(2, partitionsRead(scanOf(df, fact)));
  }

  /**
   * Only a filtered join side keeps its pushed filters, so scans that are not joined keep pushing
   * their limit and aggregate down.
   */
  @Test
  public void keepsPushDownsOfScansThatAreNotJoined() {
    String fact = createFact(true);
    Dataset<Row> limited = spark.sql("SELECT * FROM " + fact + " WHERE k > 5 LIMIT 3");
    assertEquals(3, limited.count());
    LanceScan limitScan = (LanceScan) scanOf(limited, fact).scan();
    assertEquals("Optional[3]", limitScan.getMetaData().get("limit").get());

    Dataset<Row> counted = spark.sql("SELECT count(*) FROM " + fact + " WHERE k = 5");
    assertEquals(1L, counted.collectAsList().get(0).getLong(0));
    LanceScan countScan = (LanceScan) scanOf(counted, fact).scan();
    assertFalse(countScan.getMetaData().get("pushedAggregation").get().contains("empty"));

    // The fact side of a join with a filtered Lance dimension keeps its own filter pushed.
    String dim = createLanceDim();
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.k FROM %s f JOIN %s d ON f.k = d.k WHERE d.attr = 'x' AND f.u >= 0",
                fact, dim));
    assertEquals(3, joined.count());
    assertTrue(
        ((LanceScan) scanOf(joined, fact).scan())
            .getMetaData()
            .get("whereConditions")
            .get()
            .contains("u"));
    assertFalse(hasFilterAbove(joined, fact));
  }

  /** With runtime_filtering off, the scan takes no runtime filter, nor keeps a Lance join's. */
  @Test
  public void runtimeFilteringOptionTurnsPruningOff() {
    String fact = createFact(true);
    String dim = createDim();
    String lanceDim = createLanceDim();
    for (String dimension : new String[] {dim, lanceDim}) {
      String query =
          String.format(
              "SELECT f.k, f.v FROM %s f JOIN %s d ON f.k = d.k WHERE d.attr = 'x'",
              "fact_off", dimension);
      spark
          .read()
          .option(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING, "false")
          .table(fact)
          .createOrReplaceTempView("fact_off");
      Dataset<Row> df = runWithAndWithoutPruning(query);
      assertEquals(3, df.count());
      BatchScanExec factScan = scanOf(df, fact);
      assertTrue(factScan.runtimeFilters().isEmpty(), "runtime filter on " + factScan);
      assertEquals(FRAGMENTS, partitionsRead(factScan));
      if (dimension.equals(lanceDim)) {
        assertFalse(hasFilterAbove(df, lanceDim));
      }
    }
  }
}
