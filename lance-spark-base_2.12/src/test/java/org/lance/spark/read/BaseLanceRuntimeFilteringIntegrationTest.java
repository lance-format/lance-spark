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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** End-to-end Spark 4.1+ join-generated dynamic partition pruning tests. */
public abstract class BaseLanceRuntimeFilteringIntegrationTest {
  private static final int ROWS_PER_KEY = 3;

  @TempDir Path tempDir;

  protected SparkSession spark;
  private String defaultCatalog;

  @BeforeEach
  public void setup() {
    defaultCatalog = "runtime_" + suffix();
    spark =
        SparkSession.builder()
            .appName("lance-runtime-filtering-test")
            .master("local[4]")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
            .config("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")
            .config("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "10")
            .config("spark.sql.sources.v2.bucketing.enabled", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .getOrCreate();
    configureCatalog(defaultCatalog);
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testInnerJoinRuntimeFilteringPrunesAndPreservesDuplicateMultiplicity() {
    String fact = createFact(defaultCatalog, true, false);
    String dim = createDimension(defaultCatalog, false);
    Dataset<Row> factScan = spark.sql("SELECT k FROM " + fact);
    factScan.collectAsList();
    LanceScan standaloneScan = LanceScanTestHelper.extractLanceScan(factScan);
    assertNotNull(standaloneScan);
    assertArrayEquals(
        new String[] {"k"},
        Arrays.stream(standaloneScan.filterAttributes())
            .map(reference -> String.join(".", reference.fieldNames()))
            .toArray(String[]::new));

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id, d.label FROM %s f JOIN %s d ON f.k = d.k "
                    + "WHERE length(d.label) = 5",
                fact, dim));

    assertEquals(9, joined.collectAsList().size());
    assertRuntimePruned(joined);
  }

  @Test
  public void testLeftSemiJoinRuntimeFiltering() {
    String fact = createFact(defaultCatalog, true, false);
    String dim = createDimension(defaultCatalog, false);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f LEFT SEMI JOIN "
                    + "(SELECT * FROM %s WHERE length(label) = 5) d ON f.k = d.k",
                fact, dim));

    assertEquals(2L * ROWS_PER_KEY, joined.collectAsList().size());
    assertRuntimePruned(joined);
  }

  @Test
  public void testEligibleRightOuterJoinRuntimeFiltering() {
    String fact = createFact(defaultCatalog, true, false);
    String dim = createDimension(defaultCatalog, false);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id, d.label FROM %s f RIGHT OUTER JOIN %s d "
                    + "ON f.k = d.k WHERE length(d.label) = 5",
                fact, dim));

    assertEquals(9, joined.collectAsList().size());
    assertRuntimePruned(joined);
  }

  @Test
  public void testEmptyBuildSideProducesNoLancePartitions() {
    String fact = createFact(defaultCatalog, true, false);
    String dim = createDimension(defaultCatalog, false);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k " + "WHERE length(d.label) = 7",
                fact, dim));

    assertTrue(joined.collectAsList().isEmpty());
    Map<String, String> metadata = runtimeMetadata(joined);
    assertEquals("0", metadata.get("survivingFragmentCount"));
  }

  @Test
  public void testStaticAndRuntimeFiltersCompose() {
    String fact = createFact(defaultCatalog, true, false);
    String dim = createDimension(defaultCatalog, false);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k "
                    + "WHERE length(d.label) = 5 AND f.id >= 30",
                fact, dim));

    assertEquals(ROWS_PER_KEY, joined.collectAsList().size());
    Map<String, String> metadata = runtimeMetadata(joined);
    assertNotEquals("Optional.empty", metadata.get("whereConditions"));
    assertFalse(metadata.get("runtimePredicates").equals("[]"));
  }

  @Test
  public void testUnindexedJoinColumnIsNotAdvertised() {
    String fact = createFact(defaultCatalog, false, false);
    String dim = createDimension(defaultCatalog, false);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k " + "WHERE length(d.label) = 5",
                fact, dim));

    assertEquals(9, joined.collectAsList().size());
    assertFalse(LanceScanTestHelper.hasLanceRuntimeFilters(joined));
  }

  @Test
  public void testPushDownFiltersDisablesRuntimeRouting() {
    String catalog = "runtime_no_push_" + suffix();
    configureCatalog(catalog, "pushDownFilters", "false");
    String fact = createFact(catalog, true, false);
    String dim = createDimension(catalog, false);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k " + "WHERE length(d.label) = 5",
                fact, dim));

    assertEquals(9, joined.collectAsList().size());
    assertFalse(LanceScanTestHelper.hasLanceRuntimeFilters(joined));
  }

  @Test
  public void testScalarIndexDisabledStillUsesZonemapRuntimePruning() {
    String catalog = "runtime_no_scalar_" + suffix();
    configureCatalog(catalog, "use_scalar_index", "false");
    String fact = createFact(catalog, true, false);
    String dim = createDimension(catalog, false);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k " + "WHERE length(d.label) = 5",
                fact, dim));

    assertEquals(9, joined.collectAsList().size());
    assertRuntimePruned(joined);
  }

  @Test
  public void testRuntimeGuardrailRejectsWithoutChangingJoinResults() {
    String catalog = "runtime_guard_" + suffix();
    configureCatalog(catalog, "runtime_filter_max_keys", "1");
    String fact = createFact(catalog, true, false);
    String dim = createDimension(catalog, false);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k " + "WHERE length(d.label) = 5",
                fact, dim));

    assertEquals(9, joined.collectAsList().size());
    Map<String, String> metadata = runtimeMetadata(joined);
    assertEquals("[]", metadata.get("runtimePredicates"));
    assertEquals("1", metadata.get("rejectedRuntimeFilterCount"));
    assertEquals(metadata.get("originalFragmentCount"), metadata.get("survivingFragmentCount"));
  }

  @Test
  public void testProjectedOutAndUnsupportedColumnsAreNotAdvertised() {
    String fact = createFact(defaultCatalog, true, false);
    Dataset<Row> projected = spark.sql("SELECT payload FROM " + fact);
    projected.collectAsList();
    LanceScan projectedScan = LanceScanTestHelper.extractLanceScan(projected);
    assertNotNull(projectedScan);
    assertEquals(0, projectedScan.filterAttributes().length);

    String complex = defaultCatalog + ".default.complex_" + suffix();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, values ARRAY<INT>, nested STRUCT<a: INT>) USING lance",
            complex));
    spark.sql(
        String.format("INSERT INTO %s VALUES (1, array(1, 2), named_struct('a', 1))", complex));
    Dataset<Row> complexRead = spark.sql("SELECT * FROM " + complex);
    complexRead.collectAsList();
    LanceScan complexScan = LanceScanTestHelper.extractLanceScan(complexRead);
    assertNotNull(complexScan);
    assertEquals(0, complexScan.filterAttributes().length);
  }

  @Test
  public void testRowAddressIsAdvertisedWithoutAnIndex() {
    String fact = createFact(defaultCatalog, false, false);
    Dataset<Row> rowAddresses = spark.sql("SELECT _rowaddr FROM " + fact);
    rowAddresses.collectAsList();
    LanceScan scan = LanceScanTestHelper.extractLanceScan(rowAddresses);
    assertNotNull(scan);
    assertArrayEquals(
        new String[] {"_rowaddr"},
        Arrays.stream(scan.filterAttributes())
            .map(reference -> String.join(".", reference.fieldNames()))
            .toArray(String[]::new));
  }

  @Test
  public void testBtreeScalarIndexAdvertisesProjectedScalarColumn() {
    String table = defaultCatalog + ".default.btree_" + suffix();
    spark.sql(String.format("CREATE TABLE %s (k INT, payload STRING) USING lance", table));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", table));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX k_idx USING btree (k)", table));

    Dataset<Row> read = spark.sql("SELECT k FROM " + table);
    read.collectAsList();
    LanceScan scan = LanceScanTestHelper.extractLanceScan(read);
    assertNotNull(scan);
    assertArrayEquals(
        new String[] {"k"},
        Arrays.stream(scan.filterAttributes())
            .map(reference -> String.join(".", reference.fieldNames()))
            .toArray(String[]::new));
  }

  @Test
  public void testQuotedTopLevelColumnNamesInOrdinaryAndRuntimeScans() {
    String table = defaultCatalog + ".default.quoted_" + suffix();
    String space = "a b";
    spark.sql(
        String.format(
            "CREATE TABLE %s (%s INT, payload STRING) USING lance", table, sqlIdentifier(space)));
    for (int key = 0; key < 5; key++) {
      spark.sql(String.format("INSERT INTO %s VALUES (%d, 'v_%d')", table, key, key));
    }
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX space_idx USING zonemap (%s)",
            table, sqlIdentifier(space)));
    Dataset<Row> ordinary =
        spark.sql(String.format("SELECT %s FROM %s", sqlIdentifier(space), table));
    assertEquals(5, ordinary.collectAsList().size());
    LanceScan ordinaryScan = LanceScanTestHelper.extractLanceScan(ordinary);
    assertNotNull(ordinaryScan);
    Set<String> attributes =
        Arrays.stream(ordinaryScan.filterAttributes())
            .map(reference -> reference.fieldNames()[0])
            .collect(Collectors.toSet());
    assertEquals(Set.of(space), attributes);

    String dim = createDimension(defaultCatalog, false);
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.payload FROM %s f JOIN %s d ON f.%s = d.k " + "WHERE length(d.label) = 5",
                table, dim, sqlIdentifier(space)));
    assertEquals(3, joined.collectAsList().size());
    assertTrue(
        LanceScanTestHelper.hasLanceRuntimeFilters(joined),
        joined.queryExecution().executedPlan().toString());
    assertTrue(runtimeMetadata(joined).get("runtimePredicates").contains(space));
  }

  @Test
  public void testIndexedFloatingPointColumnsAreNotRuntimeFilterEligible() {
    String fact = defaultCatalog + ".default.float_fact_" + suffix();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, f FLOAT, d DOUBLE, payload STRING) USING lance", fact));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(1, CAST(1.5 AS FLOAT), CAST(1.5 AS DOUBLE), 'one'),"
                + "(2, CAST(2.5 AS FLOAT), CAST(2.5 AS DOUBLE), 'two'),"
                + "(3, CAST(3.5 AS FLOAT), CAST(3.5 AS DOUBLE), 'three')",
            fact));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX f_idx USING zonemap (f)", fact));
    spark.sql(String.format("ALTER TABLE %s CREATE INDEX d_idx USING zonemap (d)", fact));

    Dataset<Row> ordinary = spark.sql("SELECT f, d FROM " + fact);
    ordinary.collectAsList();
    LanceScan scan = LanceScanTestHelper.extractLanceScan(ordinary);
    assertNotNull(scan);
    assertEquals(0, scan.filterAttributes().length);
    scan.filter(new Predicate[] {TestPredicates.in("d", 2.5d)});
    assertEquals(0, scan.pushedPredicates().length);

    Dataset<Row> staticFilter =
        spark.sql(
            String.format(
                "SELECT id FROM %s WHERE f = CAST(2.5 AS FLOAT) " + "AND d = CAST(2.5 AS DOUBLE)",
                fact));
    assertEquals(1, staticFilter.collectAsList().size());
    Map<String, String> staticMetadata = LanceScanTestHelper.extractLanceScanMetadata(staticFilter);
    assertNotNull(staticMetadata);
    assertNotEquals("Optional.empty", staticMetadata.get("whereConditions"));

    String dim = defaultCatalog + ".default.float_dim_" + suffix();
    spark.sql(String.format("CREATE TABLE %s (k DOUBLE, label STRING) USING lance", dim));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(CAST(2.5 AS DOUBLE), 'three'), (CAST(4.5 AS DOUBLE), 'four')",
            dim));
    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.d = d.k WHERE length(d.label) = 5",
                fact, dim));
    assertEquals(1, joined.collectAsList().size());
    assertFalse(LanceScanTestHelper.hasLanceRuntimeFilters(joined));
    LanceScan joinedScan = LanceScanTestHelper.extractLanceScan(joined);
    assertNotNull(joinedScan);
    assertEquals(0, joinedScan.pushedPredicates().length);
  }

  @Test
  public void testStoragePartitionedJoinCoexistsWithRuntimeFiltering() {
    String fact = createFact(defaultCatalog, true, true);
    String dim = createDimension(defaultCatalog, true);

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT f.id FROM %s f JOIN %s d ON f.k = d.k " + "WHERE length(d.label) = 5",
                fact, dim));

    assertEquals(9, joined.collectAsList().size());
    assertRuntimePruned(joined);
    LanceScan scan = LanceScanTestHelper.extractRuntimeFilteredLanceScan(joined);
    assertNotNull(scan);
    assertInstanceOf(KeyGroupedPartitioning.class, scan.outputPartitioning());
  }

  private String createFact(String catalog, boolean indexed, boolean partitioned) {
    String table = catalog + ".default.fact_" + suffix();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, k INT, payload STRING) USING lance%s",
            table, partitioned ? " PARTITIONED BY (k)" : ""));
    for (int key = 0; key < 5; key++) {
      int currentKey = key;
      String values =
          java.util.stream.IntStream.range(0, ROWS_PER_KEY)
              .mapToObj(
                  offset ->
                      String.format(
                          "(%d, %d, 'v_%d_%d')",
                          currentKey * 10 + offset, currentKey, currentKey, offset))
              .collect(Collectors.joining(","));
      spark.sql(String.format("INSERT INTO %s VALUES %s", table, values));
    }
    if (indexed) {
      spark.sql(String.format("ALTER TABLE %s CREATE INDEX k_idx USING zonemap (k)", table));
    }
    return table;
  }

  private String createDimension(String catalog, boolean partitioned) {
    String table = catalog + ".default.dim_" + suffix();
    spark.sql(
        String.format(
            "CREATE TABLE %s (k INT, keep BOOLEAN, label STRING) USING lance%s",
            table, partitioned ? " PARTITIONED BY (k)" : ""));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(1, true, 'one_a'),"
                + "(1, true, 'one_b'),"
                + "(3, true, 'three'),"
                + "(4, false, 'four'),"
                + "(NULL, true, 'null')",
            table));
    if (partitioned) {
      spark.sql(String.format("ALTER TABLE %s CREATE INDEX k_idx USING zonemap (k)", table));
    }
    return table;
  }

  private void assertRuntimePruned(Dataset<Row> dataset) {
    assertTrue(
        LanceScanTestHelper.hasLanceRuntimeFilters(dataset),
        dataset.queryExecution().executedPlan().toString());
    Map<String, String> metadata = runtimeMetadata(dataset);
    assertFalse(metadata.get("runtimePredicates").equals("[]"));
    assertTrue(
        Integer.parseInt(metadata.get("survivingFragmentCount"))
            < Integer.parseInt(metadata.get("originalFragmentCount")));
  }

  private Map<String, String> runtimeMetadata(Dataset<Row> dataset) {
    Map<String, String> metadata =
        LanceScanTestHelper.extractRuntimeFilteredLanceScanMetadata(dataset);
    assertNotNull(metadata, dataset.queryExecution().executedPlan().toString());
    return metadata;
  }

  private void configureCatalog(String catalog, String... option) {
    spark.conf().set("spark.sql.catalog." + catalog, "org.lance.spark.LanceNamespaceSparkCatalog");
    spark.conf().set("spark.sql.catalog." + catalog + ".impl", "dir");
    spark.conf().set("spark.sql.catalog." + catalog + ".root", tempDir.toString());
    spark.conf().set("spark.sql.catalog." + catalog + ".single_level_ns", "true");
    if (option.length == 2) {
      spark.conf().set("spark.sql.catalog." + catalog + "." + option[0], option[1]);
    }
  }

  private static String suffix() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  private static String sqlIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }
}
