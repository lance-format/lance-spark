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
package org.lance.spark.update;

import org.lance.Dataset;
import org.lance.index.Index;
import org.lance.index.IndexType;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Base integration tests for incremental vector index build: {@code ALTER TABLE ... CREATE INDEX
 * ... USING IVF_* (col) WITH (mode='incremental')}.
 *
 * <p>R1 semantics: incremental delegates to lance-core's {@code optimizeIndices(retrain=false)},
 * which extends the existing index onto unindexed fragments without retraining centroids /
 * codebook. Tests run against a local SparkSession with the dir namespace catalog.
 *
 * <p>Concrete subclasses are empty shells, one per Spark/Scala matrix module, so the same suite
 * runs everywhere — same pattern as {@link BaseAddVectorIndexTest}.
 */
public abstract class BaseIncrementalAddVectorIndexTest {

  protected static final int VEC_DIM = 32;
  protected static final int ROWS_PER_INSERT = 80;
  protected static final int INITIAL_INSERTS = 4; // ≥ 4 fragments at initial build
  protected static final int APPEND_INSERTS = 2; // ≥ 2 fragments appended after build

  protected SparkSession spark;
  protected String catalogName = "lance_test";
  protected String tableName;
  protected String fullTable;
  protected String tableDir;
  private final Random random = new Random(42);

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-incremental-vector-index-test")
            .master("local[10]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "incr_vec_idx_test_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
    this.tableDir =
        FileSystems.getDefault().getPath(testRoot, this.tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  // ---------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------

  private void createEmptyTable() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, vec ARRAY<FLOAT> NOT NULL) USING lance "
                + "TBLPROPERTIES ('vec.arrow.fixed-size-list.size' = '%d')",
            fullTable, VEC_DIM));
  }

  /** Insert {@code batches} batches of {@link #ROWS_PER_INSERT} rows each. */
  private int insertRows(int batches, int startId) {
    int rowId = startId;
    for (int b = 0; b < batches; b++) {
      List<String> values = new ArrayList<>();
      for (int r = 0; r < ROWS_PER_INSERT; r++) {
        StringBuilder arr = new StringBuilder("ARRAY(");
        for (int i = 0; i < VEC_DIM; i++) {
          if (i > 0) arr.append(", ");
          arr.append(String.format(Locale.ROOT, "CAST(%f AS FLOAT)", random.nextFloat()));
        }
        arr.append(")");
        values.add(String.format("(%d, %s)", rowId++, arr));
      }
      spark.sql(
          String.format("INSERT INTO %s (id, vec) VALUES %s", fullTable, String.join(",", values)));
    }
    return rowId;
  }

  /** Walk an exception chain to the root cause and return its message (or empty). */
  protected static String rootCauseMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    String msg = cur.getMessage();
    return msg == null ? "" : msg;
  }

  /** Number of segments under the given index name. */
  private long indexSegmentCount(String name) {
    Dataset ds = Dataset.open().uri(tableDir).build();
    try {
      return ds.getIndexes().stream().filter(i -> name.equals(i.name())).count();
    } finally {
      ds.close();
    }
  }

  /** UUIDs of all segments under the given index name. */
  private java.util.Set<UUID> indexSegmentUuids(String name) {
    Dataset ds = Dataset.open().uri(tableDir).build();
    try {
      return ds.getIndexes().stream()
          .filter(i -> name.equals(i.name()))
          .map(Index::uuid)
          .collect(Collectors.toSet());
    } finally {
      ds.close();
    }
  }

  /** Total fragments covered by all segments of the given index. */
  private int coveredFragmentCount(String name) {
    Dataset ds = Dataset.open().uri(tableDir).build();
    try {
      return ds.getIndexes().stream()
          .filter(i -> name.equals(i.name()))
          .mapToInt(i -> i.fragments().orElse(java.util.Collections.emptyList()).size())
          .sum();
    } finally {
      ds.close();
    }
  }

  private int datasetFragmentCount() {
    Dataset ds = Dataset.open().uri(tableDir).build();
    try {
      return ds.getFragments().size();
    } finally {
      ds.close();
    }
  }

  private long unindexedFragments(String name) {
    Dataset ds = Dataset.open().uri(tableDir).build();
    try {
      Object v = ds.getIndexStatistics(name).get("num_unindexed_fragments");
      return v instanceof Number ? ((Number) v).longValue() : 0L;
    } finally {
      ds.close();
    }
  }

  // ---------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------

  /**
   * Sanity test: incremental mode dispatches and finishes for IVF_FLAT after a full build, when new
   * fragments have been appended.
   *
   * <p>Steps: full build covering all initial fragments → append more rows (creates new fragments)
   * → run incremental → assert the new fragments are now covered by the index.
   */
  @Test
  public void testIncrementalCoversNewFragmentsAfterAppend() {
    createEmptyTable();
    int nextId = insertRows(INITIAL_INSERTS, 0);

    // Initial full build (mode='replace' default).
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) " + "WITH (num_partitions=4)",
            fullTable));

    int coveredBefore = coveredFragmentCount("vec_idx");
    int fragsBefore = datasetFragmentCount();
    Assertions.assertEquals(
        fragsBefore, coveredBefore, "Initial build should cover every fragment");

    // Append more data → introduces uncovered fragments.
    insertRows(APPEND_INSERTS, nextId);
    long uncoveredBefore = unindexedFragments("vec_idx");
    Assertions.assertTrue(
        uncoveredBefore > 0, "Append should produce unindexed fragments; got " + uncoveredBefore);

    // Incremental run.
    Row row =
        spark
            .sql(
                String.format(
                    "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                        + "WITH (mode='incremental')",
                    fullTable))
            .collectAsList()
            .get(0);
    long fragmentsIndexed = row.getLong(0);
    Assertions.assertEquals("vec_idx", row.getString(1));
    Assertions.assertEquals(
        uncoveredBefore,
        fragmentsIndexed,
        "Incremental should report the previously-uncovered fragment count");

    // Post-condition: every fragment of the dataset is now covered.
    Assertions.assertEquals(
        0L,
        unindexedFragments("vec_idx"),
        "After incremental, num_unindexed_fragments should be 0");
  }

  /**
   * No-op when every fragment is already covered. Returns (0, name); does not change segment UUIDs
   * (in particular: does not retrain).
   */
  @Test
  public void testIncrementalNoOpWhenAllCovered() {
    createEmptyTable();
    insertRows(INITIAL_INSERTS, 0);
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) " + "WITH (num_partitions=4)",
            fullTable));

    java.util.Set<UUID> uuidsBefore = indexSegmentUuids("vec_idx");
    Assertions.assertFalse(uuidsBefore.isEmpty(), "Initial build should produce segments");

    Row row =
        spark
            .sql(
                String.format(
                    "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                        + "WITH (mode='incremental')",
                    fullTable))
            .collectAsList()
            .get(0);
    Assertions.assertEquals(0L, row.getLong(0));
    Assertions.assertEquals("vec_idx", row.getString(1));

    // No segment UUID rotation: no retraining occurred.
    Assertions.assertEquals(
        uuidsBefore,
        indexSegmentUuids("vec_idx"),
        "No-op incremental must not rotate segment UUIDs");
  }

  /**
   * Incremental mode requires the named index to already exist. Calling it on a non-existent index
   * name must throw, never silently fall back to full build.
   */
  @Test
  public void testIncrementalRejectsWhenIndexAbsent() {
    createEmptyTable();
    insertRows(INITIAL_INSERTS, 0);

    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                                + "WITH (mode='incremental')",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("does not exist"),
        "Expected 'does not exist' message, got: " + rootCauseMessage(ex));
    Assertions.assertTrue(
        rootCauseMessage(ex).contains("incremental"),
        "Expected 'incremental' in error, got: " + rootCauseMessage(ex));
  }

  /**
   * Incremental on an empty table: same shape as full build — return (0, name) without erroring.
   * Empty table is a no-op regardless of mode.
   */
  @Test
  public void testIncrementalEmptyTable() {
    createEmptyTable();

    Row row =
        spark
            .sql(
                String.format(
                    "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                        + "WITH (mode='incremental')",
                    fullTable))
            .collectAsList()
            .get(0);
    Assertions.assertEquals(0L, row.getLong(0));
    Assertions.assertEquals("vec_idx", row.getString(1));
  }

  /**
   * Incremental works for IVF_PQ as well. Same flow as the IVF_FLAT happy path, but exercises the
   * PQ codebook reuse contract (lance-core's optimizeIndices with retrain=false must not retrain
   * the codebook).
   */
  @Test
  public void testIncrementalIvfPqCoversNewFragments() {
    createEmptyTable();
    int nextId = insertRows(INITIAL_INSERTS, 0);

    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                + "WITH (num_partitions=4, num_sub_vectors=4)",
            fullTable));

    insertRows(APPEND_INSERTS, nextId);
    long uncoveredBefore = unindexedFragments("vec_idx");
    Assertions.assertTrue(uncoveredBefore > 0);

    spark
        .sql(
            String.format(
                "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_PQ (vec) "
                    + "WITH (mode='incremental')",
                fullTable))
        .collect();
    Assertions.assertEquals(
        0L, unindexedFragments("vec_idx"), "After incremental, no fragments should be unindexed");

    // Index type unchanged (IVF_PQ or its umbrella VECTOR).
    Dataset ds = Dataset.open().uri(tableDir).build();
    try {
      List<Index> matches =
          ds.getIndexes().stream()
              .filter(i -> "vec_idx".equals(i.name()))
              .collect(Collectors.toList());
      Assertions.assertFalse(matches.isEmpty());
      for (Index idx : matches) {
        Assertions.assertTrue(
            idx.indexType() == IndexType.IVF_PQ || idx.indexType() == IndexType.VECTOR,
            "Unexpected indexType after incremental: " + idx.indexType());
      }
    } finally {
      ds.close();
    }
  }

  /**
   * Explicit mode='replace' is accepted and behaves identically to omitting mode (full rebuild +
   * atomic replace). Validates the mode whitelist accepts both values.
   */
  @Test
  public void testExplicitReplaceModeBehavesAsDefault() {
    createEmptyTable();
    insertRows(INITIAL_INSERTS, 0);

    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                + "WITH (num_partitions=4, mode='replace')",
            fullTable));

    Assertions.assertEquals(
        0L,
        unindexedFragments("vec_idx"),
        "Explicit mode=replace should produce a fully-covered index");
  }

  /** Unrecognized mode values must be rejected up front, before any work is done. */
  @Test
  public void testUnrecognizedModeIsRejected() {
    createEmptyTable();
    insertRows(INITIAL_INSERTS, 0);

    RuntimeException ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX vec_idx USING IVF_FLAT (vec) "
                                + "WITH (num_partitions=4, mode='extend')",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        rootCauseMessage(ex).toLowerCase(Locale.ROOT).contains("mode")
            || rootCauseMessage(ex).contains("'extend'"),
        "Expected mode-validation error, got: " + rootCauseMessage(ex));
  }
}
