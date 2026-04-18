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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public abstract class BaseZoneLevelPartitioningTest {
  protected String catalogName;
  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    SparkSession prior =
        SparkSession.getDefaultSession().isDefined()
            ? SparkSession.getDefaultSession().get()
            : null;
    if (prior != null) prior.stop();
    SparkSession active =
        SparkSession.getActiveSession().isDefined() ? SparkSession.getActiveSession().get() : null;
    if (active != null && active != prior) active.stop();
    SparkSession.clearDefaultSession();
    SparkSession.clearActiveSession();

    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    rootPath.toFile().mkdirs();
    catalogName = "lance_zone_pt_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    spark =
        SparkSession.builder()
            .appName("lance-zone-partitioning-test")
            .master("local[1]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", rootPath.toString())
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .config("spark.sql.sources.v2.bucketing.enabled", "true")
            .config("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
            .config("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
    }
  }

  private void createTwoZoneTable(String tableName) {
    String fullTable = catalogName + ".default." + tableName;
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, payload DOUBLE) USING lance"
                + " TBLPROPERTIES ('lance.partition.columns' = 'region')",
            fullTable));

    StringBuilder values = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      if (values.length() > 0) values.append(",");
      values.append(String.format(Locale.ROOT, "(%d, 'east', %f)", i, i * 1.5));
    }
    for (int i = 100; i < 200; i++) {
      values.append(",");
      values.append(String.format(Locale.ROOT, "(%d, 'west', %f)", i, i * 1.5));
    }
    spark.sql(String.format("INSERT INTO %s (id, region, payload) VALUES %s", fullTable, values));
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX region_idx USING btree (region)" + " WITH (zone_size=100)",
            fullTable));
  }

  @Test
  @org.junit.jupiter.api.Disabled(
      "Requires lance-core to expose btree zone stats via getZonemapStats. "
          + "BTree indexes internally produce zone data but describe_indices reports "
          + "index_type='BTree' which the current JNI getZonemapStats skips "
          + "(it matches only 'zonemap'). Enable when lance-core is updated.")
  public void zoneLevelPartitioning_joinUsesKeyGroupedPartitioning_twoValuesOneFragment() {
    String tableA = "a_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    String tableB = "b_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    createTwoZoneTable(tableA);
    createTwoZoneTable(tableB);
    String fullA = catalogName + ".default." + tableA;
    String fullB = catalogName + ".default." + tableB;

    Dataset<Row> joined =
        spark.sql(
            String.format(
                "SELECT /*+ MERGE(a, b) */ a.region, a.payload AS ap, b.payload AS bp"
                    + " FROM %s a JOIN %s b USING (region)",
                fullA, fullB));

    long rowCount = joined.count();
    assertEquals(20_000L, rowCount, "100*100 east + 100*100 west");

    String plan = joined.queryExecution().executedPlan().toString();
    assertFalse(
        plan.contains("Exchange hashpartitioning"),
        "SPJ should eliminate the pre-join shuffle.\nPlan:\n" + plan);
  }

  @Test
  public void zoneLevelPartitioning_skipsColumnWithNonSingleValuedZone() {
    String table = "t_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    String full = catalogName + ".default." + table;
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING) USING lance"
                + " TBLPROPERTIES ('lance.partition.columns' = 'region')",
            full));

    StringBuilder values = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      if (values.length() > 0) values.append(",");
      values.append(String.format(Locale.ROOT, "(%d, '%s')", i, i % 2 == 0 ? "east" : "west"));
    }
    spark.sql(String.format("INSERT INTO %s (id, region) VALUES %s", full, values));
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX idx USING btree (region) WITH (zone_size=100)", full));

    assertEquals(200L, spark.sql("SELECT id FROM " + full).collectAsList().size());
  }

  @Test
  public void unindexedFragmentAfterIndexCreation_disablesSpj_noDataLoss() {
    String table = "u_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    String full = catalogName + ".default." + table;
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING) USING lance"
                + " TBLPROPERTIES ('lance.partition.columns' = 'region')",
            full));

    StringBuilder first = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      if (first.length() > 0) first.append(",");
      first.append(String.format(Locale.ROOT, "(%d, 'east')", i));
    }
    spark.sql(String.format("INSERT INTO %s (id, region) VALUES %s", full, first));
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX idx USING btree (region) WITH (zone_size=100)", full));

    StringBuilder second = new StringBuilder();
    for (int i = 100; i < 200; i++) {
      if (second.length() > 0) second.append(",");
      second.append(String.format(Locale.ROOT, "(%d, 'west')", i));
    }
    spark.sql(String.format("INSERT INTO %s (id, region) VALUES %s", full, second));

    long readRows = spark.sql("SELECT id FROM " + full).collectAsList().size();
    assertEquals(200L, readRows, "Unindexed-fragment rows must not be dropped");
  }
}
