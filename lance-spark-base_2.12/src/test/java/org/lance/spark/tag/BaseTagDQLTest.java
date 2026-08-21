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
package org.lance.spark.tag;

import org.lance.Ref;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRef;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Base tests for querying tagged table snapshots. */
public abstract class BaseTagDQLTest {
  private static final String CATALOG_NAME = "lance_test";

  private SparkSession spark;
  private String tableName;
  private String fullTable;
  private String tableDir;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-tag-dql-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + CATALOG_NAME, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME + ".impl", "dir")
            .config("spark.sql.catalog." + CATALOG_NAME + ".root", testRoot)
            .config("spark.sql.catalog." + CATALOG_NAME + ".single_level_ns", "true")
            .getOrCreate();
    tableName = "tag_dql_test_" + UUID.randomUUID().toString().replace("-", "");
    fullTable = CATALOG_NAME + ".default." + tableName;
    tableDir = FileSystems.getDefault().getPath(testRoot, tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testTagSnapshotDoesNotIncludeDataWrittenAfterTagCreation() {
    spark.sql(String.format("create table %s (id int, text string) using lance", fullTable));
    insertRange(0, 5);
    String tag = "snapshot_before_new_data";
    createTag(tag);

    insertRange(5, 10);

    String taggedTable = String.format("%s version as of '%s'", fullTable, tag);

    Assertions.assertEquals(5, spark.sql("select * from " + taggedTable).count());
    Assertions.assertEquals(
        0, spark.sql("select * from " + taggedTable + " where id >= 5").count());

    Assertions.assertEquals(10, spark.table(fullTable).count());
    Assertions.assertEquals(5, spark.sql("select * from " + fullTable + " where id >= 5").count());
  }

  @Test
  public void testQueryTagCreatedFromBranchAfterMainAdvances() {
    spark.sql(String.format("create table %s (id int, text string) using lance", fullTable));
    insertRange(0, 5);

    String branch = "source_branch";
    String tag = "branch_snapshot";
    spark.sql(String.format("alter table %s create branch %s", fullTable, branch));
    spark.sql(
        String.format("alter table %s create tag %s as of branch %s", fullTable, tag, branch));

    insertRange(5, 10);

    String taggedTable = String.format("%s version as of '%s'", fullTable, tag);
    Assertions.assertEquals(5, spark.sql("select * from " + taggedTable).count());
    Assertions.assertEquals(
        0, spark.sql("select * from " + taggedTable + " where id >= 5").count());
    Assertions.assertEquals(10, spark.table(fullTable).count());
    Assertions.assertEquals(5, spark.sql("select * from " + fullTable + " where id >= 5").count());
  }

  @Test
  public void testQueryNonexistentTagThrowsException() {
    spark.sql(String.format("create table %s (id int, text string) using lance", fullTable));
    insertRange(0, 5);

    String query = String.format("select * from %s version as of 'nonexistent_tag'", fullTable);

    Assertions.assertThrows(Exception.class, () -> spark.sql(query).collectAsList());
  }

  @Test
  public void testTagTableUsesTagReferenceAndIsReadOnly() throws Exception {
    DatasetVersions versions = prepareDatasetWithHistory();
    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(CATALOG_NAME);

    Table taggedTable =
        catalog.loadTable(
            Identifier.of(new String[] {"default"}, tableName), versions.firstInsertTag);
    LanceRef ref = ((LanceDataset) taggedTable).readOptions().getRef();

    Assertions.assertEquals(versions.firstInsertTag, ref.getTagName().get());
    Assertions.assertTrue(ref.getVersionNumber().isEmpty());
    Assertions.assertEquals(
        Collections.singleton(TableCapability.BATCH_READ), taggedTable.capabilities());
  }

  @Test
  public void testRejectWritesToTag() {
    DatasetVersions versions = prepareDatasetWithHistory();
    String taggedTable = String.format("%s version as of '%s'", fullTable, versions.firstInsertTag);
    spark.sql(
        String.format(
            "create temporary view tag_write_source as "
                + "select id, text, _rowaddr, _fragid from %s",
            taggedTable));

    String[] statements = {
      String.format("update %s set text = 'updated' where id = 0", taggedTable),
      String.format("delete from %s where id = 0", taggedTable),
      String.format("insert into %s values (10, 'inserted')", taggedTable),
      String.format(
          "merge into %s t using tag_write_source s on t.id = s.id "
              + "when matched then update set text = s.text",
          taggedTable),
      String.format("alter table %s add columns copied_text from tag_write_source", taggedTable),
      String.format("alter table %s update columns text from tag_write_source", taggedTable)
    };

    for (String statement : statements) {
      Assertions.assertThrows(
          Exception.class, () -> spark.sql(statement).collectAsList(), statement);
    }
    Assertions.assertEquals(10, spark.table(fullTable).count());
    Assertions.assertEquals(2, spark.table(fullTable).schema().size());
  }

  private DatasetVersions prepareDatasetWithHistory() {
    spark.sql(String.format("create table %s (id int, text string) using lance", fullTable));
    insertRange(0, 5);
    long firstInsertVersion = currentVersion();
    String firstInsertTag = "tag_" + firstInsertVersion;
    createTag(firstInsertTag);
    insertRange(5, 10);
    return new DatasetVersions(firstInsertTag);
  }

  private void insertRange(int startInclusive, int endExclusive) {
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s",
            fullTable,
            IntStream.range(startInclusive, endExclusive)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
  }

  private long currentVersion() {
    try (org.lance.Dataset dataset = org.lance.Dataset.open().uri(tableDir).build()) {
      return dataset.getVersion().getId();
    }
  }

  private void createTag(String tag) {
    try (org.lance.Dataset dataset = org.lance.Dataset.open().uri(tableDir).build()) {
      dataset.tags().create(tag, Ref.ofMain());
    }
  }

  private static final class DatasetVersions {
    private final String firstInsertTag;

    private DatasetVersions(String firstInsertTag) {
      this.firstInsertTag = firstInsertTag;
    }
  }
}
