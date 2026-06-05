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
package org.lance.spark.branch;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BaseTagTest {
  protected String catalogName = "lance_test";
  protected String tableName = "tag_test";
  protected String fullTable = catalogName + ".default." + tableName;

  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-tag-test")
            .master("local[10]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .getOrCreate();
    this.tableName = "tag_test_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  private void prepareDataset() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    // First insert to create initial fragments
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
    // Second insert to ensure multiple fragments
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(10, 20)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
  }

  @Test
  public void testCreateTag() {
    prepareDataset();

    spark.sql(String.format("alter table %s create tag tag_0", fullTable)).collectAsList();
    for (Row row : spark.sql(String.format("show tags in %s", fullTable)).collectAsList()) {
      Assertions.assertEquals("tag_0", row.getString(0));
      Assertions.assertTrue(row.getLong(1) > 0);
      Assertions.assertTrue(row.getInt(2) > 0);
    }

    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(10, 20)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));

    spark.sql(String.format("alter table %s create tag tag_1", fullTable)).collectAsList();

    List<Row> rows = spark.sql(String.format("show tags in %s", fullTable)).collectAsList();
    Assertions.assertEquals(2, rows.size());
  }

  @Test
  public void testDropTag() {
    prepareDataset();

    spark.sql(String.format("alter table %s create tag tag_0", fullTable));
    for (Row row : spark.sql(String.format("show tags in %s", fullTable)).collectAsList()) {
      Assertions.assertEquals("tag_0", row.getString(0));
      Assertions.assertTrue(row.getLong(1) > 0);
      Assertions.assertTrue(row.getInt(2) > 0);
    }

    spark.sql(String.format("alter table %s drop tag tag_0", fullTable));
    Assertions.assertEquals(
        0, spark.sql(String.format("show tags in %s", fullTable)).collectAsList().size());

    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(10, 20)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));

    spark.sql(String.format("alter table %s create tag tag_1", fullTable)).collectAsList();

    List<Row> rows = spark.sql(String.format("show tags in %s", fullTable)).collectAsList();
    Assertions.assertEquals(1, rows.size());

    for (Row row : rows) {
      Assertions.assertEquals("tag_1", row.getString(0));
      Assertions.assertTrue(row.getLong(1) > 0);
      Assertions.assertTrue(row.getInt(2) > 0);
    }
  }

  @Test
  public void testQueryFromTag() {
    prepareDataset();

    spark.sql(String.format("alter table %s create tag tag_0", fullTable)).collectAsList();

    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(20, 30)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));

    List<Row> rows =
        spark
            .sql(String.format("select * from %s__tag__tag_0 where id >= 10", fullTable))
            .collectAsList();
    Assertions.assertEquals(10, rows.size());

    spark.sql(String.format("alter table %s create tag tag_1", fullTable)).collectAsList();

    rows = spark.sql(String.format("select * from %s__tag__tag_1", fullTable)).collectAsList();
    Assertions.assertEquals(30, rows.size());
  }
}
