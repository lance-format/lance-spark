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

import org.apache.spark.sql.Dataset;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Base tests for TAG DDL commands. */
public abstract class BaseTagDDLTest {
  protected String catalogName = "lance_test";
  protected String tableName = "tag_test";
  protected String fullTable = catalogName + ".default." + tableName;

  protected SparkSession spark;

  @TempDir Path tempDir;
  protected String tableDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-tag-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "tag_test_" + UUID.randomUUID().toString().replace("-", "");
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

  @Test
  public void testCreateTagFromLatestMain() {
    DatasetVersions versions = prepareDatasetWithHistory();

    Dataset<Row> result =
        spark.sql(
            String.format("alter table %s create tag if not exists tag_from_main", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("tag_from_main", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    TagInfo tag = assertTagExists(tags, "tag_from_main");
    Assertions.assertNull(tag.branch);
    Assertions.assertEquals(versions.latestVersion, tag.version);
    Assertions.assertNotNull(tag.createdAt);
    Assertions.assertNotNull(tag.updatedAt);
    Assertions.assertTrue(tag.manifestSize >= 1, "Expected manifest_size to be positive");
  }

  @Test
  public void testCreateTagFromSpecificMainVersion() {
    DatasetVersions versions = prepareDatasetWithHistory();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create tag if not exists tag_from_main_v1 as of version %d",
                fullTable, versions.firstInsertVersion));

    assertSingleNameSchema(result);
    Assertions.assertEquals("tag_from_main_v1", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    TagInfo tag = assertTagExists(tags, "tag_from_main_v1");
    Assertions.assertNull(tag.branch);
    Assertions.assertEquals(versions.firstInsertVersion, tag.version);
  }

  @Test
  public void testCreateTagFromBranchHead() {
    DatasetVersions versions = prepareDatasetWithHistory();

    spark.sql(String.format("alter table %s create branch if not exists source_branch", fullTable));

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create tag tag_from_branch as of branch source_branch", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("tag_from_branch", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    TagInfo tag = assertTagExists(tags, "tag_from_branch");
    Assertions.assertEquals("source_branch", tag.branch);
    Assertions.assertEquals(versions.latestVersion, tag.version);
  }

  @Test
  public void testCreateTagFromBranchHeadWithBacktickQuotedSourceBranchName() {
    DatasetVersions versions = prepareDatasetWithHistory();
    String sourceBranchName = "source-branch";

    spark.sql(
        String.format(
            "alter table %s create branch if not exists `%s`", fullTable, sourceBranchName));

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create tag tag_from_quoted_branch as of branch `%s`",
                fullTable, sourceBranchName));

    assertSingleNameSchema(result);
    Assertions.assertEquals("tag_from_quoted_branch", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    TagInfo tag = assertTagExists(tags, "tag_from_quoted_branch");
    Assertions.assertEquals(sourceBranchName, tag.branch);
    Assertions.assertEquals(versions.latestVersion, tag.version);
  }

  @Test
  public void testCreateTagFromSpecificBranchVersion() {
    DatasetVersions versions = prepareDatasetWithHistory();

    spark.sql(
        String.format(
            "alter table %s create branch if not exists source_branch as of version %d",
            fullTable, versions.firstInsertVersion));

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create tag tag_from_branch_v1 as of branch source_branch version %d",
                fullTable, versions.firstInsertVersion));

    assertSingleNameSchema(result);
    Assertions.assertEquals("tag_from_branch_v1", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    TagInfo tag = assertTagExists(tags, "tag_from_branch_v1");
    Assertions.assertEquals("source_branch", tag.branch);
    Assertions.assertEquals(versions.firstInsertVersion, tag.version);
  }

  @Test
  public void testCreateTagFromSpecificTag() {
    DatasetVersions versions = prepareDatasetWithHistory();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create tag if not exists tag_from_tag as of tag %s",
                fullTable, versions.firstInsertTag));

    assertSingleNameSchema(result);
    Assertions.assertEquals("tag_from_tag", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    TagInfo tag = assertTagExists(tags, "tag_from_tag");
    Assertions.assertNull(tag.branch);
    Assertions.assertEquals(versions.firstInsertVersion, tag.version);
  }

  @Test
  public void testCreateTagFailsWhenSourceBranchDoesNotExist() {
    prepareDatasetWithHistory();

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(
                    String.format(
                        "alter table %s create tag tag_from_missing_branch as of branch missing_branch",
                        fullTable))
                .collectAsList());

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    Assertions.assertFalse(tags.containsKey("tag_from_missing_branch"));
  }

  @Test
  public void testCreateTagFailsWhenSourceTagDoesNotExist() {
    prepareDatasetWithHistory();

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(
                    String.format(
                        "alter table %s create tag tag_from_missing_tag as of tag missing_tag",
                        fullTable))
                .collectAsList());

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    Assertions.assertFalse(tags.containsKey("tag_from_missing_tag"));
  }

  @Test
  public void testCreateTagFailsWhenSourceVersionDoesNotExist() {
    DatasetVersions versions = prepareDatasetWithHistory();
    long missingVersion = versions.latestVersion + 1000L;

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(
                    String.format(
                        "alter table %s create tag tag_from_missing_version as of version %d",
                        fullTable, missingVersion))
                .collectAsList());

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    Assertions.assertFalse(tags.containsKey("tag_from_missing_version"));
  }

  @Test
  public void testCreateTagIfNotExistsNoOpWhenTagExists() {
    DatasetVersions versions = prepareDatasetWithHistory();

    spark.sql(String.format("alter table %s create tag existing_tag", fullTable)).collectAsList();

    Dataset<Row> result =
        spark.sql(String.format("alter table %s create tag if not exists existing_tag", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("existing_tag", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    TagInfo tag = assertTagExists(tags, "existing_tag");
    Assertions.assertNull(tag.branch);
    Assertions.assertEquals(versions.latestVersion, tag.version);
  }

  @Test
  public void testCreateTagFailsWhenTagExistsWithoutIfNotExists() {
    prepareDatasetWithHistory();

    spark.sql(String.format("alter table %s create tag duplicate_tag", fullTable)).collectAsList();

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(String.format("alter table %s create tag duplicate_tag", fullTable))
                .collectAsList());
  }

  @Test
  public void testDropTag() {
    prepareDatasetWithHistory();

    spark.sql(String.format("alter table %s create tag tag_to_drop", fullTable));

    Dataset<Row> result =
        spark.sql(String.format("alter table %s drop tag if exists tag_to_drop", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("tag_to_drop", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    Assertions.assertFalse(tags.containsKey("tag_to_drop"));
  }

  @Test
  public void testDropTagIfExistsNoOpWhenTagDoesNotExist() {
    prepareDatasetWithHistory();

    Dataset<Row> result =
        spark.sql(String.format("alter table %s drop tag if exists missing_tag", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("missing_tag", result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    Assertions.assertFalse(tags.containsKey("missing_tag"));
  }

  @Test
  public void testDropTagFailsWhenTagDoesNotExistWithoutIfExists() {
    prepareDatasetWithHistory();

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(String.format("alter table %s drop tag missing_tag", fullTable))
                .collectAsList());
  }

  @Test
  public void testShowTagAliasWithInSyntax() {
    prepareDatasetWithHistory();
    spark.sql(String.format("alter table %s create tag tag_for_show", fullTable));

    Map<String, TagInfo> tags = showTags(String.format("show tag in %s", fullTable));

    Assertions.assertTrue(tags.containsKey("tag_for_show"), "Expected created tag to be returned");
  }

  @Test
  public void testCreateTagWithBacktickQuotedName() {
    prepareDatasetWithHistory();
    String tagName = "tag-with-dash";

    Dataset<Row> result =
        spark.sql(String.format("alter table %s create tag `%s`", fullTable, tagName));

    assertSingleNameSchema(result);
    Assertions.assertEquals(tagName, result.collectAsList().get(0).getString(0));

    Map<String, TagInfo> tags = showTags(String.format("show tags from %s", fullTable));
    Assertions.assertTrue(tags.containsKey(tagName), "Expected backtick-quoted tag to be returned");
  }

  private DatasetVersions prepareDatasetWithHistory() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    insertRange(0, 5);
    long firstInsertVersion = currentVersion();
    String firstInsertTag = "tag_" + firstInsertVersion;
    createTag(firstInsertTag);
    insertRange(5, 10);
    long latestVersion = currentVersion();
    return new DatasetVersions(firstInsertVersion, firstInsertTag, latestVersion);
  }

  private void insertRange(int startInclusive, int endExclusive) {
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
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

  private Map<String, TagInfo> showTags(String sqlText) {
    Dataset<Row> result = spark.sql(sqlText);
    Assertions.assertEquals(
        "StructType(StructField(name,StringType,false),"
            + "StructField(branch,StringType,true),"
            + "StructField(version,LongType,false),"
            + "StructField(created_at,LongType,true),"
            + "StructField(updated_at,LongType,true),"
            + "StructField(manifest_size,IntegerType,false))",
        result.schema().toString());

    Map<String, TagInfo> tags = new HashMap<>();
    for (Row row : result.collectAsList()) {
      tags.put(
          row.getString(0),
          new TagInfo(
              row.isNullAt(1) ? null : row.getString(1),
              row.getLong(2),
              row.isNullAt(3) ? null : row.getLong(3),
              row.isNullAt(4) ? null : row.getLong(4),
              row.getInt(5)));
    }
    return tags;
  }

  private TagInfo assertTagExists(Map<String, TagInfo> tags, String tagName) {
    TagInfo tag = tags.get(tagName);
    Assertions.assertNotNull(tag, "Expected tag to exist: " + tagName);
    return tag;
  }

  private void assertSingleNameSchema(Dataset<Row> result) {
    Assertions.assertEquals(
        "StructType(StructField(name,StringType,false))", result.schema().toString());
  }

  private static final class DatasetVersions {
    private final long firstInsertVersion;
    private final String firstInsertTag;
    private final long latestVersion;

    private DatasetVersions(long firstInsertVersion, String firstInsertTag, long latestVersion) {
      this.firstInsertVersion = firstInsertVersion;
      this.firstInsertTag = firstInsertTag;
      this.latestVersion = latestVersion;
    }
  }

  private static final class TagInfo {
    private final String branch;
    private final long version;
    private final Long createdAt;
    private final Long updatedAt;
    private final int manifestSize;

    private TagInfo(String branch, long version, Long createdAt, Long updatedAt, int manifestSize) {
      this.branch = branch;
      this.version = version;
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
      this.manifestSize = manifestSize;
    }
  }
}
