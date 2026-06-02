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

/** Base tests for BRANCH DDL commands. */
public abstract class BaseBranchDDLTest {
  protected String catalogName = "lance_test";
  protected String tableName = "branch_test";
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
            .appName("lance-branch-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "branch_test_" + UUID.randomUUID().toString().replace("-", "");
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
  public void testCreateBranchFromLatestMain() {
    DatasetVersions versions = prepareDatasetWithHistory();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create branch if not exists branch_from_main", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("branch_from_main", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    BranchInfo branch = assertBranchExists(branches, "branch_from_main");
    Assertions.assertNull(branch.parentBranch);
    Assertions.assertEquals(versions.latestVersion, branch.parentVersion);
    Assertions.assertTrue(branch.createdAt > 0L, "Expected created_at to be positive");
    Assertions.assertTrue(branch.manifestSize >= 1, "Expected manifest_size to be positive");
  }

  @Test
  public void testCreateBranchFromSpecificMainVersion() {
    DatasetVersions versions = prepareDatasetWithHistory();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create branch if not exists branch_from_main_v1 "
                    + "as of version %d",
                fullTable, versions.firstInsertVersion));

    assertSingleNameSchema(result);
    Assertions.assertEquals("branch_from_main_v1", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    BranchInfo branch = assertBranchExists(branches, "branch_from_main_v1");
    Assertions.assertNull(branch.parentBranch);
    Assertions.assertEquals(versions.firstInsertVersion, branch.parentVersion);
  }

  @Test
  public void testCreateBranchFromBranchHead() {
    DatasetVersions versions = prepareDatasetWithHistory();

    spark.sql(String.format("alter table %s create branch if not exists source_branch", fullTable));

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create branch child_branch " + "as of branch source_branch",
                fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("child_branch", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    BranchInfo branch = assertBranchExists(branches, "child_branch");
    Assertions.assertEquals("source_branch", branch.parentBranch);
    Assertions.assertEquals(versions.latestVersion, branch.parentVersion);
  }

  @Test
  public void testCreateBranchFromBranchHeadWithBacktickQuotedSourceBranchName() {
    DatasetVersions versions = prepareDatasetWithHistory();
    String sourceBranchName = "source-branch";

    spark.sql(
        String.format(
            "alter table %s create branch if not exists `%s`", fullTable, sourceBranchName));

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create branch child_branch_from_quoted_source as of branch `%s`",
                fullTable, sourceBranchName));

    assertSingleNameSchema(result);
    Assertions.assertEquals(
        "child_branch_from_quoted_source", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    BranchInfo branch = assertBranchExists(branches, "child_branch_from_quoted_source");
    Assertions.assertEquals(sourceBranchName, branch.parentBranch);
    Assertions.assertEquals(versions.latestVersion, branch.parentVersion);
  }

  @Test
  public void testCreateBranchFromSpecificBranchVersion() {
    DatasetVersions versions = prepareDatasetWithHistory();

    spark.sql(
        String.format(
            "alter table %s create branch if not exists source_branch " + "as of version %d",
            fullTable, versions.firstInsertVersion));

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create branch child_branch_v1 "
                    + "as of branch source_branch version %d",
                fullTable, versions.firstInsertVersion));

    assertSingleNameSchema(result);
    Assertions.assertEquals("child_branch_v1", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    BranchInfo branch = assertBranchExists(branches, "child_branch_v1");
    Assertions.assertEquals("source_branch", branch.parentBranch);
    Assertions.assertEquals(versions.firstInsertVersion, branch.parentVersion);
  }

  @Test
  public void testCreateBranchFromSpecificTag() {
    DatasetVersions versions = prepareDatasetWithHistory();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "alter table %s create branch if not exists branch_from_tag " + "as of tag %s",
                fullTable, versions.firstInsertTag));

    assertSingleNameSchema(result);
    Assertions.assertEquals("branch_from_tag", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    BranchInfo branch = assertBranchExists(branches, "branch_from_tag");
    Assertions.assertNull(branch.parentBranch);
    Assertions.assertEquals(versions.firstInsertVersion, branch.parentVersion);
  }

  @Test
  public void testCreateBranchFailsWhenSourceBranchDoesNotExist() {
    prepareDatasetWithHistory();

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(
                    String.format(
                        "alter table %s create branch branch_from_missing_branch "
                            + "as of branch missing_branch",
                        fullTable))
                .collectAsList());

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    Assertions.assertFalse(branches.containsKey("branch_from_missing_branch"));
  }

  @Test
  public void testCreateBranchFailsWhenSourceTagDoesNotExist() {
    prepareDatasetWithHistory();

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(
                    String.format(
                        "alter table %s create branch branch_from_missing_tag as of tag missing_tag",
                        fullTable))
                .collectAsList());

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    Assertions.assertFalse(branches.containsKey("branch_from_missing_tag"));
  }

  @Test
  public void testCreateBranchFailsWhenSourceVersionDoesNotExist() {
    DatasetVersions versions = prepareDatasetWithHistory();
    long missingVersion = versions.latestVersion + 1000L;

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(
                    String.format(
                        "alter table %s create branch branch_from_missing_version "
                            + "as of version %d",
                        fullTable, missingVersion))
                .collectAsList());

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    Assertions.assertFalse(branches.containsKey("branch_from_missing_version"));
  }

  @Test
  public void testCreateBranchIfNotExistsNoOpWhenBranchExists() {
    DatasetVersions versions = prepareDatasetWithHistory();

    spark
        .sql(String.format("alter table %s create branch existing_branch", fullTable))
        .collectAsList();

    Dataset<Row> result =
        spark.sql(
            String.format("alter table %s create branch if not exists existing_branch", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("existing_branch", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    BranchInfo branch = assertBranchExists(branches, "existing_branch");
    Assertions.assertNull(branch.parentBranch);
    Assertions.assertEquals(versions.latestVersion, branch.parentVersion);
  }

  @Test
  public void testCreateBranchFailsWhenBranchExistsWithoutIfNotExists() {
    prepareDatasetWithHistory();

    spark
        .sql(String.format("alter table %s create branch duplicate_branch", fullTable))
        .collectAsList();

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(String.format("alter table %s create branch duplicate_branch", fullTable))
                .collectAsList());
  }

  @Test
  public void testDropBranch() {
    prepareDatasetWithHistory();

    spark.sql(String.format("alter table %s create branch branch_to_drop", fullTable));

    Dataset<Row> result =
        spark.sql(String.format("alter table %s drop branch if exists branch_to_drop", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("branch_to_drop", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    Assertions.assertFalse(branches.containsKey("branch_to_drop"));
  }

  @Test
  public void testDropBranchIfExistsNoOpWhenBranchDoesNotExist() {
    prepareDatasetWithHistory();

    Dataset<Row> result =
        spark.sql(String.format("alter table %s drop branch if exists missing_branch", fullTable));

    assertSingleNameSchema(result);
    Assertions.assertEquals("missing_branch", result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    Assertions.assertFalse(branches.containsKey("missing_branch"));
  }

  @Test
  public void testDropBranchFailsWhenBranchDoesNotExistWithoutIfExists() {
    prepareDatasetWithHistory();

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql(String.format("alter table %s drop branch missing_branch", fullTable))
                .collectAsList());
  }

  @Test
  public void testShowBranchAliasWithInSyntax() {
    prepareDatasetWithHistory();
    spark.sql(String.format("alter table %s create branch branch_for_show", fullTable));

    Map<String, BranchInfo> branches = showBranches(String.format("show branch in %s", fullTable));

    Assertions.assertTrue(
        branches.containsKey("branch_for_show"), "Expected created branch to be returned");
  }

  @Test
  public void testCreateBranchWithBacktickQuotedName() {
    prepareDatasetWithHistory();
    String branchName = "branch-with-dash";

    Dataset<Row> result =
        spark.sql(String.format("alter table %s create branch `%s`", fullTable, branchName));

    assertSingleNameSchema(result);
    Assertions.assertEquals(branchName, result.collectAsList().get(0).getString(0));

    Map<String, BranchInfo> branches =
        showBranches(String.format("show branches from %s", fullTable));
    Assertions.assertTrue(
        branches.containsKey(branchName), "Expected backtick-quoted branch to be returned");
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

  private Map<String, BranchInfo> showBranches(String sqlText) {
    Dataset<Row> result = spark.sql(sqlText);
    Assertions.assertEquals(
        "StructType(StructField(name,StringType,false),"
            + "StructField(parent_branch,StringType,true),"
            + "StructField(parent_version,LongType,false),"
            + "StructField(created_at,LongType,false),"
            + "StructField(manifest_size,IntegerType,false))",
        result.schema().toString());

    Map<String, BranchInfo> branches = new HashMap<>();
    for (Row row : result.collectAsList()) {
      branches.put(
          row.getString(0),
          new BranchInfo(row.getString(1), row.getLong(2), row.getLong(3), row.getInt(4)));
    }
    return branches;
  }

  private BranchInfo assertBranchExists(Map<String, BranchInfo> branches, String branchName) {
    BranchInfo branch = branches.get(branchName);
    Assertions.assertNotNull(branch, "Expected branch to exist: " + branchName);
    return branch;
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

  private static final class BranchInfo {
    private final String parentBranch;
    private final long parentVersion;
    private final long createdAt;
    private final int manifestSize;

    private BranchInfo(String parentBranch, long parentVersion, long createdAt, int manifestSize) {
      this.parentBranch = parentBranch;
      this.parentVersion = parentVersion;
      this.createdAt = createdAt;
      this.manifestSize = manifestSize;
    }
  }
}
