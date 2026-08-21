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
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.read.LanceSplit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

  @Test
  public void testBranchReadUsesBranchSchemaAfterMainAddsColumn() throws Exception {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));
    spark.sql(
        String.format(
            "create temporary view extra_cols as select _rowaddr, _fragid, 1 as extra from %s",
            fullTable));
    spark.sql(String.format("alter table %s add columns extra from extra_cols", fullTable));

    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    List<String> expectedBranchSchema =
        columnNames(
            spark.sql(
                "select * from " + fullTable + " version as of " + versions.firstInsertVersion));
    List<String> catalogSchema =
        Arrays.asList(
            catalog
                .loadTable(Identifier.of(new String[] {"default", tableName}, "branch_audit"))
                .schema()
                .fieldNames());
    List<String> identifierSchema = columnNames(spark.table(fullTable + ".branch_audit"));

    Assertions.assertEquals(expectedBranchSchema, catalogSchema);
    Assertions.assertEquals(expectedBranchSchema, identifierSchema);
  }

  @Test
  public void testBranchIdentifierPinsVersionOnLoad() throws Exception {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));

    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    LanceDataset table =
        (LanceDataset)
            catalog.loadTable(Identifier.of(new String[] {"default", tableName}, "branch_audit"));
    LanceRef ref = table.readOptions().getRef();

    Assertions.assertEquals("audit", ref.getBranchName().get());
    Assertions.assertEquals(versions.firstInsertVersion, ref.getVersionNumber().get());
  }

  @Test
  public void testBranchOptionPinsVersionOnScan() throws Exception {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));

    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    LanceDataset table =
        (LanceDataset) catalog.loadTable(Identifier.of(new String[] {"default"}, tableName));
    Map<String, String> options = new HashMap<>();
    options.put("branch", "audit");
    LanceInputPartition partition =
        (LanceInputPartition)
            table
                .newScanBuilder(new CaseInsensitiveStringMap(options))
                .build()
                .toBatch()
                .planInputPartitions()[0];
    LanceRef ref = partition.getReadOptions().getRef();

    Assertions.assertEquals("audit", ref.getBranchName().get());
    Assertions.assertEquals(versions.firstInsertVersion, ref.getVersionNumber().get());
  }

  @Test
  public void testTagScanPinsVersionNotName() {
    DatasetVersions versions = prepareDatasetWithHistory();
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(tableDir)
            .ref(LanceRef.ofTag(versions.firstInsertTag))
            .build();

    LanceRef plannedRef = LanceSplit.planScan(readOptions).getRef();

    Assertions.assertTrue(plannedRef.getTagName().isEmpty());
    Assertions.assertEquals(versions.firstInsertVersion, plannedRef.getVersionNumber().get());
  }

  @Test
  public void testReadBranchByOptionPathAndIdentifier() {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));
    insertRange(10, 15);

    Assertions.assertEquals(15, spark.table(fullTable).count());
    Assertions.assertEquals(5, spark.read().option("branch", "audit").table(fullTable).count());
    Assertions.assertEquals(
        5, spark.read().format("lance").option("branch", "audit").load(tableDir).count());
    Assertions.assertEquals(5, spark.table(fullTable + ".branch_audit").count());
    Assertions.assertEquals(
        5, spark.read().option("branch", "audit").table(fullTable + ".branch_audit").count());
  }

  @Test
  public void testExistingTableWinsOverBranchIdentifier() throws Exception {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));

    String literalTable = fullTable + ".branch_audit";
    spark.sql("create table " + literalTable + " (id int, text string) using lance");
    spark.sql("insert into " + literalTable + " values (99, 'literal')");

    Assertions.assertEquals(1, spark.table(literalTable).count());
    Assertions.assertEquals(99, spark.table(literalTable).collectAsList().get(0).getInt(0));
    Assertions.assertEquals(5, spark.read().option("branch", "audit").table(fullTable).count());

    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    LanceDataset table =
        (LanceDataset)
            catalog.loadTable(Identifier.of(new String[] {"default", tableName}, "branch_audit"));
    LanceRef ref = table.readOptions().getRef();
    Assertions.assertTrue(ref == null || ref.isMain());
  }

  @Test
  public void testBranchIdentifierKeepsRowsWhenBatchSizeIsSet() {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));
    insertRange(10, 15);

    Assertions.assertEquals(
        5, spark.read().option("batch_size", "1024").table(fullTable + ".branch_audit").count());
  }

  @Test
  public void testBranchScanPinsBranchAndVersion() {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(tableDir)
            .ref(LanceRef.ofBranch("audit"))
            .build();

    LanceRef plannedRef = LanceSplit.planScan(readOptions).getRef();

    Assertions.assertEquals("audit", plannedRef.getBranchName().get());
    Assertions.assertTrue(plannedRef.getVersionNumber().isPresent());
    Assertions.assertEquals(versions.firstInsertVersion, plannedRef.getVersionNumber().get());
  }

  @Test
  public void testMissingBranchFails() {
    prepareDatasetWithHistory();

    Exception missing =
        Assertions.assertThrows(
            Exception.class,
            () -> spark.read().option("branch", "no_such_branch").table(fullTable).collectAsList());
    Assertions.assertTrue(exceptionChainMessages(missing).contains("no_such_branch"));
  }

  @Test
  public void testBranchAndVersionOptionsFail() {
    prepareDatasetWithHistory();

    Exception conflicting =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .read()
                    .option("branch", "audit")
                    .option("version", "1")
                    .table(fullTable)
                    .collectAsList());
    String conflictMessages = exceptionChainMessages(conflicting);
    Assertions.assertTrue(conflictMessages.contains("branch"));
    Assertions.assertTrue(conflictMessages.contains("version"));
  }

  @Test
  public void testBranchIdentifierRejectsVersionAsOf() {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));

    Exception asOf =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql("select * from " + fullTable + ".branch_audit version as of 1")
                    .collectAsList());
    Assertions.assertTrue(exceptionChainMessages(asOf).contains("Cannot combine"));

    Exception timestampAsOf =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql("select * from " + fullTable + ".branch_audit timestamp as of now()")
                    .collectAsList());
    Assertions.assertTrue(exceptionChainMessages(timestampAsOf).contains("Cannot combine"));
  }

  @Test
  public void testBranchIdentifierRejectsDifferentBranchOption() {
    DatasetVersions versions = prepareDatasetWithHistory();
    spark.sql(
        String.format(
            "alter table %s create branch audit as of version %d",
            fullTable, versions.firstInsertVersion));

    Exception mismatch =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .read()
                    .option("branch", "other")
                    .table(fullTable + ".branch_audit")
                    .collectAsList());
    String mismatchMessages = exceptionChainMessages(mismatch);
    Assertions.assertTrue(mismatchMessages.contains("audit"));
    Assertions.assertTrue(mismatchMessages.contains("other"));
  }

  @Test
  public void testInsertIntoBranchIdentifierFails() {
    prepareDatasetWithHistory();
    spark.sql(String.format("alter table %s create branch audit", fullTable));

    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql("insert into " + fullTable + ".branch_audit values (99, 'branch')")
                .collectAsList());
    Assertions.assertThrows(
        Exception.class,
        () ->
            spark
                .sql("select 99 as id, 'branch' as text")
                .write()
                .format("lance")
                .option("branch", "audit")
                .mode("append")
                .save(tableDir));
    Assertions.assertEquals(10, spark.table(fullTable).count());
    Assertions.assertEquals(10, spark.table(fullTable + ".branch_audit").count());
  }

  @Test
  public void testBranchIdentifierRejectsCreateIndexAndVacuum() throws Exception {
    prepareDatasetWithHistory();
    spark.sql(String.format("alter table %s create branch audit", fullTable));

    TableCatalog catalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier branchIdentifier =
        Identifier.of(new String[] {"default", tableName}, "branch_audit");
    long branchVersionBefore =
        ((LanceDataset) catalog.loadTable(branchIdentifier))
            .readOptions()
            .getRef()
            .getVersionNumber()
            .get();
    long fileCountBefore = datasetFileCount();

    Exception createIndex =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        "alter table "
                            + fullTable
                            + ".branch_audit create index id_idx using zonemap (id) "
                            + "with (train=false)")
                    .collectAsList());
    Assertions.assertTrue(exceptionChainMessages(createIndex).contains("Writes are not supported"));

    Exception vacuum =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql("vacuum " + fullTable + ".branch_audit with (before_version=1000000)")
                    .collectAsList());
    Assertions.assertTrue(exceptionChainMessages(vacuum).contains("Writes are not supported"));

    long branchVersionAfter =
        ((LanceDataset) catalog.loadTable(branchIdentifier))
            .readOptions()
            .getRef()
            .getVersionNumber()
            .get();
    Assertions.assertEquals(branchVersionBefore, branchVersionAfter);
    Assertions.assertEquals(fileCountBefore, datasetFileCount());
    Assertions.assertEquals(10, spark.table(fullTable).count());
    Assertions.assertEquals(10, spark.table(fullTable + ".branch_audit").count());
  }

  private long datasetFileCount() throws IOException {
    try (Stream<Path> files = Files.walk(FileSystems.getDefault().getPath(tableDir))) {
      return files.filter(Files::isRegularFile).count();
    }
  }

  private static String exceptionChainMessages(Throwable throwable) {
    StringBuilder messages = new StringBuilder();
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null) {
        messages.append(current.getMessage()).append('\n');
      }
    }
    return messages.toString();
  }

  private static List<String> columnNames(Dataset<Row> rows) {
    return Arrays.asList(rows.columns());
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
