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

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.WriteParams.WriteMode;
import org.lance.fragment.DataFile;
import org.lance.schema.LanceField;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;

import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceBatchWriteTest {
  @TempDir static Path tempDir;

  @Test
  public void testTaskCommitRejectsNullFragments() {
    NullPointerException error =
        assertThrows(NullPointerException.class, () -> LanceBatchWrite.taskCommit(null));

    assertEquals("fragments must not be null", error.getMessage());
  }

  @Test
  public void testTaskCommitRejectsNullFragmentElements() {
    FragmentMetadata fragment = new FragmentMetadata(1, Collections.emptyList(), 0L, null, null);

    NullPointerException error =
        assertThrows(
            NullPointerException.class,
            () -> LanceBatchWrite.taskCommit(Arrays.asList(fragment, null)));

    assertEquals("fragments must not contain null", error.getMessage());
  }

  @Test
  public void testTaskCommitDefensivelyCopiesFragments() {
    FragmentMetadata first = new FragmentMetadata(1, Collections.emptyList(), 0L, null, null);
    FragmentMetadata second = new FragmentMetadata(2, Collections.emptyList(), 0L, null, null);
    List<FragmentMetadata> fragments = new ArrayList<>(Arrays.asList(first, second, first));

    WriterCommitMessage message = LanceBatchWrite.taskCommit(fragments);
    LanceBatchWrite.TaskCommit taskCommit =
        assertInstanceOf(LanceBatchWrite.TaskCommit.class, message);
    fragments.clear();

    assertEquals(Arrays.asList(first, second, first), taskCommit.getFragments());
  }

  @Test
  public void testTaskCommitAcceptsEmptyFragments() {
    LanceBatchWrite.TaskCommit taskCommit =
        assertInstanceOf(
            LanceBatchWrite.TaskCommit.class, LanceBatchWrite.taskCommit(Collections.emptyList()));

    assertTrue(taskCommit.getFragments().isEmpty());
  }

  @Test
  public void testLanceDataWriter(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // Create lance dataset
      Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(field));
      Dataset.create(allocator, datasetUri, schema, new WriteParams.Builder().build()).close();

      // Append data to lance dataset
      LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
      LanceBatchWrite lanceBatchWrite =
          new LanceBatchWrite(
              sparkSchema,
              writeOptions,
              false,
              null, // initialStorageOptions
              null, // namespaceImpl
              null, // namespaceProperties
              null, // tableId
              false, // managedVersioning
              null); // stagedCommit
      DataWriterFactory factor = lanceBatchWrite.createBatchWriterFactory(() -> 1);

      int rows = 132;
      WriterCommitMessage message;
      try (DataWriter<InternalRow> writer = factor.createWriter(0, 0)) {
        for (int i = 0; i < rows; i++) {
          InternalRow row = new GenericInternalRow(new Object[] {i});
          writer.write(row);
        }
        message = writer.commit();
      }
      lanceBatchWrite.commit(new WriterCommitMessage[] {message});

      // Validate lance dataset data
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        try (Scanner scanner = dataset.newScan()) {
          try (ArrowReader reader = scanner.scanBatches()) {
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
            int totalRowsRead = 0;
            while (reader.loadNextBatch()) {
              int batchRows = readerRoot.getRowCount();
              for (int i = 0; i < batchRows; i++) {
                int value = (int) readerRoot.getVector("column1").getObject(i);
                assertEquals(totalRowsRead + i, value);
              }
              totalRowsRead += batchRows;
            }
            assertEquals(rows, totalRowsRead);
          }
        }
      }
    }
  }

  /**
   * Appends fragments written directly through the Lance API, the way an external columnar writer
   * such as Apache Gluten does, to a table whose field ids are not contiguous.
   */
  @Test
  public void testTaskCommitAppendsExternallyWrittenFragments(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createTableWithDroppedMiddleColumn(allocator, datasetUri);
      LanceBatchWrite batchWrite = newBatchWrite(schema, datasetUri, false);

      // Two tasks, each writing several fragments in APPEND mode.
      List<FragmentMetadata> first =
          writeFragments(allocator, datasetUri, schema, WriteMode.APPEND, 0, 25);
      List<FragmentMetadata> second =
          writeFragments(allocator, datasetUri, schema, WriteMode.APPEND, 25, 25);
      assertTrue(first.size() > 1);
      assertTrue(second.size() > 1);

      batchWrite.commit(
          new WriterCommitMessage[] {
            LanceBatchWrite.taskCommit(first), LanceBatchWrite.taskCommit(second)
          });

      assertEquals(expectedRows(0, 50), readRows(allocator, datasetUri));
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        assertEquals(first.size() + second.size(), dataset.getFragments().size());
      }
    }
  }

  /**
   * Overwrites a table whose field ids are not contiguous with fragments written directly through
   * the Lance API in OVERWRITE mode, without passing the table's schema.
   */
  @Test
  public void testTaskCommitOverwritesWithExternallyWrittenFragments(TestInfo testInfo)
      throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createTableWithDroppedMiddleColumn(allocator, datasetUri);

      // Seed rows that the overwrite must replace.
      newBatchWrite(schema, datasetUri, false)
          .commit(
              new WriterCommitMessage[] {
                LanceBatchWrite.taskCommit(
                    writeFragments(allocator, datasetUri, schema, WriteMode.APPEND, 0, 5))
              });
      assertEquals(expectedRows(0, 5), readRows(allocator, datasetUri));

      LanceBatchWrite batchWrite = newBatchWrite(schema, datasetUri, true);

      // Two tasks, each writing several fragments in OVERWRITE mode.
      List<FragmentMetadata> first =
          writeFragments(allocator, datasetUri, schema, WriteMode.OVERWRITE, 100, 25);
      List<FragmentMetadata> second =
          writeFragments(allocator, datasetUri, schema, WriteMode.OVERWRITE, 125, 25);
      assertTrue(first.size() > 1);
      assertTrue(second.size() > 1);

      batchWrite.commit(
          new WriterCommitMessage[] {
            LanceBatchWrite.taskCommit(first), LanceBatchWrite.taskCommit(second)
          });

      assertEquals(expectedRows(100, 50), readRows(allocator, datasetUri));
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        assertEquals(first.size() + second.size(), dataset.getFragments().size());
      }
    }
  }

  /** {@code isOverwrite} reports the branch {@code commit} takes, not what the query asked for. */
  @Test
  public void testIsOverwriteMatchesTheCommit(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createTableWithDroppedMiddleColumn(allocator, datasetUri);
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

      assertFalse(newBatchWrite(schema, datasetUri, false).isOverwrite());
      assertTrue(newBatchWrite(schema, datasetUri, true).isOverwrite());

      // The query appends, but the write_mode option makes the commit overwrite.
      LanceSparkWriteOptions writeModeOverwrite =
          LanceSparkWriteOptions.from(
              Collections.singletonMap("write_mode", "overwrite"), datasetUri);
      assertTrue(newBatchWrite(sparkSchema, writeModeOverwrite, false, null).isOverwrite());

      // A staged create or replace always commits as an overwrite.
      String stagedUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName + "_staged");
      StagedCommit stagedCommit =
          StagedCommit.forNewTable(
              schema,
              stagedUri,
              StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
      assertTrue(
          newBatchWrite(sparkSchema, LanceSparkWriteOptions.from(stagedUri), false, stagedCommit)
              .isOverwrite());
    }
  }

  /**
   * An append query with {@code write_mode=overwrite} commits as an overwrite, so an external
   * writer must pick its write mode from {@code isOverwrite}.
   */
  @Test
  public void testTaskCommitFollowsWriteModeOption(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createTableWithDroppedMiddleColumn(allocator, datasetUri);
      newBatchWrite(schema, datasetUri, false)
          .commit(
              new WriterCommitMessage[] {
                LanceBatchWrite.taskCommit(
                    writeFragments(allocator, datasetUri, schema, WriteMode.APPEND, 0, 5))
              });

      LanceBatchWrite batchWrite =
          newBatchWrite(
              LanceArrowUtils.fromArrowSchema(schema),
              LanceSparkWriteOptions.from(
                  Collections.singletonMap("write_mode", "overwrite"), datasetUri),
              false,
              null);
      WriteMode mode = batchWrite.isOverwrite() ? WriteMode.OVERWRITE : WriteMode.APPEND;

      batchWrite.commit(
          new WriterCommitMessage[] {
            LanceBatchWrite.taskCommit(writeFragments(allocator, datasetUri, schema, mode, 100, 25))
          });

      assertEquals(expectedRows(100, 25), readRows(allocator, datasetUri));
    }
  }

  /** {@code getCommitSchema} reflects resolved options an external writer cannot see. */
  @Test
  public void testGetCommitSchemaFollowsResolvedOptions(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    StructType sparkSchema =
        new StructType()
            .add("ts", DataTypes.TimestampType)
            .add("s", DataTypes.StringType)
            .add("b", DataTypes.BinaryType);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(
              allocator,
              datasetUri,
              LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", true, false),
              new WriteParams.Builder().build())
          .close();

      Schema regular =
          newBatchWrite(sparkSchema, LanceSparkWriteOptions.from(datasetUri), true, null)
              .getCommitSchema();
      assertEquals(
          new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), regular.findField("ts").getType());
      assertEquals(ArrowType.Utf8.INSTANCE, regular.findField("s").getType());
      assertEquals(ArrowType.Binary.INSTANCE, regular.findField("b").getType());

      Schema large =
          newBatchWrite(
                  sparkSchema,
                  LanceSparkWriteOptions.from(
                      Collections.singletonMap("use_large_var_types", "true"), datasetUri),
                  true,
                  null)
              .getCommitSchema();
      assertEquals(ArrowType.LargeUtf8.INSTANCE, large.findField("s").getType());
      assertEquals(ArrowType.LargeBinary.INSTANCE, large.findField("b").getType());
    }
  }

  /**
   * Overwrites with a stream that uses the column types of {@code getCommitSchema}: microsecond
   * timestamps and, with {@code use_large_var_types}, large strings.
   */
  @Test
  public void testTaskCommitOverwritesWithCommitSchemaTypes(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    StructType sparkSchema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("ts", DataTypes.TimestampType)
            .add("s", DataTypes.StringType);
    int rows = 25;
    long baseMicros = 1_700_000_000_000_000L;
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(
              allocator,
              datasetUri,
              LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", true, false),
              new WriteParams.Builder().build())
          .close();
      LanceBatchWrite batchWrite =
          newBatchWrite(
              sparkSchema,
              LanceSparkWriteOptions.from(
                  Collections.singletonMap("use_large_var_types", "true"), datasetUri),
              true,
              null);

      List<FragmentMetadata> fragments;
      try (VectorSchemaRoot root =
          VectorSchemaRoot.create(batchWrite.getCommitSchema(), allocator)) {
        IntVector id = (IntVector) root.getVector("id");
        TimeStampMicroTZVector ts = (TimeStampMicroTZVector) root.getVector("ts");
        LargeVarCharVector s = (LargeVarCharVector) root.getVector("s");
        for (int i = 0; i < rows; i++) {
          id.setSafe(i, i);
          ts.setSafe(i, baseMicros + i);
          s.setSafe(i, ("row-" + i).getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(rows);
        fragments =
            Fragment.write()
                .datasetUri(datasetUri)
                .allocator(allocator)
                .data(root)
                .mode(WriteMode.OVERWRITE)
                .execute();
      }
      batchWrite.commit(new WriterCommitMessage[] {LanceBatchWrite.taskCommit(fragments)});

      Map<Integer, String> expectedTs = new HashMap<>();
      Map<Integer, String> expectedS = new HashMap<>();
      for (int i = 0; i < rows; i++) {
        expectedTs.put(i, String.valueOf(baseMicros + i));
        expectedS.put(i, "row-" + i);
      }
      assertEquals(expectedTs, readColumnById(allocator, datasetUri, "ts"));
      assertEquals(expectedS, readColumnById(allocator, datasetUri, "s"));
    }
  }

  /**
   * A staged replace commits as an overwrite, so fragments written in OVERWRITE mode replace a
   * table whose field ids are not contiguous.
   */
  @Test
  public void testTaskCommitReplacesTableThroughStagedCommit(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = createTableWithDroppedMiddleColumn(allocator, datasetUri);
      newBatchWrite(schema, datasetUri, false)
          .commit(
              new WriterCommitMessage[] {
                LanceBatchWrite.taskCommit(
                    writeFragments(allocator, datasetUri, schema, WriteMode.APPEND, 0, 5))
              });

      StagedCommit stagedCommit =
          StagedCommit.forExistingTable(
              Dataset.open(datasetUri, allocator),
              schema,
              StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
      LanceBatchWrite batchWrite =
          newBatchWrite(
              LanceArrowUtils.fromArrowSchema(schema),
              LanceSparkWriteOptions.from(datasetUri),
              false,
              stagedCommit);
      assertTrue(batchWrite.isOverwrite());

      batchWrite.commit(
          new WriterCommitMessage[] {
            LanceBatchWrite.taskCommit(
                writeFragments(allocator, datasetUri, schema, WriteMode.OVERWRITE, 100, 25))
          });
      stagedCommit.commit();

      assertEquals(expectedRows(100, 25), readRows(allocator, datasetUri));
    }
  }

  /**
   * A staged create commits as an overwrite, and the table does not exist yet when its fragments
   * are written.
   */
  @Test
  public void testTaskCommitCreatesTableThroughStagedCommit(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = new Schema(Arrays.asList(intField("a"), intField("c")));
      StagedCommit stagedCommit =
          StagedCommit.forNewTable(
              schema,
              datasetUri,
              StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
      LanceBatchWrite batchWrite =
          newBatchWrite(
              LanceArrowUtils.fromArrowSchema(schema),
              LanceSparkWriteOptions.from(datasetUri),
              false,
              stagedCommit);
      assertTrue(batchWrite.isOverwrite());

      batchWrite.commit(
          new WriterCommitMessage[] {
            LanceBatchWrite.taskCommit(
                writeFragments(allocator, datasetUri, schema, WriteMode.OVERWRITE, 0, 25))
          });
      stagedCommit.commit();

      assertEquals(expectedRows(0, 25), readRows(allocator, datasetUri));
    }
  }

  /**
   * The spec carries the Lance write parameters the commit expects: the write mode from {@code
   * isOverwrite()}, resolved options, and storage options vended by a namespace. It survives
   * serialization.
   */
  @Test
  public void testFragmentWriteSpecCarriesResolvedOptions(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = new Schema(Arrays.asList(intField("a"), intField("c")));
      Dataset.create(allocator, datasetUri, schema, new WriteParams.Builder().build()).close();
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
      Map<String, String> options = new HashMap<>();
      options.put(LanceSparkWriteOptions.CONFIG_FILE_FORMAT_VERSION, "2.1");
      options.put(LanceSparkWriteOptions.CONFIG_MAX_ROWS_PER_FILE, "10");
      options.put(LanceSparkWriteOptions.CONFIG_ENABLE_STABLE_ROW_IDS, "true");
      // write_mode=create still gives APPEND: the spec takes the write mode from isOverwrite().
      options.put(LanceSparkWriteOptions.CONFIG_WRITE_MODE, "create");
      LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(options, datasetUri);
      Map<String, String> vended = new HashMap<>();
      vended.put("access_key_id", "from-namespace");

      FragmentWriteSpec appendSpec =
          new LanceBatchWrite(
                  sparkSchema, writeOptions, false, vended, null, null, null, false, null)
              .getFragmentWriteSpec();
      // The spec keeps its own copy of the vended options.
      vended.put("access_key_id", "changed");
      FragmentWriteSpec shipped = TestUtils.roundTrip(appendSpec);
      WriteParams params = shipped.toWriteParams();
      assertEquals(datasetUri, shipped.getDatasetUri());
      assertEquals(Optional.of("APPEND"), params.getMode());
      assertEquals(Optional.of("2.1"), params.getDataStorageVersion());
      assertEquals(Optional.of(10), params.getMaxRowsPerFile());
      // Stable row ids are assigned at commit, as for the row writer's fragments.
      assertEquals(Optional.of(false), params.getEnableStableRowIds());
      assertEquals("from-namespace", params.getStorageOptions().get("access_key_id"));

      FragmentWriteSpec overwriteSpec =
          new LanceBatchWrite(
                  sparkSchema, writeOptions, true, vended, null, null, null, false, null)
              .getFragmentWriteSpec();
      assertEquals(Optional.of("OVERWRITE"), overwriteSpec.toWriteParams().getMode());
    }
  }

  /** Fragments written with the spec keep a table's pinned file format version, not the default. */
  @Test
  public void testFragmentWriteSpecKeepsPinnedFileFormatVersion(TestInfo testInfo)
      throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = new Schema(Arrays.asList(intField("a"), intField("c")));
      Dataset.create(
              allocator,
              datasetUri,
              schema,
              new WriteParams.Builder().withDataStorageVersion("2.1").build())
          .close();
      // LanceDataset merges the table's file_format_version into the write options.
      LanceSparkWriteOptions writeOptions =
          LanceSparkWriteOptions.builder()
              .datasetUri(datasetUri)
              .fileFormatVersion("2.1")
              .maxRowsPerFile(10)
              .build();
      LanceBatchWrite batchWrite =
          newBatchWrite(LanceArrowUtils.fromArrowSchema(schema), writeOptions, false, null);

      List<FragmentMetadata> fragments =
          writeFragments(allocator, batchWrite.getFragmentWriteSpec(), schema, 0, 25);
      assertTrue(fragments.size() > 1);
      assertFileFormatVersion("2.1", fragments);

      batchWrite.commit(new WriterCommitMessage[] {LanceBatchWrite.taskCommit(fragments)});
      assertEquals(expectedRows(0, 25), readRows(allocator, datasetUri));
    }
  }

  /** A staged commit pinned to a file format version when it was staged makes the spec use it. */
  @Test
  public void testFragmentWriteSpecUsesStagedFileFormatVersion(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = new Schema(Arrays.asList(intField("a"), intField("c")));
      StagedCommit stagedCommit =
          StagedCommit.forNewTable(
              schema,
              datasetUri,
              StagedCommitOptions.pathBased(Collections.emptyMap(), false, "2.1"));
      LanceBatchWrite batchWrite =
          newBatchWrite(
              LanceArrowUtils.fromArrowSchema(schema),
              LanceSparkWriteOptions.from(datasetUri),
              false,
              stagedCommit);
      FragmentWriteSpec spec = batchWrite.getFragmentWriteSpec();
      // A staged create or replace commits as an overwrite.
      assertEquals(Optional.of("OVERWRITE"), spec.toWriteParams().getMode());
      assertEquals(Optional.of("2.1"), spec.toWriteParams().getDataStorageVersion());

      List<FragmentMetadata> fragments = writeFragments(allocator, spec, schema, 0, 25);
      assertFileFormatVersion("2.1", fragments);
      batchWrite.commit(new WriterCommitMessage[] {LanceBatchWrite.taskCommit(fragments)});
      stagedCommit.commit();

      assertEquals(expectedRows(0, 25), readRows(allocator, datasetUri));
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        assertEquals("2.1", dataset.getLanceFileFormatVersion());
      }
    }
  }

  private static Field intField(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  /**
   * Creates an empty table {@code (a, b, c)} and drops {@code b}, which leaves the field ids {@code
   * a=0, c=2}. Returns the remaining columns as an Arrow schema without field ids.
   */
  private static Schema createTableWithDroppedMiddleColumn(
      BufferAllocator allocator, String datasetUri) {
    Schema tableSchema = new Schema(Arrays.asList(intField("a"), intField("b"), intField("c")));
    try (Dataset dataset =
        Dataset.create(allocator, datasetUri, tableSchema, new WriteParams.Builder().build())) {
      dataset.dropColumns(Collections.singletonList("b"));
    }
    try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
      List<Integer> fieldIds =
          dataset.getLanceSchema().fields().stream()
              .map(LanceField::getId)
              .collect(Collectors.toList());
      assertEquals(Arrays.asList(0, 2), fieldIds);
    }
    return new Schema(Arrays.asList(intField("a"), intField("c")));
  }

  private static LanceBatchWrite newBatchWrite(
      Schema schema, String datasetUri, boolean overwrite) {
    return newBatchWrite(
        LanceArrowUtils.fromArrowSchema(schema),
        LanceSparkWriteOptions.from(datasetUri),
        overwrite,
        null);
  }

  private static LanceBatchWrite newBatchWrite(
      StructType sparkSchema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      StagedCommit stagedCommit) {
    return new LanceBatchWrite(
        sparkSchema,
        writeOptions,
        overwrite,
        null, // initialStorageOptions
        null, // namespaceImpl
        null, // namespaceProperties
        null, // tableId
        false, // managedVersioning
        stagedCommit);
  }

  /** Writes rows {@code a = start, start + 1, ...} with {@code c = 10 * a} as fragments. */
  private static List<FragmentMetadata> writeFragments(
      BufferAllocator allocator,
      String datasetUri,
      Schema schema,
      WriteMode mode,
      int start,
      int rows) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      fillRows(root, start, rows);
      return Fragment.write()
          .datasetUri(datasetUri)
          .allocator(allocator)
          .data(root)
          .mode(mode)
          .maxRowsPerFile(10)
          .execute();
    }
  }

  /** Writes the same rows with the parameters of {@code spec}, as an external writer does. */
  private static List<FragmentMetadata> writeFragments(
      BufferAllocator allocator, FragmentWriteSpec spec, Schema schema, int start, int rows) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      fillRows(root, start, rows);
      return Fragment.write()
          .datasetUri(spec.getDatasetUri())
          .allocator(allocator)
          .data(root)
          .writeParams(spec.toWriteParams())
          .execute();
    }
  }

  private static void fillRows(VectorSchemaRoot root, int start, int rows) {
    IntVector a = (IntVector) root.getVector("a");
    IntVector c = (IntVector) root.getVector("c");
    a.allocateNew(rows);
    c.allocateNew(rows);
    for (int i = 0; i < rows; i++) {
      a.set(i, start + i);
      c.set(i, 10 * (start + i));
    }
    root.setRowCount(rows);
  }

  private static void assertFileFormatVersion(String expected, List<FragmentMetadata> fragments) {
    for (FragmentMetadata fragment : fragments) {
      for (DataFile file : fragment.getFiles()) {
        assertEquals(
            expected,
            file.getFileMajorVersion() + "." + file.getFileMinorVersion(),
            file.getPath());
      }
    }
  }

  private static Map<Integer, Integer> expectedRows(int start, int rows) {
    Map<Integer, Integer> expected = new HashMap<>();
    for (int i = start; i < start + rows; i++) {
      expected.put(i, 10 * i);
    }
    return expected;
  }

  /** Reads {@code column} keyed by the {@code id} column, as strings. */
  private static Map<Integer, String> readColumnById(
      BufferAllocator allocator, String datasetUri, String column) throws Exception {
    Map<Integer, String> rows = new HashMap<>();
    try (Dataset dataset = Dataset.open(datasetUri, allocator);
        Scanner scanner = dataset.newScan();
        ArrowReader reader = scanner.scanBatches()) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        for (int i = 0; i < root.getRowCount(); i++) {
          Object value = root.getVector(column).getObject(i);
          rows.put(
              (Integer) root.getVector("id").getObject(i), value == null ? null : value.toString());
        }
      }
    }
    return rows;
  }

  /** Reads the table as {@code a -> c}; a column with mismatched field ids reads back as null. */
  private static Map<Integer, Integer> readRows(BufferAllocator allocator, String datasetUri)
      throws Exception {
    Map<Integer, Integer> rows = new HashMap<>();
    try (Dataset dataset = Dataset.open(datasetUri, allocator);
        Scanner scanner = dataset.newScan();
        ArrowReader reader = scanner.scanBatches()) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        for (int i = 0; i < root.getRowCount(); i++) {
          rows.put(
              (Integer) root.getVector("a").getObject(i),
              (Integer) root.getVector("c").getObject(i));
        }
      }
    }
    return rows;
  }

  /**
   * Two overwrite writers both pin the same table version at construction. After the first driver
   * commit advances the version, the second commit must fail (OCC), not apply a stale overwrite.
   */
  @Test
  public void testConcurrentWriteConflict(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(field));
      Dataset.create(allocator, datasetUri, schema, new WriteParams.Builder().build()).close();

      LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

      LanceBatchWrite writerA =
          new LanceBatchWrite(sparkSchema, writeOptions, true, null, null, null, null, false, null);
      LanceBatchWrite writerB =
          new LanceBatchWrite(sparkSchema, writeOptions, true, null, null, null, null, false, null);

      WriterCommitMessage messageA = writeRows(writerA, sparkSchema, 10, 0);
      WriterCommitMessage messageB = writeRows(writerB, sparkSchema, 10, 100);

      writerA.commit(new WriterCommitMessage[] {messageA});
      assertThrows(Exception.class, () -> writerB.commit(new WriterCommitMessage[] {messageB}));
    }
  }

  /**
   * A staged create (e.g. {@code stageCreateAtPath}) builds {@code StagedCommit} with only the
   * catalog's static storage options. Confirms {@link LanceBatchWrite#commit} merges {@code
   * initialStorageOptions} in before the eventual {@link StagedCommit#commit()}.
   */
  @Test
  public void testCommitMergesInitialStorageOptionsIntoStagedCommit(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);

    // StagedCommit starts with no storage options, as for a path-based staged create.
    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema, datasetUri, StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));

    // initialStorageOptions represents namespace-vended credentials, passed to LanceBatchWrite
    // independently of how stagedCommit was constructed.
    Map<String, String> initialStorageOptions = new HashMap<>();
    initialStorageOptions.put("access_key_id", "AKIA-from-namespace");

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema,
            writeOptions,
            false,
            initialStorageOptions,
            null, // namespaceImpl
            null, // namespaceProperties
            null, // tableId
            false, // managedVersioning
            stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals("AKIA-from-namespace", stagedCommit.getStorageOptions().get("access_key_id"));

    // The merged options must not break the subsequent commit.
    stagedCommit.commit();
    try (Dataset dataset = Dataset.open(datasetUri, LanceRuntime.allocator())) {
      assertEquals(0, dataset.countRows());
    }
  }

  /**
   * On key conflict, {@code initialStorageOptions} wins over write-time options; a write-time-only
   * key (no namespace-vended counterpart) must still be included in the merge.
   */
  @Test
  public void testCommitStagedMergePrefersInitialStorageOptionsOnConflict(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);
    Map<String, String> writeTimeOptions = new HashMap<>();
    writeTimeOptions.put("access_key_id", "stale-write-time-key");
    writeTimeOptions.put("region", "us-west-2");
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.from(datasetUri).toBuilder()
            .storageOptions(writeTimeOptions)
            .build();

    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema, datasetUri, StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema,
            writeOptions,
            false,
            Collections.singletonMap("access_key_id", "fresh-namespace-key"),
            null,
            null,
            null,
            false,
            stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    // initialStorageOptions wins on the conflicting key...
    assertEquals("fresh-namespace-key", stagedCommit.getStorageOptions().get("access_key_id"));
    // ...but a write-time-only key (absent from initialStorageOptions) must still come through.
    assertEquals("us-west-2", stagedCommit.getStorageOptions().get("region"));
  }

  /**
   * When a write-time option sets {@code file_format_version} (e.g. via {@code
   * DataFrameWriterV2.option("file_format_version", "2.2")}), the staged commit must use that
   * version even if none was set at stage time.
   */
  @Test
  public void testCommitStagedPropagatesWriteTimeFileFormatVersion(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

    // Stage-time: no file format version set (simulates stageCreate without table property)
    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema, datasetUri, StagedCommitOptions.pathBased(Collections.emptyMap(), false, null));
    assertNull(stagedCommit.getFileFormatVersion());

    // Write-time: user set file_format_version via DataFrameWriterV2.option(...)
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.from(datasetUri).toBuilder().fileFormatVersion("2.1").build();

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema, writeOptions, false, null, null, null, null, false, stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals("2.1", stagedCommit.getFileFormatVersion());
  }

  /**
   * Write-time {@code file_format_version} takes precedence over the stage-time value. This covers
   * the case where the catalog resolved a default at stage time but the user explicitly overrides
   * it at write time.
   */
  @Test
  public void testCommitStagedWriteTimeFileFormatVersionTakesPrecedence(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

    // Stage-time: catalog resolved file format version "2.0"
    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema,
            datasetUri,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, "2.0"));
    assertEquals("2.0", stagedCommit.getFileFormatVersion());

    // Write-time: user explicitly overrides to "2.2"
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.from(datasetUri).toBuilder().fileFormatVersion("2.2").build();

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema, writeOptions, false, null, null, null, null, false, stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals("2.2", stagedCommit.getFileFormatVersion());
  }

  /**
   * When write options have no {@code file_format_version}, the stage-time value must be preserved.
   * Null write-time must not overwrite a stage-time resolution.
   */
  @Test
  public void testCommitStagedPreservesStageTimeVersionWhenWriteTimeIsNull(TestInfo testInfo) {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    Field field = new Field("column1", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    StructType sparkSchema = LanceArrowUtils.fromArrowSchema(schema);

    // Stage-time: version set from table properties
    StagedCommit stagedCommit =
        StagedCommit.forNewTable(
            schema,
            datasetUri,
            StagedCommitOptions.pathBased(Collections.emptyMap(), false, "2.1"));

    // Write-time: no file_format_version option
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);

    LanceBatchWrite batchWrite =
        new LanceBatchWrite(
            sparkSchema, writeOptions, false, null, null, null, null, false, stagedCommit);

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals("2.1", stagedCommit.getFileFormatVersion());
  }

  private static WriterCommitMessage writeRows(
      LanceBatchWrite batchWrite, StructType sparkSchema, int numRows, int startValue)
      throws Exception {
    DataWriterFactory factory = batchWrite.createBatchWriterFactory(() -> 1);
    try (DataWriter<InternalRow> writer = factory.createWriter(0, 0)) {
      for (int i = 0; i < numRows; i++) {
        InternalRow row = new GenericInternalRow(new Object[] {startValue + i});
        writer.write(row);
      }
      return writer.commit();
    }
  }
}
