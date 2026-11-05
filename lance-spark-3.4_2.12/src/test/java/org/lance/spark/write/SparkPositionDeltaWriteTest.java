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

import org.lance.BlobFile;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.spark.LancePositionDeltaOperation;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.BlobUtils;
import org.lance.spark.utils.Utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SparkPositionDeltaWriteTest {
  @TempDir static Path tempDir;

  /** The delta commit must also accept legacy blob fields written directly as 2.2 fragments. */
  @Test
  public void commitPreservesBlobV1FieldsWrittenAtTwoTwo(TestInfo testInfo) throws Exception {
    String uri =
        TestUtils.getDatasetUri(tempDir.toString(), testInfo.getTestMethod().get().getName());
    Field data =
        new Field(
            "data",
            new FieldType(
                true,
                ArrowType.LargeBinary.INSTANCE,
                null,
                Collections.singletonMap(
                    BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)),
            null);
    Schema schema = new Schema(Arrays.asList(ARROW_SCHEMA.getFields().get(0), data));
    byte[][] expected = {new byte[] {1, 2, 3}, new byte[] {4, 5, 6, 7}};
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.write()
          .allocator(allocator)
          .uri(uri)
          .schema(schema)
          .dataStorageVersion("2.1")
          .execute()
          .close();
      for (int commit = 0; commit < expected.length; commit++) {
        List<FragmentMetadata> fragments;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
          ((IntVector) root.getVector("id")).setSafe(0, commit + 1);
          ((LargeVarBinaryVector) root.getVector("data")).setSafe(0, expected[commit]);
          root.setRowCount(1);
          fragments =
              Fragment.write()
                  .datasetUri(uri)
                  .allocator(allocator)
                  .data(root)
                  .mode(WriteParams.WriteMode.APPEND)
                  .dataStorageVersion("2.2")
                  .execute();
        }
        DeltaBatchWrite batch =
            new SparkPositionDeltaWrite(
                    LanceArrowUtils.fromArrowSchema(schema),
                    LanceSparkWriteOptions.from(uri).toBuilder().fileFormatVersion("2.2").build(),
                    null,
                    null,
                    null,
                    false,
                    null)
                .toBatch();
        batch.commit(
            new WriterCommitMessage[] {
              new SparkPositionDeltaWrite.DeltaWriteTaskCommit(fragments, Collections.emptyMap())
            });
        try (Dataset dataset =
            Utils.openDatasetBuilder(LanceSparkReadOptions.builder().datasetUri(uri).build())
                .build()) {
          assertEquals(commit + 2, dataset.version());
          assertEquals(commit + 1, dataset.countRows());
          assertEquals(commit + 1, dataset.getFragments().size());
          List<Long> rowAddresses = new ArrayList<>();
          for (Fragment fragment : dataset.getFragments()) {
            rowAddresses.add(((long) fragment.getId()) << 32);
            fragment
                .metadata()
                .getFiles()
                .forEach(
                    file -> {
                      assertEquals(2, file.getFileMajorVersion());
                      assertEquals(2, file.getFileMinorVersion());
                    });
          }
          List<BlobFile> blobs = dataset.takeBlobs(rowAddresses, "data");
          try {
            assertEquals(commit + 1, blobs.size());
            for (int row = 0; row <= commit; row++) {
              assertArrayEquals(expected[row], blobs.get(row).read());
            }
          } finally {
            for (BlobFile blob : blobs) {
              blob.close();
            }
          }
        }
      }
    }
  }

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  @Test
  public void positionDeltaWriteDoesNotUseCommitCoordinator(TestInfo testInfo) throws Exception {
    String datasetName = testInfo.getTestMethod().get().getName();
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), datasetName);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(allocator, datasetUri, ARROW_SCHEMA, new WriteParams.Builder().build())
          .close();

      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);
      SparkPositionDeltaWrite write =
          new SparkPositionDeltaWrite(
              sparkSchema, LanceSparkWriteOptions.from(datasetUri), null, null, null, false, null);

      DeltaBatchWrite batchWrite = write.toBatch();
      Method useCommitCoordinator = batchWrite.getClass().getMethod("useCommitCoordinator");
      assertEquals(
          batchWrite.getClass(),
          useCommitCoordinator.getDeclaringClass(),
          "Position delta writes must explicitly override Spark's commit coordinator default");
      assertFalse(batchWrite.useCommitCoordinator());
    }
  }

  @Test
  public void newWriteBuilderPropagatesCacheBackendConfiguration() throws Exception {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri("file:///unused")
            .catalogName("configured-catalog")
            .indexCacheBackend("moka://?capacity=1048576")
            .metadataCacheBackend("moka://?capacity=524288")
            .build();
    StructType schema = new StructType();
    LancePositionDeltaOperation operation =
        new LancePositionDeltaOperation(
            RowLevelOperation.Command.DELETE,
            schema,
            readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            false,
            null,
            Collections.emptyMap());
    LogicalWriteInfo info =
        new LogicalWriteInfo() {
          @Override
          public String queryId() {
            return "probe";
          }

          @Override
          public StructType schema() {
            return schema;
          }

          @Override
          public CaseInsensitiveStringMap options() {
            return new CaseInsensitiveStringMap(Collections.emptyMap());
          }
        };

    DeltaWriteBuilder builder = operation.newWriteBuilder(info);
    LanceSparkWriteOptions writeOptions = extractWriteOptions(builder);
    assertEquals("configured-catalog", writeOptions.getCatalogName());
    assertEquals("moka://?capacity=1048576", writeOptions.getIndexCacheBackend());
    assertEquals("moka://?capacity=524288", writeOptions.getMetadataCacheBackend());
  }

  private static LanceSparkWriteOptions extractWriteOptions(DeltaWriteBuilder builder)
      throws Exception {
    // Fully qualified to avoid clashing with the Arrow Field import used above.
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("writeOptions");
    field.setAccessible(true);
    return (LanceSparkWriteOptions) field.get(builder);
  }
}
