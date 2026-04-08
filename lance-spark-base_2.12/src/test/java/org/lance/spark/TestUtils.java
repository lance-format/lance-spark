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
package org.lance.spark;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.read.LanceSplit;
import org.lance.spark.utils.Optional;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class TestUtils {
  public static class TestTable1Config {
    public static final String dbPath;
    public static final String datasetName = "test_dataset1";
    public static final String datasetUri;
    public static final List<List<Long>> expectedValues =
        Arrays.asList(
            Arrays.asList(0L, 0L, 0L, 0L),
            Arrays.asList(1L, 2L, 3L, -1L),
            Arrays.asList(2L, 4L, 6L, -2L),
            Arrays.asList(3L, 6L, 9L, -3L));
    public static final List<List<Long>> expectedValuesWithRowId =
        Arrays.asList(
            Arrays.asList(0L, 0L, 0L, 0L, 0L),
            Arrays.asList(1L, 2L, 3L, -1L, 1L),
            Arrays.asList(2L, 4L, 6L, -2L, (1L << 32) + 0L),
            Arrays.asList(3L, 6L, 9L, -3L, (1L << 32) + 1L));
    public static final List<List<Long>> expectedValuesWithRowAddress =
        Arrays.asList(
            Arrays.asList(0L, 0L, 0L, 0L, 0L),
            Arrays.asList(1L, 2L, 3L, -1L, 1L),
            Arrays.asList(2L, 4L, 6L, -2L, (1L << 32) + 0L),
            Arrays.asList(3L, 6L, 9L, -3L, (1L << 32) + 1L));
    public static final List<List<Long>> expectedValuesWithCdfVersionColumns =
        Arrays.asList(
            Arrays.asList(0L, 0L, 0L, 0L, 1L, 1L),
            Arrays.asList(1L, 2L, 3L, -1L, 1L, 1L),
            Arrays.asList(2L, 4L, 6L, -2L, 1L, 1L),
            Arrays.asList(3L, 6L, 9L, -3L, 1L, 1L));
    public static final LanceSparkReadOptions readOptions;

    public static final StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("x", DataTypes.LongType, true),
              DataTypes.createStructField("y", DataTypes.LongType, true),
              DataTypes.createStructField("b", DataTypes.LongType, true),
              DataTypes.createStructField("c", DataTypes.LongType, true),
            });

    public static final LanceInputPartition inputPartition;

    static {
      URL resource = TestUtils.class.getResource("/example_db");
      if (resource != null) {
        dbPath = resource.toString();
      } else {
        throw new IllegalArgumentException("example_db not found in resources directory");
      }
      datasetUri = getDatasetUri(dbPath, datasetName);
      readOptions = LanceSparkReadOptions.from(datasetUri);
      inputPartition =
          new LanceInputPartition(
              schema,
              0 /* partitionId */,
              new LanceSplit(Arrays.asList(0, 1)),
              readOptions,
              Optional.empty() /* whereCondition */,
              Optional.empty() /* limit */,
              Optional.empty() /* offset */,
              Optional.empty() /* topNSortOrders */,
              Optional.empty() /* pushedAggregation */,
              "test" /* scanId */,
              null /* initialStorageOptions */,
              null /* namespaceImpl */,
              null /* namespaceProperties */);
    }
  }

  public static String getDatasetUri(String dbPath, String datasetUri) {
    StringBuilder sb = new StringBuilder().append(dbPath);
    if (!dbPath.endsWith("/")) {
      sb.append("/");
    }
    if (dbPath.equals(TestTable1Config.dbPath) && datasetUri.startsWith("test_dataset")) {
      return sb.append(datasetUri).append(".lance").toString();
    }
    return sb.append(datasetUri).toString();
  }

  /**
   * Creates a v2-format Lance dataset with the same schema and data as {@link TestTable1Config}.
   *
   * <p>The dataset is written in two batches (each as a separate fragment) using Lance's {@code
   * dataStorageVersion("stable")} to produce v2-format files, which is required for FilteredRead
   * operations.
   *
   * @param tempDir the temporary directory to write the dataset into
   * @return the dataset URI for the created v2 dataset
   */
  public static String createTestTable1V2Dataset(Path tempDir) {
    String datasetUri = tempDir.resolve("test_dataset1_v2.lance").toString();
    BufferAllocator allocator = LanceRuntime.allocator();

    Schema arrowSchema =
        new Schema(
            Arrays.asList(
                new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("y", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("b", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("c", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    // Fragment 0: rows (0,0,0,0) and (1,2,3,-1)
    long[][] fragment0 = {{0, 0, 0, 0}, {1, 2, 3, -1}};
    writeFragment(allocator, arrowSchema, datasetUri, fragment0, WriteParams.WriteMode.CREATE);

    // Fragment 1: rows (2,4,6,-2) and (3,6,9,-3)
    long[][] fragment1 = {{2, 4, 6, -2}, {3, 6, 9, -3}};
    writeFragment(allocator, arrowSchema, datasetUri, fragment1, WriteParams.WriteMode.APPEND);

    return datasetUri;
  }

  private static void writeFragment(
      BufferAllocator allocator,
      Schema arrowSchema,
      String datasetUri,
      long[][] rows,
      WriteParams.WriteMode mode) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      root.allocateNew();
      BigIntVector xVec = (BigIntVector) root.getVector("x");
      BigIntVector yVec = (BigIntVector) root.getVector("y");
      BigIntVector bVec = (BigIntVector) root.getVector("b");
      BigIntVector cVec = (BigIntVector) root.getVector("c");

      for (int i = 0; i < rows.length; i++) {
        xVec.setSafe(i, rows[i][0]);
        yVec.setSafe(i, rows[i][1]);
        bVec.setSafe(i, rows[i][2]);
        cVec.setSafe(i, rows[i][3]);
      }
      root.setRowCount(rows.length);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write Arrow stream", e);
      }

      try (ArrowStreamReader reader =
              new ArrowStreamReader(new ByteArrayInputStream(baos.toByteArray()), allocator);
          ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, arrowStream);
        Dataset.write().stream(arrowStream)
            .uri(datasetUri)
            .mode(mode)
            .maxRowsPerFile(rows.length)
            .dataStorageVersion("stable")
            .execute()
            .close();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write Lance dataset fragment", e);
      }
    }
  }
}
