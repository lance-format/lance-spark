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
import org.lance.WriteParams;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.distributions.ClusteredDistribution;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SparkWriteTest {
  @TempDir Path tempDir;

  private static final Schema ARROW_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

  private static final StructType SPARK_SCHEMA = LanceArrowUtils.fromArrowSchema(ARROW_SCHEMA);

  /** Creates a real Lance dataset on disk so that toBatch() can open it. */
  private String createDataset(String name) {
    String datasetUri = TestUtils.getDatasetUri(tempDir.toString(), name);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Dataset.create(allocator, datasetUri, ARROW_SCHEMA, new WriteParams.Builder().build())
          .close();
    }
    return datasetUri;
  }

  private SparkWrite.SparkWriteBuilder createBuilder(String datasetUri) {
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);
    return new SparkWrite.SparkWriteBuilder(
        SPARK_SCHEMA,
        writeOptions,
        Collections.emptyMap(),
        null,
        Collections.emptyMap(),
        Arrays.asList("default", "test_table"),
        false,
        Collections.emptyMap());
  }

  @Test
  public void testBuildReturnsSparkWrite(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    Write write = createBuilder(datasetUri).build();
    assertInstanceOf(SparkWrite.class, write);
  }

  @Test
  public void testToBatchReturnsLanceBatchWrite(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    Write write = createBuilder(datasetUri).build();
    assertInstanceOf(LanceBatchWrite.class, write.toBatch());
  }

  @Test
  public void testToStreamingThrowsUnsupportedOperationException(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    Write write = createBuilder(datasetUri).build();
    assertThrows(UnsupportedOperationException.class, write::toStreaming);
  }

  @Test
  public void testTruncateThenToBatch(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(datasetUri)
            .writeMode(WriteParams.WriteMode.APPEND)
            .build();
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            SPARK_SCHEMA,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false,
            Collections.emptyMap());
    assertSame(builder, builder.truncate());
    BatchWrite batchWrite = builder.build().toBatch();
    assertInstanceOf(LanceBatchWrite.class, batchWrite);
  }

  @Test
  public void testTruncatePreservesUseLargeVarTypes(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    LanceSparkWriteOptions writeOptions =
        LanceSparkWriteOptions.builder()
            .datasetUri(datasetUri)
            .writeMode(WriteParams.WriteMode.APPEND)
            .useLargeVarTypes(true)
            .build();
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            SPARK_SCHEMA,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            null,
            false,
            Collections.emptyMap());
    builder.truncate();
    SparkWrite sparkWrite = (SparkWrite) builder.build();
    assertTrue(
        sparkWrite.getWriteOptions().isUseLargeVarTypes(),
        "useLargeVarTypes should be preserved after truncate()");
  }

  // --- requiredDistribution / requiredOrdering tests ---

  private SparkWrite createWriteWithTableProperties(
      String datasetUri, Map<String, String> tableProps) {
    LanceSparkWriteOptions writeOptions = LanceSparkWriteOptions.from(datasetUri);
    SparkWrite.SparkWriteBuilder builder =
        new SparkWrite.SparkWriteBuilder(
            SPARK_SCHEMA,
            writeOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Arrays.asList("default", "test_table"),
            false,
            tableProps);
    return (SparkWrite) builder.build();
  }

  @Test
  public void testRequiredDistributionWithPartitionColumn(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    Map<String, String> props = new HashMap<>();
    props.put("lance.partition.columns", "name");
    SparkWrite write = createWriteWithTableProperties(datasetUri, props);

    Distribution dist = write.requiredDistribution();
    assertInstanceOf(ClusteredDistribution.class, dist);
    ClusteredDistribution clustered = (ClusteredDistribution) dist;
    assertEquals(1, clustered.clustering().length);
  }

  @Test
  public void testRequiredDistributionWithoutPartitionColumn(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    SparkWrite write = createWriteWithTableProperties(datasetUri, Collections.emptyMap());

    Distribution dist = write.requiredDistribution();
    // Unspecified distribution — no clustering required
    assertFalse(dist instanceof ClusteredDistribution);
  }

  @Test
  public void testRequiredOrderingWithPartitionColumn(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    Map<String, String> props = new HashMap<>();
    props.put("lance.partition.columns", "name");
    SparkWrite write = createWriteWithTableProperties(datasetUri, props);

    SortOrder[] ordering = write.requiredOrdering();
    assertEquals(1, ordering.length);
  }

  @Test
  public void testRequiredOrderingWithoutPartitionColumn(TestInfo testInfo) {
    String datasetUri = createDataset(testInfo.getTestMethod().get().getName());
    SparkWrite write = createWriteWithTableProperties(datasetUri, Collections.emptyMap());

    SortOrder[] ordering = write.requiredOrdering();
    assertEquals(0, ordering.length);
  }
}
