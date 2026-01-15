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

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.internal.LanceFragmentScanner;
import org.lance.spark.utils.Optional;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceDatasetReadTest {
  @Test
  public void testSchema() {
    StructType expectedSchema = TestUtils.TestTable1Config.schema;
    Optional<StructType> schema = getSchema(TestUtils.TestTable1Config.readOptions);
    assertTrue(schema.isPresent());
    assertEquals(expectedSchema, schema.get());
  }

  @Test
  public void testFragmentIds() {
    List<Integer> fragments = getFragmentIds(TestUtils.TestTable1Config.readOptions);
    assertEquals(2, fragments.size());
    assertEquals(0, fragments.get(0));
    assertEquals(1, fragments.get(1));
  }

  @Test
  public void getFragmentScanner() throws IOException {
    List<List<Object>> expectedValues =
        Arrays.asList(Arrays.asList(0L, 0L, 0L, 0L), Arrays.asList(1L, 2L, 3L, -1L));
    validateFragment(expectedValues, 0, TestUtils.TestTable1Config.schema);
    List<List<Object>> expectedValues1 =
        Arrays.asList(Arrays.asList(2L, 4L, 6L, -2L), Arrays.asList(3L, 6L, 9L, -3L));
    validateFragment(expectedValues1, 1, TestUtils.TestTable1Config.schema);
    List<List<Object>> expectedValuesColumnsyb =
        Arrays.asList(Arrays.asList(4L, 6L), Arrays.asList(6L, 9L));
    validateFragment(
        expectedValuesColumnsyb,
        1,
        new StructType(
            new StructField[] {
              DataTypes.createStructField("y", DataTypes.LongType, true),
              DataTypes.createStructField("b", DataTypes.LongType, true)
            }));
    List<List<Object>> expectedValuesColumnsbc =
        Arrays.asList(Arrays.asList(0L, 0L), Arrays.asList(3L, -1L));
    validateFragment(
        expectedValuesColumnsbc,
        0,
        new StructType(
            new StructField[] {
              DataTypes.createStructField("b", DataTypes.LongType, true),
              DataTypes.createStructField("c", DataTypes.LongType, true)
            }));
  }

  private Optional<StructType> getSchema(LanceSparkReadOptions readOptions) {
    try (Dataset dataset =
        Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(readOptions.getDatasetUri())
            .readOptions(readOptions.toReadOptions())
            .build()) {
      return Optional.of(LanceArrowUtils.fromArrowSchema(dataset.getSchema()));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  private List<Integer> getFragmentIds(LanceSparkReadOptions readOptions) {
    try (Dataset dataset =
        Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(readOptions.getDatasetUri())
            .readOptions(readOptions.toReadOptions())
            .build()) {
      return dataset.getFragments().stream().map(Fragment::getId).collect(Collectors.toList());
    }
  }

  public void validateFragment(List<List<Object>> expectedValues, int fragment, StructType schema)
      throws IOException {
    try (LanceFragmentScanner scanner =
        LanceFragmentScanner.create(
            fragment,
            new LanceInputPartition(
                schema,
                0 /* partitionId */,
                new LanceSplit(Arrays.asList(fragment)),
                TestUtils.TestTable1Config.readOptions,
                Optional.empty() /* whereCondition */,
                Optional.empty() /* limit */,
                Optional.empty() /* offset */,
                Optional.empty() /* topNSortOrders */,
                Optional.empty() /* pushedAggregation */,
                "validateFragment" /* scanId */,
                null /* initialStorageOptions */,
                null /* namespaceImpl */,
                null /* namespaceProperties */))) {
      try (ArrowReader reader = scanner.getArrowReader()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertNotNull(root);

        while (reader.loadNextBatch()) {
          for (int i = 0; i < root.getRowCount(); i++) {
            for (int j = 0; j < root.getFieldVectors().size(); j++) {
              assertEquals(
                  expectedValues.get(i).get(j), root.getFieldVectors().get(j).getObject(i));
            }
          }
        }
      }
    }
  }

  // TODO test_dataset4 [UNSUPPORTED_ARROWTYPE] Unsupported arrow type FixedSizeList(128).
}
