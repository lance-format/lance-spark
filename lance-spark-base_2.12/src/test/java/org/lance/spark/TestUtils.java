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

import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.read.LanceSplit;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.net.URL;
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
}
