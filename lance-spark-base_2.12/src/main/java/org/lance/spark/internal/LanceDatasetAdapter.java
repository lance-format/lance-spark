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
package org.lance.spark.internal;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

public class LanceDatasetAdapter {
  public static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static Optional<StructType> getSchema(LanceSparkReadOptions opt) {
    String uri = opt.getDatasetUri();
    try (Dataset dataset = Dataset.open(allocator, uri, opt.toReadOptions())) {
      return Optional.of(LanceArrowUtils.fromArrowSchema(dataset.getSchema()));
    } catch (IllegalArgumentException e) {
      // dataset not found
      return Optional.empty();
    }
  }

  public static Optional<StructType> getSchema(String datasetUri) {
    try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
      return Optional.of(LanceArrowUtils.fromArrowSchema(dataset.getSchema()));
    } catch (IllegalArgumentException e) {
      // dataset not found
      return Optional.empty();
    }
  }

  public static List<FragmentMetadata> getFragments(LanceSparkReadOptions options) {
    String uri = options.getDatasetUri();
    try (Dataset dataset = Dataset.open(allocator, uri, options.toReadOptions())) {
      return dataset.getFragments().stream().map(Fragment::metadata).collect(Collectors.toList());
    }
  }

  public static void appendFragments(
      LanceSparkReadOptions options, List<FragmentMetadata> fragments) {
    FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
    String uri = options.getDatasetUri();
    try (Dataset datasetRead = Dataset.open(allocator, uri, options.toReadOptions())) {
      Dataset.commit(
              allocator,
              options.getDatasetUri(),
              appendOp,
              java.util.Optional.of(datasetRead.version()),
              options.getStorageOptions())
          .close();
    }
  }

  public static void overwriteFragments(
      LanceSparkReadOptions options, List<FragmentMetadata> fragments, StructType sparkSchema) {
    Schema schema = LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false);
    FragmentOperation.Overwrite overwrite = new FragmentOperation.Overwrite(fragments, schema);
    String uri = options.getDatasetUri();
    try (Dataset datasetRead = Dataset.open(allocator, uri, options.toReadOptions())) {
      Dataset.commit(
              allocator,
              options.getDatasetUri(),
              overwrite,
              java.util.Optional.of(datasetRead.version()),
              options.getStorageOptions())
          .close();
    }
  }

  public static void createDataset(String datasetUri, StructType sparkSchema, WriteParams params) {
    Dataset.create(
            allocator,
            datasetUri,
            LanceArrowUtils.toArrowSchema(sparkSchema, ZoneId.systemDefault().getId(), true, false),
            params)
        .close();
  }

  public static void cleanupDataset(LanceSparkReadOptions options) {
    String uri = options.getDatasetUri();
    try (Dataset dataset = Dataset.open(allocator, uri, options.toReadOptions())) {
      dataset.delete("true");
    }
  }
}
