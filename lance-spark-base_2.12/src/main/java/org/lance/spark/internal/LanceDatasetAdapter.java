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
import org.lance.ReadOptions;
import org.lance.spark.LanceSparkReadOptions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapter for interacting with Lance datasets from Spark.
 *
 * <p>This class provides utility methods for accessing Lance dataset metadata, primarily used for
 * fragment-aware join optimization.
 */
public class LanceDatasetAdapter {

  /** Shared Arrow buffer allocator for Lance operations. */
  public static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private LanceDatasetAdapter() {
    // Utility class, prevent instantiation
  }

  /**
   * Opens a Lance dataset. Caller is responsible for closing the dataset.
   *
   * @param options the Lance read options
   * @return the opened Dataset
   */
  public static Dataset openDataset(LanceSparkReadOptions options) {
    String uri = options.getDatasetUri();
    ReadOptions readOptions = options.toReadOptions();
    return Dataset.open(allocator, uri, readOptions);
  }

  /**
   * Get fragment IDs from a Lance dataset.
   *
   * @param options the Lance read options
   * @return list of fragment IDs
   */
  public static List<Integer> getFragmentIds(LanceSparkReadOptions options) {
    String uri = options.getDatasetUri();
    ReadOptions readOptions = options.toReadOptions();
    try (Dataset dataset = Dataset.open(allocator, uri, readOptions)) {
      return getFragmentIds(dataset);
    }
  }

  /**
   * Get fragment IDs from an already-opened dataset.
   *
   * @param dataset the opened Dataset
   * @return list of fragment IDs
   */
  public static List<Integer> getFragmentIds(Dataset dataset) {
    return dataset.getFragments().stream().map(Fragment::getId).collect(Collectors.toList());
  }
}
