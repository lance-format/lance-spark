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
import org.lance.ipc.ColumnOrdering;
import org.lance.ipc.FilteredReadPlan;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.ipc.Splits;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.LanceFragmentScanner;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LanceSplit implements Serializable {
  private static final long serialVersionUID = 2983749283749283749L;

  private final List<Integer> fragments;
  private final FilteredReadPlan filteredReadPlan;

  public LanceSplit(List<Integer> fragments) {
    this.fragments = fragments;
    this.filteredReadPlan = null;
  }

  public LanceSplit(FilteredReadPlan filteredReadPlan) {
    this.fragments = new ArrayList<>(filteredReadPlan.getFragmentRanges().keySet());
    this.filteredReadPlan = filteredReadPlan;
  }

  public List<Integer> getFragments() {
    return fragments;
  }

  public Optional<FilteredReadPlan> getFilteredReadPlan() {
    return Optional.ofNullable(filteredReadPlan);
  }

  public static List<LanceSplit> generateLanceSplits(
      LanceSparkReadOptions readOptions,
      StructType schema,
      Optional<String> whereCondition,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders) {
    try (Dataset dataset = openDataset(readOptions)) {
      ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
      scanOptionsBuilder.columns(LanceFragmentScanner.getColumnNames(schema));
      if (whereCondition.isPresent()) {
        scanOptionsBuilder.filter(whereCondition.get());
      }
      scanOptionsBuilder.batchSize(readOptions.getBatchSize());
      scanOptionsBuilder.withRowId(LanceFragmentScanner.getWithRowId(schema));
      scanOptionsBuilder.withRowAddress(LanceFragmentScanner.getWithRowAddress(schema));
      if (readOptions.getNearest() != null) {
        scanOptionsBuilder.nearest(readOptions.getNearest());
      }
      if (limit.isPresent()) {
        scanOptionsBuilder.limit(limit.get());
      }
      if (offset.isPresent()) {
        scanOptionsBuilder.offset(offset.get());
      }
      if (topNSortOrders.isPresent()) {
        scanOptionsBuilder.setColumnOrderings(topNSortOrders.get());
      }

      try (LanceScanner scanner = dataset.newScan(scanOptionsBuilder.build())) {
        Splits splits = scanner.planSplits(null);
        if (splits.getFilteredReadPlans().isPresent()) {
          return splits.getFilteredReadPlans().get().stream()
              .map(LanceSplit::new)
              .collect(Collectors.toList());
        } else if (splits.getFragments().isPresent()) {
          return splits.getFragments().get().stream()
              .map(id -> new LanceSplit(Collections.singletonList(id)))
              .collect(Collectors.toList());
        } else {
          return Collections.emptyList();
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to plan splits", e);
      }
    }
  }

  private static Dataset openDataset(LanceSparkReadOptions readOptions) {
    if (readOptions.hasNamespace()) {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .namespace(readOptions.getNamespace())
          .tableId(readOptions.getTableId())
          .readOptions(readOptions.toReadOptions())
          .build();
    } else {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .uri(readOptions.getDatasetUri())
          .readOptions(readOptions.toReadOptions())
          .build();
    }
  }
}
