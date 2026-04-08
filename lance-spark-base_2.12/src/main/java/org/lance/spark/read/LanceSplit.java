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
import org.lance.ipc.ColumnOrdering;
import org.lance.ipc.FilteredRead;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.LanceFragmentScanner;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceSplit implements Serializable {
  private static final long serialVersionUID = 2983749283749283749L;

  private final List<Integer> fragments;

  public LanceSplit(List<Integer> fragments) {
    this.fragments = fragments;
  }

  public List<Integer> getFragments() {
    return fragments;
  }

  /** Result of scan planning containing splits, resolved version, and per-fragment row counts. */
  public static class ScanPlanResult {
    private final List<LanceSplit> splits;
    private final long resolvedVersion;

    /** Per-fragment logical row counts (after deletions). Key is fragment ID. */
    private final Map<Integer, Long> fragmentRowCounts;

    public ScanPlanResult(
        List<LanceSplit> splits, long resolvedVersion, Map<Integer, Long> fragmentRowCounts) {
      this.splits = splits;
      this.resolvedVersion = resolvedVersion;
      this.fragmentRowCounts = fragmentRowCounts;
    }

    public List<LanceSplit> getSplits() {
      return splits;
    }

    public long getResolvedVersion() {
      return resolvedVersion;
    }

    public Map<Integer, Long> getFragmentRowCounts() {
      return fragmentRowCounts;
    }
  }

  /**
   * Generates splits and resolves the dataset version.
   *
   * <p>This method opens the dataset at the specified version (or latest if not specified), gets
   * the fragment IDs and per-fragment row counts, and returns both the splits and the resolved
   * version. The resolved version should be passed to workers to ensure snapshot isolation.
   */
  public static ScanPlanResult planScan(LanceSparkReadOptions readOptions) {
    try (Dataset dataset = openDataset(readOptions)) {
      List<Fragment> fragments = dataset.getFragments();
      List<LanceSplit> splits = new ArrayList<>(fragments.size());
      Map<Integer, Long> fragmentRowCounts = new HashMap<>(fragments.size());
      for (Fragment fragment : fragments) {
        int id = fragment.getId();
        splits.add(new LanceSplit(Collections.singletonList(id)));
        fragmentRowCounts.put(id, fragment.metadata().getNumRows());
      }
      long resolvedVersion = dataset.getVersion().getId();
      return new ScanPlanResult(splits, resolvedVersion, fragmentRowCounts);
    }
  }

  /**
   * @deprecated Use {@link #planScan(LanceSparkReadOptions)} instead to get resolved version.
   */
  @Deprecated
  public static List<LanceSplit> generateLanceSplits(LanceSparkReadOptions readOptions) {
    return planScan(readOptions).getSplits();
  }

  /** Result of distributed scan planning containing per-fragment tasks and resolved version. */
  public static class DistributedScanPlanResult {
    private final List<byte[]> tasks;
    private final long resolvedVersion;
    private final int[] fragmentIds;

    public DistributedScanPlanResult(List<byte[]> tasks, long resolvedVersion, int[] fragmentIds) {
      this.tasks = tasks;
      this.resolvedVersion = resolvedVersion;
      this.fragmentIds = fragmentIds;
    }

    public List<byte[]> getTasks() {
      return tasks;
    }

    public long getResolvedVersion() {
      return resolvedVersion;
    }

    public int[] getFragmentIds() {
      return fragmentIds;
    }
  }

  /**
   * Plans a distributed scan using Lance's FilteredRead API.
   *
   * <p>This method opens the dataset, builds a scanner with the given options (columns, filter,
   * limit, offset, topN, batchSize), and calls {@link FilteredRead#planFilteredRead} to produce a
   * {@link FilteredRead}. The plan is split into per-fragment tasks (serialized as {@code byte[]})
   * that can be sent to workers for execution.
   *
   * @param readOptions dataset read options
   * @param schema the projected schema (column names are extracted from it)
   * @param whereCondition optional SQL filter expression
   * @param limit optional row limit
   * @param offset optional row offset
   * @param topNSortOrders optional topN sort orderings
   * @return a {@link DistributedScanPlanResult} containing tasks, resolved version, and fragment
   *     IDs
   */
  public static DistributedScanPlanResult planDistributedScan(
      LanceSparkReadOptions readOptions,
      StructType schema,
      Optional<String> whereCondition,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders) {
    try (Dataset dataset = openDataset(readOptions)) {
      long resolvedVersion = dataset.getVersion().getId();

      ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
      // Set projected columns from schema (reuse legacy path's filtering logic)
      List<String> columns = LanceFragmentScanner.getColumnNames(schema);
      scanOptionsBuilder.columns(columns);

      // Set filter
      if (whereCondition.isPresent()) {
        scanOptionsBuilder.filter(whereCondition.get());
      }

      // Set batch size
      scanOptionsBuilder.batchSize(readOptions.getBatchSize());

      // Set limit
      if (limit.isPresent()) {
        scanOptionsBuilder.limit(limit.get());
      }

      // Set offset
      if (offset.isPresent()) {
        scanOptionsBuilder.offset(offset.get());
      }

      // Set topN sort orderings
      if (topNSortOrders.isPresent()) {
        scanOptionsBuilder.setColumnOrderings(topNSortOrders.get());
      }

      ScanOptions scanOptions = scanOptionsBuilder.build();
      try (LanceScanner scanner =
          LanceScanner.create(dataset, scanOptions, LanceRuntime.allocator())) {
        FilteredRead plan = FilteredRead.planFilteredRead(scanner);
        List<byte[]> tasks = plan.getTasks();
        int[] fragmentIds = plan.getFragmentIds();
        return new DistributedScanPlanResult(tasks, resolvedVersion, fragmentIds);
      } catch (Exception e) {
        throw new RuntimeException("Failed to plan distributed scan", e);
      }
    }
  }

  private static Dataset openDataset(LanceSparkReadOptions readOptions) {
    if (readOptions.hasNamespace()) {
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .namespaceClient(readOptions.getNamespace())
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
