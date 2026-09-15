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
import org.lance.ipc.FragmentSlice;
import org.lance.spark.LanceRef;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceSplit implements Serializable {
  private static final long serialVersionUID = 2983749283749283749L;

  private final List<Integer> fragments;
  private final List<FragmentSlice> fragmentSlices;

  public LanceSplit(List<Integer> fragments) {
    this(fragments, Collections.emptyList());
  }

  public LanceSplit(List<Integer> fragments, List<FragmentSlice> fragmentSlices) {
    this.fragments = Collections.unmodifiableList(new ArrayList<>(fragments));
    this.fragmentSlices = Collections.unmodifiableList(new ArrayList<>(fragmentSlices));
  }

  public List<Integer> getFragments() {
    return fragments;
  }

  /**
   * Physical row slices selected for this split. An empty list means scan the full fragment list.
   */
  public List<FragmentSlice> getFragmentSlices() {
    return fragmentSlices == null ? Collections.emptyList() : fragmentSlices;
  }

  /** Result of scan planning containing splits, resolved version, and per-fragment row counts. */
  public static class ScanPlanResult {
    private final List<LanceSplit> splits;
    private final LanceRef ref;

    /** Per-fragment logical row counts (after deletions). Key is fragment ID. */
    private final Map<Integer, Long> fragmentRowCounts;

    /** Per-fragment physical row counts (before deletions). Key is fragment ID. */
    private final Map<Integer, Long> physicalFragmentRowCounts;

    public ScanPlanResult(
        List<LanceSplit> splits, LanceRef ref, Map<Integer, Long> fragmentRowCounts) {
      this(splits, ref, fragmentRowCounts, fragmentRowCounts);
    }

    public ScanPlanResult(
        List<LanceSplit> splits,
        LanceRef ref,
        Map<Integer, Long> fragmentRowCounts,
        Map<Integer, Long> physicalFragmentRowCounts) {
      this.splits = splits;
      this.ref = ref;
      this.fragmentRowCounts = fragmentRowCounts;
      this.physicalFragmentRowCounts = physicalFragmentRowCounts;
    }

    public List<LanceSplit> getSplits() {
      return splits;
    }

    public LanceRef getRef() {
      return ref;
    }

    public Map<Integer, Long> getFragmentRowCounts() {
      return fragmentRowCounts;
    }

    public Map<Integer, Long> getPhysicalFragmentRowCounts() {
      return physicalFragmentRowCounts;
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
    try (Dataset dataset = Utils.openDatasetBuilder(readOptions).build()) {
      return planScan(dataset, readOptions);
    }
  }

  /**
   * Generates splits and resolves the dataset version using an already-opened dataset.
   *
   * <p>Prefer this overload over {@link #planScan(LanceSparkReadOptions)} when the caller already
   * holds an open {@link Dataset} (e.g. in scan planning), to avoid re-opening the dataset and
   * paying the manifest IO cost twice.
   *
   * <p>The caller retains ownership of the dataset; this method does not close it.
   */
  public static ScanPlanResult planScan(Dataset dataset, LanceSparkReadOptions readOptions) {
    List<Fragment> fragments = dataset.getFragments();
    List<LanceSplit> splits = new ArrayList<>(fragments.size());
    Map<Integer, Long> fragmentRowCounts = new HashMap<>(fragments.size());
    Map<Integer, Long> physicalFragmentRowCounts = new HashMap<>(fragments.size());
    for (Fragment fragment : fragments) {
      int id = fragment.getId();
      splits.add(new LanceSplit(Collections.singletonList(id)));
      fragmentRowCounts.put(id, fragment.metadata().getNumRows());
      physicalFragmentRowCounts.put(id, fragment.metadata().getPhysicalRows());
    }

    LanceRef ref = Utils.pinOpenedRef(dataset, readOptions.getRef());
    return new ScanPlanResult(splits, ref, fragmentRowCounts, physicalFragmentRowCounts);
  }

  /**
   * @deprecated Use {@link #planScan(LanceSparkReadOptions)} instead to get resolved version.
   */
  @Deprecated
  public static List<LanceSplit> generateLanceSplits(LanceSparkReadOptions readOptions) {
    return planScan(readOptions).getSplits();
  }
}
