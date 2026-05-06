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
    try (Dataset dataset = Utils.openDatasetBuilder(readOptions).build()) {
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
}
