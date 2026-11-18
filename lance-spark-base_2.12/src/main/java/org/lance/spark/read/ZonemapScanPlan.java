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

import org.lance.ipc.FragmentSlice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Builder-local zonemap result; prepared LanceSplit selections are passed to the scan. */
final class ZonemapScanPlan {

  private final Set<Integer> fullScanFragments;
  private final Map<Integer, List<FragmentSlice>> slicesByFragment;

  ZonemapScanPlan(
      Set<Integer> fullScanFragments, Map<Integer, List<FragmentSlice>> slicesByFragment) {
    this.fullScanFragments = Collections.unmodifiableSet(new HashSet<>(fullScanFragments));

    Map<Integer, List<FragmentSlice>> slicesCopy = new HashMap<>();
    for (Map.Entry<Integer, List<FragmentSlice>> entry : slicesByFragment.entrySet()) {
      slicesCopy.put(
          entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
    }
    this.slicesByFragment = Collections.unmodifiableMap(slicesCopy);
  }

  static ZonemapScanPlan fullFragments(Set<Integer> fragmentIds) {
    return new ZonemapScanPlan(fragmentIds, Collections.emptyMap());
  }

  Set<Integer> getSurvivingFragmentIds() {
    Set<Integer> result = new HashSet<>(fullScanFragments);
    result.addAll(slicesByFragment.keySet());
    return Collections.unmodifiableSet(result);
  }

  boolean scansFullFragment(int fragmentId) {
    return fullScanFragments.contains(fragmentId);
  }

  List<FragmentSlice> getFragmentSlices(int fragmentId) {
    return slicesByFragment.getOrDefault(fragmentId, Collections.emptyList());
  }
}
