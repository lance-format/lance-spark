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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** One reader task containing either full fragments or bounded physical slices. */
public class LanceSplit implements Serializable {
  private static final long serialVersionUID = 2983749283749283750L;

  private final List<FragmentSplit> fragmentSplits;

  public LanceSplit(List<FragmentSplit> fragmentSplits) {
    this.fragmentSplits = Collections.unmodifiableList(new ArrayList<>(fragmentSplits));
    this.fragmentSplits.forEach(Objects::requireNonNull);
    // The current Java scan adapter does not support mixing full fragments and slices in a task.
    if (hasFragmentSlices()
        && this.fragmentSplits.stream().anyMatch(FragmentSplit::isFullFragment)) {
      throw new IllegalArgumentException("A task cannot mix full fragments and physical slices");
    }
  }

  public List<FragmentSplit> getFragmentSplits() {
    return fragmentSplits;
  }

  /** Each fragment is scanned once, with all its ranges, in first-occurrence order. */
  public List<Integer> getFragments() {
    return fragmentSplits.stream()
        .map(FragmentSplit::getFragmentId)
        .distinct()
        .collect(Collectors.toList());
  }

  public boolean hasFragmentSlices() {
    return fragmentSplits.stream().anyMatch(split -> !split.isFullFragment());
  }
}
