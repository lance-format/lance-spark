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

import java.io.Serializable;
import java.util.Optional;

/** A full fragment or a physical row range within the pinned dataset snapshot. */
public final class FragmentSplit implements Serializable {
  private static final long serialVersionUID = 3L;

  private final int fragmentId;
  private final FragmentSlice slice;

  private FragmentSplit(int fragmentId, FragmentSlice slice) {
    this.fragmentId = fragmentId;
    this.slice = slice;
  }

  public static FragmentSplit full(int fragmentId) {
    return new FragmentSplit(fragmentId, null);
  }

  /** Deletions do not shift these physical offsets. Empty selections must not create a task. */
  public static FragmentSplit slice(int fragmentId, long rowOffset, long rowCount) {
    if (rowCount == 0) {
      throw new IllegalArgumentException("A fragment slice must contain at least one row");
    }
    return new FragmentSplit(fragmentId, new FragmentSlice(fragmentId, rowOffset, rowCount));
  }

  public int getFragmentId() {
    return fragmentId;
  }

  Optional<FragmentSlice> getSlice() {
    return Optional.ofNullable(slice);
  }

  public boolean isFullFragment() {
    return slice == null;
  }
}
