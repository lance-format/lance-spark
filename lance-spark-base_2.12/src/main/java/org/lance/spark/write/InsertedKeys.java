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
package org.lance.spark.write;

import org.lance.operation.KeyExistenceFilter;

import java.io.Serializable;
import java.util.HashSet;

/**
 * Accumulates the key hashes of rows a position-delta write inserts, bounded in memory. Small
 * writes keep an exact hash set (precise cross-writer intersection checks); once the set exceeds
 * lance-core's bloom sizing it spills into a fixed 32 KiB {@link SplitBlockBloomFilter}, so a task
 * or the driver never holds more than the exact-set cap regardless of row count.
 *
 * <p>Each write task ships its accumulator to the driver in the task commit message; the driver
 * merges them and converts the result into the {@link KeyExistenceFilter} attached to the Update
 * transaction. Merging anything into a spilled accumulator spills it too, since exact hashes can
 * always be added to a bloom filter but not the reverse.
 */
final class InsertedKeys implements Serializable {
  private static final long serialVersionUID = 1L;

  // Spilling past the bloom filter's expected item count keeps its designed false-positive rate.
  static final int EXACT_LIMIT = (int) SplitBlockBloomFilter.NUMBER_OF_ITEMS;

  private final HashSet<Long> exactHashes = new HashSet<>();
  private SplitBlockBloomFilter bloom;

  void add(long keyHash) {
    if (bloom != null) {
      bloom.insertHash(keyHash);
      return;
    }
    exactHashes.add(keyHash);
    if (exactHashes.size() > EXACT_LIMIT) {
      spill();
    }
  }

  void merge(InsertedKeys other) {
    if (other.bloom != null) {
      if (bloom == null) {
        spill();
      }
      bloom.union(other.bloom);
      return;
    }
    for (long hash : other.exactHashes) {
      add(hash);
    }
  }

  boolean isEmpty() {
    return bloom == null && exactHashes.isEmpty();
  }

  KeyExistenceFilter toFilter(int[] fieldIds) {
    if (bloom != null) {
      return KeyExistenceFilter.bloom(
          fieldIds,
          bloom.toBytes(),
          SplitBlockBloomFilter.NUM_BITS,
          SplitBlockBloomFilter.NUMBER_OF_ITEMS,
          SplitBlockBloomFilter.PROBABILITY);
    }
    long[] hashes = new long[exactHashes.size()];
    int i = 0;
    for (long hash : exactHashes) {
      hashes[i++] = hash;
    }
    return KeyExistenceFilter.exact(fieldIds, hashes);
  }

  private void spill() {
    bloom = new SplitBlockBloomFilter();
    for (long hash : exactHashes) {
      bloom.insertHash(hash);
    }
    exactHashes.clear();
  }
}
