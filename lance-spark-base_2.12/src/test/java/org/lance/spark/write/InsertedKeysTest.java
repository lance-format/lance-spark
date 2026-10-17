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

import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InsertedKeysTest {

  private static final int[] FIELD_IDS = {1, 2};

  @Test
  public void smallKeySetsProduceAnExactFilter() {
    InsertedKeys keys = new InsertedKeys();
    assertTrue(keys.isEmpty());
    keys.add(10L);
    keys.add(20L);
    keys.add(20L);
    assertFalse(keys.isEmpty());

    KeyExistenceFilter filter = keys.toFilter(FIELD_IDS);
    assertEquals(KeyExistenceFilter.Type.EXACT, filter.getType());
    assertArrayEqualsUnordered(new long[] {10L, 20L}, filter.getExactKeyHashes());
    assertArrayEquals(FIELD_IDS, filter.getFieldIds());
  }

  @Test
  public void exceedingTheExactLimitSpillsToLanceCoreCompatibleBloom() {
    InsertedKeys keys = new InsertedKeys();
    Random random = new Random(3);
    long[] hashes = new long[InsertedKeys.EXACT_LIMIT + 1];
    for (int i = 0; i < hashes.length; i++) {
      hashes[i] = random.nextLong();
      keys.add(hashes[i]);
    }

    KeyExistenceFilter filter = keys.toFilter(FIELD_IDS);
    assertEquals(KeyExistenceFilter.Type.BLOOM, filter.getType());
    // The declared configuration must match lance-core's defaults exactly, else its conflict
    // resolver refuses the comparison.
    assertEquals(SplitBlockBloomFilter.NUMBER_OF_ITEMS, filter.getBloomNumberOfItems());
    assertEquals(SplitBlockBloomFilter.PROBABILITY, filter.getBloomProbability());
    assertEquals(SplitBlockBloomFilter.NUM_BITS, filter.getBloomNumBits());
    assertEquals(SplitBlockBloomFilter.NUM_BYTES, filter.getBloomBitmap().length);

    // Every spilled hash must still be present in the bitmap (parquet-java reads the same format).
    BlockSplitBloomFilter parquet = new BlockSplitBloomFilter(filter.getBloomBitmap());
    for (long hash : hashes) {
      assertTrue(parquet.findHash(hash));
    }
  }

  @Test
  public void mergingExactSetsBelowTheLimitStaysExact() {
    InsertedKeys a = new InsertedKeys();
    InsertedKeys b = new InsertedKeys();
    a.add(1L);
    b.add(2L);
    a.merge(b);

    KeyExistenceFilter filter = a.toFilter(FIELD_IDS);
    assertEquals(KeyExistenceFilter.Type.EXACT, filter.getType());
    assertArrayEqualsUnordered(new long[] {1L, 2L}, filter.getExactKeyHashes());
  }

  @Test
  public void mergingASpilledAccumulatorSpillsTheTarget() {
    InsertedKeys exact = new InsertedKeys();
    exact.add(99L);

    InsertedKeys spilled = new InsertedKeys();
    Random random = new Random(5);
    long[] hashes = new long[InsertedKeys.EXACT_LIMIT + 1];
    for (int i = 0; i < hashes.length; i++) {
      hashes[i] = random.nextLong();
      spilled.add(hashes[i]);
    }

    exact.merge(spilled);
    KeyExistenceFilter filter = exact.toFilter(FIELD_IDS);
    assertEquals(KeyExistenceFilter.Type.BLOOM, filter.getType());

    BlockSplitBloomFilter parquet = new BlockSplitBloomFilter(filter.getBloomBitmap());
    assertTrue(parquet.findHash(99L));
    for (long hash : hashes) {
      assertTrue(parquet.findHash(hash));
    }
  }

  private static void assertArrayEquals(int[] expected, int[] actual) {
    assertTrue(Arrays.equals(expected, actual), Arrays.toString(actual));
  }

  private static void assertArrayEqualsUnordered(long[] expected, long[] actual) {
    Set<Long> expectedSet = new HashSet<>();
    for (long value : expected) {
      expectedSet.add(value);
    }
    Set<Long> actualSet = new HashSet<>();
    for (long value : actual) {
      actualSet.add(value);
    }
    assertEquals(expectedSet, actualSet);
  }
}
