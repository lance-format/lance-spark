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

import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SplitBlockBloomFilterTest {

  /**
   * parquet-java's BlockSplitBloomFilter implements the same Parquet SBBF spec lance-core's Rust
   * filter follows; a byte-identical bitmap for the same hashes shows this implementation matches
   * the spec (and therefore lance-core) bit-for-bit.
   */
  @Test
  public void bitmapMatchesParquetImplementation() throws IOException {
    SplitBlockBloomFilter ours = new SplitBlockBloomFilter();
    BlockSplitBloomFilter parquet = new BlockSplitBloomFilter(SplitBlockBloomFilter.NUM_BYTES);
    Random random = new Random(7);
    for (int i = 0; i < 20000; i++) {
      long hash = random.nextLong();
      ours.insertHash(hash);
      parquet.insertHash(hash);
    }
    ByteArrayOutputStream parquetBytes = new ByteArrayOutputStream();
    parquet.writeTo(parquetBytes);
    assertArrayEquals(parquetBytes.toByteArray(), ours.toBytes());
  }

  @Test
  public void insertedHashesAreAlwaysFound() {
    SplitBlockBloomFilter filter = new SplitBlockBloomFilter();
    Random random = new Random(11);
    long[] hashes = new long[10000];
    for (int i = 0; i < hashes.length; i++) {
      hashes[i] = random.nextLong();
      filter.insertHash(hashes[i]);
    }
    for (long hash : hashes) {
      assertTrue(filter.mightContainHash(hash));
    }
  }

  @Test
  public void absentHashesAreMostlyRejected() {
    SplitBlockBloomFilter filter = new SplitBlockBloomFilter();
    Random random = new Random(13);
    for (int i = 0; i < 8192; i++) {
      filter.insertHash(random.nextLong());
    }
    int falsePositives = 0;
    for (int i = 0; i < 100000; i++) {
      if (filter.mightContainHash(random.nextLong())) {
        falsePositives++;
      }
    }
    // Designed false-positive probability is 0.00057; 100 in 100k is far above it.
    assertTrue(falsePositives < 100, "false positives: " + falsePositives);
  }

  @Test
  public void unionContainsBothSides() {
    SplitBlockBloomFilter a = new SplitBlockBloomFilter();
    SplitBlockBloomFilter b = new SplitBlockBloomFilter();
    Random random = new Random(17);
    long[] inA = new long[500];
    long[] inB = new long[500];
    for (int i = 0; i < 500; i++) {
      inA[i] = random.nextLong();
      inB[i] = random.nextLong();
      a.insertHash(inA[i]);
      b.insertHash(inB[i]);
    }
    a.union(b);
    for (long hash : inA) {
      assertTrue(a.mightContainHash(hash));
    }
    for (long hash : inB) {
      assertTrue(a.mightContainHash(hash));
    }
  }

  @Test
  public void emptyFilterContainsNothingAndSerializesToDeclaredSize() {
    SplitBlockBloomFilter filter = new SplitBlockBloomFilter();
    assertFalse(filter.mightContainHash(42L));
    byte[] bytes = filter.toBytes();
    assertEquals(SplitBlockBloomFilter.NUM_BYTES, bytes.length);
    assertEquals(SplitBlockBloomFilter.NUM_BITS, bytes.length * 8);
  }
}
