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

import java.io.Serializable;

/**
 * A split-block bloom filter (Parquet SBBF) with the exact configuration lance-core uses for {@code
 * KeyExistenceFilter} bloom filters: 8192 expected items at 0.00057 false-positive probability,
 * which lance-core's sizing resolves to 32 KiB (1024 blocks, 262144 bits).
 *
 * <p>The size, salt constants, block selection and serialization all follow the Parquet SBBF
 * specification, matching lance-core's Rust implementation bit-for-bit. lance-core's conflict
 * resolver compares two bloom filters by raw bitmap intersection and rejects filters whose
 * configuration differs, so any deviation here would turn every cross-writer check into an error.
 */
final class SplitBlockBloomFilter implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Configuration lance-core declares on its key-existence bloom filters. */
  static final long NUMBER_OF_ITEMS = 8192;

  static final double PROBABILITY = 0.00057;
  static final int NUM_BYTES = 32768;
  static final int NUM_BITS = NUM_BYTES * 8;

  private static final int NUM_BLOCKS = NUM_BYTES / 32;

  /** Salt as defined in the Parquet spec. */
  private static final int[] SALT = {
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
  };

  /** Bitset as 32-bit words in block order; each block is eight consecutive words. */
  private final int[] words = new int[NUM_BLOCKS * 8];

  void insertHash(long hash) {
    int base = blockIndex(hash) * 8;
    int x = (int) hash;
    for (int i = 0; i < 8; i++) {
      words[base + i] |= 1 << ((x * SALT[i]) >>> 27);
    }
  }

  boolean mightContainHash(long hash) {
    int base = blockIndex(hash) * 8;
    int x = (int) hash;
    for (int i = 0; i < 8; i++) {
      if ((words[base + i] & (1 << ((x * SALT[i]) >>> 27))) == 0) {
        return false;
      }
    }
    return true;
  }

  void union(SplitBlockBloomFilter other) {
    for (int i = 0; i < words.length; i++) {
      words[i] |= other.words[i];
    }
  }

  /** Serializes the bitset as the Parquet spec's little-endian word sequence. */
  byte[] toBytes() {
    byte[] bytes = new byte[NUM_BYTES];
    for (int i = 0; i < words.length; i++) {
      int word = words[i];
      int offset = i * 4;
      bytes[offset] = (byte) word;
      bytes[offset + 1] = (byte) (word >>> 8);
      bytes[offset + 2] = (byte) (word >>> 16);
      bytes[offset + 3] = (byte) (word >>> 24);
    }
    return bytes;
  }

  private static int blockIndex(long hash) {
    return (int) (((hash >>> 32) * NUM_BLOCKS) >>> 32);
  }
}
