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

/**
 * xxHash64 over a byte array, producing the same value as the xxHash reference implementation.
 *
 * <p>lance-core hashes primary key bytes with xxHash64 (seed 0) before inserting them into a {@code
 * KeyExistenceFilter} bloom filter, so this must match bit-for-bit for the Spark-built filter to
 * interoperate with filters built by lance-core.
 */
final class XxHash64 {

  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

  private XxHash64() {}

  /** Hash of the whole array with seed 0, the seed lance-core uses for key filters. */
  static long hash(byte[] data) {
    return hash(data, 0, data.length, 0L);
  }

  static long hash(byte[] data, int offset, int length, long seed) {
    long hash;
    int index = offset;
    int end = offset + length;

    if (length >= 32) {
      long v1 = seed + PRIME64_1 + PRIME64_2;
      long v2 = seed + PRIME64_2;
      long v3 = seed;
      long v4 = seed - PRIME64_1;
      int limit = end - 32;
      do {
        v1 = round(v1, readLongLE(data, index));
        v2 = round(v2, readLongLE(data, index + 8));
        v3 = round(v3, readLongLE(data, index + 16));
        v4 = round(v4, readLongLE(data, index + 24));
        index += 32;
      } while (index <= limit);
      hash =
          Long.rotateLeft(v1, 1)
              + Long.rotateLeft(v2, 7)
              + Long.rotateLeft(v3, 12)
              + Long.rotateLeft(v4, 18);
      hash = mergeRound(hash, v1);
      hash = mergeRound(hash, v2);
      hash = mergeRound(hash, v3);
      hash = mergeRound(hash, v4);
    } else {
      hash = seed + PRIME64_5;
    }

    hash += length;

    while (index + 8 <= end) {
      hash ^= round(0L, readLongLE(data, index));
      hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
      index += 8;
    }
    if (index + 4 <= end) {
      hash ^= (readIntLE(data, index) & 0xFFFFFFFFL) * PRIME64_1;
      hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
      index += 4;
    }
    while (index < end) {
      hash ^= (data[index] & 0xFFL) * PRIME64_5;
      hash = Long.rotateLeft(hash, 11) * PRIME64_1;
      index++;
    }

    hash ^= hash >>> 33;
    hash *= PRIME64_2;
    hash ^= hash >>> 29;
    hash *= PRIME64_3;
    hash ^= hash >>> 32;
    return hash;
  }

  private static long round(long acc, long input) {
    acc += input * PRIME64_2;
    return Long.rotateLeft(acc, 31) * PRIME64_1;
  }

  private static long mergeRound(long hash, long value) {
    hash ^= round(0L, value);
    return hash * PRIME64_1 + PRIME64_4;
  }

  private static long readLongLE(byte[] data, int index) {
    return (data[index] & 0xFFL)
        | (data[index + 1] & 0xFFL) << 8
        | (data[index + 2] & 0xFFL) << 16
        | (data[index + 3] & 0xFFL) << 24
        | (data[index + 4] & 0xFFL) << 32
        | (data[index + 5] & 0xFFL) << 40
        | (data[index + 6] & 0xFFL) << 48
        | (data[index + 7] & 0xFFL) << 56;
  }

  private static int readIntLE(byte[] data, int index) {
    return (data[index] & 0xFF)
        | (data[index + 1] & 0xFF) << 8
        | (data[index + 2] & 0xFF) << 16
        | (data[index + 3] & 0xFF) << 24;
  }
}
