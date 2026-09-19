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

import org.apache.spark.sql.catalyst.expressions.XXH64;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XxHash64Test {

  /**
   * The value lance-core's Rust Sbbf tests pin for {@code XxHash64::oneshot(0, b"")}. The key
   * filters only interoperate if the Java hash matches the Rust one bit-for-bit.
   */
  @Test
  public void emptyInputMatchesLanceCoreVector() {
    assertEquals(Long.parseUnsignedLong("17241709254077376921"), XxHash64.hash(new byte[0]));
  }

  /** Spark's catalyst XXH64 implements the same reference algorithm; use it as an oracle. */
  @Test
  public void matchesCatalystXxHash64() {
    Random random = new Random(42);
    long[] seeds = {0L, 1L, -1L, 0x9747b28cL};
    for (int length = 0; length <= 130; length++) {
      byte[] data = new byte[length];
      random.nextBytes(data);
      for (long seed : seeds) {
        assertEquals(
            XXH64.hashUnsafeBytes(data, Platform.BYTE_ARRAY_OFFSET, length, seed),
            XxHash64.hash(data, 0, length, seed),
            "length=" + length + " seed=" + seed);
      }
    }
    for (int length : new int[] {1024, 4096, 65537}) {
      byte[] data = new byte[length];
      random.nextBytes(data);
      assertEquals(
          XXH64.hashUnsafeBytes(data, Platform.BYTE_ARRAY_OFFSET, length, 0L),
          XxHash64.hash(data),
          "length=" + length);
    }
  }
}
