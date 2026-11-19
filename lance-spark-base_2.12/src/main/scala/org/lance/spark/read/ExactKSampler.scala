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
package org.lance.spark.read

import org.lance.spark.LanceSparkReadOptions

import java.lang.Long.numberOfLeadingZeros
import java.util.{Arrays, Random}

import scala.collection.mutable

/** Samples logical positions; the PRNG and bounded draws are part of the replay contract. */
object ExactKSampler {
  val Version = "floyd-v2"

  def sample(population: Long, size: Long, seed: Long): Array[Long] = {
    val random = new Random(seed)
    select(population, size, bound => nextLong(random, bound))
  }

  private[read] def select(
      population: Long,
      size: Long,
      draw: Long => Long): Array[Long] = {
    require(population >= 0, "Sample population must be nonnegative")
    require(size >= 0 && size <= population, "sample_size must be between 0 and the live row count")
    require(
      size <= LanceSparkReadOptions.MAX_SAMPLE_SIZE,
      "sample_size exceeds driver planning limit")
    // Primitive keys avoid retaining a boxed Long for every selected position.
    val selected = new mutable.LongMap[Unit](math.max(16, size.toInt * 2))
    var j = population - size
    while (j < population) {
      val candidate = draw(j + 1)
      selected.update(if (selected.contains(candidate)) j else candidate, ())
      j += 1
    }
    // The final size is known; avoid iterator.toArray's growing temporary buffers.
    val positions = new Array[Long](size.toInt)
    var index = 0
    selected.foreachKey { position =>
      positions(index) = position
      index += 1
    }
    Arrays.sort(positions)
    positions
  }

  // Rejection avoids modulo bias; >>> avoids abs(Long.MinValue) and retains 63 random bits.
  private[read] def nextLong(random: Random, bound: Long): Long = {
    require(bound > 0, "Random bound must be positive")
    val mask = bound - 1
    var bits = random.nextLong() >>> 1
    if ((bound & mask) == 0) {
      // Prefer the high bits of the LCG output, as Random.nextInt does for powers of two.
      bits >>> numberOfLeadingZeros(bound)
    } else {
      var value = bits % bound
      while (bits - value + mask < 0) {
        bits = random.nextLong() >>> 1
        value = bits % bound
      }
      value
    }
  }
}
