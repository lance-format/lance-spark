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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.spark.LanceSparkReadOptions

import java.util.Random

import scala.collection.mutable

class ExactKSamplerTest {
  @Test
  def goldenSelection(): Unit = {
    assertEquals("floyd-v2", ExactKSampler.Version)
    assertArrayEquals(Array[Long](5, 7, 9, 13, 36, 63, 95), ExactKSampler.sample(100, 7, 42))
    assertArrayEquals(Array[Long](1, 3, 5, 6, 12, 13, 15), ExactKSampler.sample(16, 7, 42))
    assertArrayEquals(Array[Long](23, 47, 64, 65, 71, 111, 120), ExactKSampler.sample(128, 7, 42))
    assertArrayEquals(
      Array[Long](405481101665L, 408229177271L, 489512234525L, 663726459471L,
        830455457952L, 956606868897L, 1033071240037L),
      ExactKSampler.sample(1L << 40, 7, 42))
    assertArrayEquals(
      Array[Long](93830867483L, 197458609219L, 300221710454L, 333214231862L, 682877160444L),
      ExactKSampler.sample((1L << 40) + 99, 5, -7))
    assertArrayEquals(
      Array[Long](2218556890522892383L, 5076885383333679745L, 5511382431250863030L,
        5879138130930366493L, 6741987804016584860L),
      ExactKSampler.sample(Long.MaxValue, 5, 0))
  }

  @Test
  def boundaries(): Unit = {
    assertArrayEquals(Array.empty[Long], ExactKSampler.sample(0, 0, 0))
    assertArrayEquals(Array.empty[Long], ExactKSampler.sample(Long.MaxValue, 0, -1))
    assertArrayEquals((0L until 12L).toArray, ExactKSampler.sample(12, 12, 0))
    assertThrows(classOf[IllegalArgumentException], () => ExactKSampler.sample(1, 2, 0))
    assertThrows(classOf[IllegalArgumentException], () => ExactKSampler.sample(-1, 0, 0))
    assertThrows(classOf[IllegalArgumentException], () => ExactKSampler.sample(12, -1, 0))
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        ExactKSampler.sample(Long.MaxValue, LanceSparkReadOptions.MAX_SAMPLE_SIZE + 1, 0))
  }

  @Test
  def everySmallSubsetHasEqualDrawMultiplicity(): Unit = {
    // Enumerating bounded draws tests uniformity without statistical/flaky assertions.
    for (n <- 0 to 6; k <- 0 to n) {
      val counts = mutable.Map.empty[Seq[Long], Int].withDefaultValue(0)
      def enumerate(draws: List[Long], j: Int): Unit = {
        if (j == n) {
          val iterator = draws.reverse.iterator
          val sample = ExactKSampler.select(n, k, _ => iterator.next()).toSeq
          assertEquals(k, sample.distinct.length)
          counts(sample) += 1
        } else {
          (0 to j).foreach(value => enumerate(value.toLong :: draws, j + 1))
        }
      }
      enumerate(Nil, n - k)
      val expected = (0 until n).map(_.toLong).combinations(k).map(_.toSeq).toSet
      assertEquals(expected, counts.keySet.toSet)
      assertEquals(1, counts.values.toSet.size)
    }
  }

  @Test
  def boundedDrawRejectsOverflowAndHandlesPowersOfTwo(): Unit = {
    val values = Iterator(-1L, 2L)
    val random = new Random(0) {
      override def nextLong(): Long = values.next()
    }
    assertEquals(1L, ExactKSampler.nextLong(random, Long.MaxValue))
    assertFalse(values.hasNext, "Overflowing bounded draw must be rejected")
    assertEquals(0L, ExactKSampler.nextLong(new Random(42), 1))
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        ExactKSampler.nextLong(new Random(42), 0))
    for (bound <- Seq(2L, 16L, 1L << 40, Long.MaxValue)) {
      val rng = new Random(42)
      (0 until 100).foreach { _ =>
        val value = ExactKSampler.nextLong(rng, bound)
        assertTrue(value >= 0 && value < bound)
      }
    }
  }

  @Test
  def powersOfTwoUseHighBits(): Unit = {
    val highBit = new Random(0) {
      override def nextLong(): Long = Long.MinValue
    }
    assertEquals(0L, ExactKSampler.nextLong(highBit, 1))
    assertEquals(1L, ExactKSampler.nextLong(highBit, 2))
    assertEquals(4L, ExactKSampler.nextLong(highBit, 8))
    assertEquals(1L << 61, ExactKSampler.nextLong(highBit, 1L << 62))
    val lowBits = new Random(0) {
      override def nextLong(): Long = 6L
    }
    assertEquals(0L, ExactKSampler.nextLong(lowBits, 8))
  }

  @Test
  def maximumSampleFitsBoundedHeap(): Unit = {
    val output = LanceSampleTestProcess.run("org.lance.spark.read.SampleHeapProbe", Some("64m"))
    assertTrue(output.contains("MAX_SAMPLE_OK rows=1000000"), output)
  }
}
