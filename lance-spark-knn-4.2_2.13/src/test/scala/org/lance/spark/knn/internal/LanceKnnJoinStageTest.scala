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
package org.lance.spark.knn.internal

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.concurrent.atomic.AtomicInteger

/**
 * Backend-free unit tests for the two pieces of [[LanceKnnJoinStage]] that don't need a Lance
 * dataset: the lazy per-partition output composition ([[LanceKnnJoinStage.lazyJoinIterator]]) and
 * the Spark-type coercion of materialized right-side payloads
 * ([[LanceKnnJoinStage.coerceToSpark]]). Both are `private[knn]`, so this test lives in the
 * `org.lance.spark.knn.internal` package to reach them. The full probe → trim → materialize
 * pipeline is covered by the e2e test in this module against a real Lance dataset.
 */
class LanceKnnJoinStageTest {

  // -- fix #5: streaming output must be lazy ------------------------------------------------

  /**
   * `lazyJoinIterator` must pull left rows ON DEMAND, never drain them up front — otherwise the
   * whole partition would materialize in memory before a single output row is produced (the exact
   * regression the streaming rewrite fixed). We feed a large source with a side-effect pull counter,
   * take only the first 3 outputs (one per left row here), and assert only 3 left rows were pulled.
   */
  @Test def testLazyJoinIteratorPullsLeftRowsOnDemand(): Unit = {
    val pulled = new AtomicInteger(0)
    val left: Iterator[Row] = Iterator.range(0, 1000000).map { i =>
      pulled.incrementAndGet()
      Row(i)
    }
    val out = LanceKnnJoinStage.lazyJoinIterator(left, r => Iterator.single(r))
    val first3 = out.take(3).toList
    assertEquals(3, first3.size, "should surface exactly the requested rows")
    assertTrue(
      pulled.get() <= 3,
      s"lazyJoinIterator must pull left rows on demand, not drain them; pulled ${pulled.get()}")
  }

  /**
   * Fan-out (each left row expands to several join rows) stays lazy too: taking 3 outputs at 2 rows
   * per left row must pull only the first 2 left rows, not the whole source.
   */
  @Test def testLazyJoinIteratorFansOutLazily(): Unit = {
    val pulled = new AtomicInteger(0)
    val left: Iterator[Row] = Iterator.range(0, 1000000).map { i =>
      pulled.incrementAndGet()
      Row(i)
    }
    val out = LanceKnnJoinStage.lazyJoinIterator(
      left,
      r => {
        val i = r.getInt(0)
        Iterator(Row(i, 0), Row(i, 1))
      })
    val first3 = out.take(3).toList
    assertEquals(3, first3.size)
    assertTrue(pulled.get() <= 2, s"expected <= 2 left pulls for 3 outputs, got ${pulled.get()}")
  }

  // -- fix #3: schema-aware materialization -------------------------------------------------

  /** A `Map` payload for a `StructType` slot becomes a positional `Row` in declared field order. */
  @Test def testCoerceStructFromMapToRowInFieldOrder(): Unit = {
    val dt = new StructType().add("a", IntegerType).add("b", StringType)
    // Keyed by name and deliberately out of declared order — coercion must reorder by field.
    val value = Map("b" -> "x", "a" -> Integer.valueOf(1))
    val row = LanceKnnJoinStage.coerceToSpark(value, dt).asInstanceOf[Row]
    assertEquals(1, row.getInt(0))
    assertEquals("x", row.getString(1))
  }

  /** A field absent from the payload map materializes as null, not a missing slot. */
  @Test def testCoerceStructFillsMissingFieldWithNull(): Unit = {
    val dt = new StructType().add("a", IntegerType).add("b", StringType)
    val value = Map[String, Any]("a" -> Integer.valueOf(1))
    val row = LanceKnnJoinStage.coerceToSpark(value, dt).asInstanceOf[Row]
    assertEquals(1, row.getInt(0))
    assertTrue(row.isNullAt(1), "missing struct field should be null")
  }

  /** `ArrayType` recurses: an array of struct payloads becomes a `Seq[Row]`. */
  @Test def testCoerceArrayOfStructsRecurses(): Unit = {
    val dt = ArrayType(new StructType().add("a", IntegerType))
    val value = Seq(Map("a" -> Integer.valueOf(7)))
    val out = LanceKnnJoinStage.coerceToSpark(value, dt).asInstanceOf[Seq[_]]
    assertEquals(1, out.size)
    assertEquals(7, out.head.asInstanceOf[Row].getInt(0))
  }

  /** `MapType` recurses on its values: a struct-valued map entry becomes a `Row`. */
  @Test def testCoerceMapOfStructsRecurses(): Unit = {
    val dt = MapType(StringType, new StructType().add("a", IntegerType))
    val value = Map("k" -> Map("a" -> Integer.valueOf(9)))
    val out =
      LanceKnnJoinStage.coerceToSpark(value, dt).asInstanceOf[scala.collection.Map[String, Any]]
    assertEquals(9, out("k").asInstanceOf[Row].getInt(0))
  }

  /** Primitives pass straight through; null stays null. */
  @Test def testCoercePrimitivesAndNullPassThrough(): Unit = {
    assertEquals("hello", LanceKnnJoinStage.coerceToSpark("hello", StringType))
    assertEquals(42, LanceKnnJoinStage.coerceToSpark(Integer.valueOf(42), IntegerType))
    assertNull(LanceKnnJoinStage.coerceToSpark(null, IntegerType).asInstanceOf[AnyRef])
  }
}
