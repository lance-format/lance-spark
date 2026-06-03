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
package org.lance.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end write/read round-trip test for Spark 4.1 {@code TimeType}.
 *
 * <p>TimeType stores time-of-day as nanoseconds since midnight internally (via {@code
 * LocalTime.getLong(NANO_OF_DAY)}). On the Arrow wire it uses Time(NANOSECOND, 64), so the value
 * passes through without conversion. This test verifies the full write -> lance-file -> read path.
 *
 * <p>Since Spark 4.1 CatalystTypeConverters does not yet support TimeType for {@code
 * createDataFrame}, we use SQL expressions to construct TimeType values.
 */
public class TimeTypeRoundtripTest {
  private static SparkSession spark;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-timetype-roundtrip-test")
            .master("local")
            // TimeType is gated behind a feature flag in Spark 4.1
            .config("spark.sql.timeType.enabled", "true")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private String outputPath(String name) {
    return tempDir.resolve(name + ".lance").toString();
  }

  @Test
  public void testTimeTypeRoundtrip() {
    // Use SQL to create TimeType values — Spark 4.1 supports TIME literals in SQL.
    Dataset<Row> df =
        spark.sql(
            "SELECT 0 AS id, CAST('00:00:00' AS TIME) AS t "
                + "UNION ALL SELECT 1, CAST('12:30:45.123456' AS TIME) "
                + "UNION ALL SELECT 2, CAST('23:59:59.999999' AS TIME) "
                + "UNION ALL SELECT 3, CAST(NULL AS TIME)");

    String path = outputPath("time");
    df.write().format("lance").save(path);
    Dataset<Row> result = spark.read().format("lance").load(path);

    // Verify the read-back schema has TimeType
    String readTypeName = result.schema().apply("t").dataType().getClass().getName();
    assertEquals("org.apache.spark.sql.types.TimeType", readTypeName);

    List<Row> out = result.orderBy("id").collectAsList();
    assertEquals(4, out.size());

    // Spark exposes TimeType values as java.time.LocalTime at the Row level
    assertEquals(LocalTime.of(0, 0, 0), out.get(0).<LocalTime>getAs(1));
    assertEquals(LocalTime.of(12, 30, 45, 123_456_000), out.get(1).<LocalTime>getAs(1));
    assertEquals(LocalTime.of(23, 59, 59, 999_999_000), out.get(2).<LocalTime>getAs(1));
    assertTrue(out.get(3).isNullAt(1));
  }

  @Test
  public void testTimeTypeNullOnly() {
    Dataset<Row> df =
        spark.sql(
            "SELECT 0 AS id, CAST(NULL AS TIME) AS t " + "UNION ALL SELECT 1, CAST(NULL AS TIME)");

    String path = outputPath("time_null");
    df.write().format("lance").save(path);
    List<Row> out = spark.read().format("lance").load(path).orderBy("id").collectAsList();
    assertEquals(2, out.size());
    assertTrue(out.get(0).isNullAt(1));
    assertTrue(out.get(1).isNullAt(1));
  }
}
