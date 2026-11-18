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
package org.lance.spark.extensions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Verifies that the Lance SQL extensions parser stays transparent for plain Spark SQL. Every {@link
 * org.apache.spark.sql.catalyst.parser.ParserInterface} method the extension does not intercept has
 * to reach the delegate, otherwise enabling the extension breaks SQL that has nothing to do with
 * Lance.
 *
 * <p>Spark 4.0 has no parameterized {@code parsePlanWithParameters} entry point, so only the
 * routine parameter case applies here.
 */
public class SqlExtensionsParserDelegationTest {

  private SparkSession spark;

  @BeforeEach
  public void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-parser-delegation-test")
            .master("local[1]")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .getOrCreate();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testRoutineParamIsDelegated() throws Exception {
    StructType params = spark.sessionState().sqlParser().parseRoutineParam("x INT, y STRING");

    Assertions.assertEquals(2, params.size());
    Assertions.assertEquals("x", params.fields()[0].name());
    Assertions.assertEquals("y", params.fields()[1].name());
  }
}
