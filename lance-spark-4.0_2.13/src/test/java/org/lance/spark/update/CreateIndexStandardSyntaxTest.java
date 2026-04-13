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
package org.lance.spark.update;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies that standard {@code CREATE INDEX} syntax is intercepted on Spark 4.0+ with a helpful
 * error message directing users to the Lance-specific {@code ALTER TABLE ... CREATE INDEX} syntax.
 */
public class CreateIndexStandardSyntaxTest extends BaseAddIndexTest {

  @Test
  public void testCreateIndexStandardSyntaxThrowsHelpfulError() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                spark.sql(String.format("create index std_idx on %s (id) using btree", fullTable)));

    Assertions.assertTrue(
        exception.getMessage().contains("ALTER TABLE"),
        "Expected error message to mention ALTER TABLE syntax, got: " + exception.getMessage());
  }
}
