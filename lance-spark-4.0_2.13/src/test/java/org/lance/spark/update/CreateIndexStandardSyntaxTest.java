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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Spark 4.0-specific CREATE INDEX tests.
 *
 * <p>Tests the standard SQL {@code CREATE INDEX ... ON table ... USING method} syntax which is
 * natively parsed by Spark 4.0+ into a {@code CreateIndex} plan that the Lance parser intercepts.
 */
public class CreateIndexStandardSyntaxTest extends BaseAddIndexTest {

  @Test
  public void testCreateIndexStandardSyntax() {
    spark.sql(String.format("create table %s (id int, text string) using lance;", fullTable));
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(0, 10)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));
    spark.sql(
        String.format(
            "insert into %s (id, text) values %s ;",
            fullTable,
            IntStream.range(10, 20)
                .boxed()
                .map(i -> String.format("(%d, 'text_%d')", i, i))
                .collect(Collectors.joining(","))));

    Dataset<Row> result =
        spark.sql(String.format("create index std_idx on %s (id) using btree", fullTable));

    Assertions.assertEquals(
        "StructType(StructField(fragments_indexed,LongType,true),StructField(index_name,StringType,true))",
        result.schema().toString());

    Row row = result.collectAsList().get(0);
    long fragmentsIndexed = row.getLong(0);
    String indexName = row.getString(1);

    Assertions.assertTrue(fragmentsIndexed >= 2, "Expected at least 2 fragments to be indexed");
    Assertions.assertEquals("std_idx", indexName);

    // Verify query using the indexed field
    Dataset<Row> query = spark.sql(String.format("select * from %s where id=5", fullTable));
    Assertions.assertEquals(1L, query.count());
    Row r = query.collectAsList().get(0);
    Assertions.assertEquals(5, r.getInt(0));
    Assertions.assertEquals("text_5", r.getString(1));
  }
}
