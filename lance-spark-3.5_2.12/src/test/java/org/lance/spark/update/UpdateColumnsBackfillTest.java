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

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateColumnsBackfillTest extends BaseUpdateColumnsBackfillTest {
  // All test methods are inherited from BaseUpdateColumnsBackfillTest

  @Test
  public void testUpdateWithDeletedRecords() {
    prepareDataset();

    // Delete row with id=1
    spark.sql(String.format("delete from %s where id = 1;", fullTable));

    // Update value for id=2
    spark.sql(
        String.format(
            "create temporary view update_source as "
                + "select _rowaddr, _fragid, 200 as value "
                + "from %s where id = 2",
            fullTable));
    spark.sql(String.format("alter table %s update columns value from update_source", fullTable));

    List<Row> results =
        spark.sql(String.format("select id, value from %s order by id", fullTable)).collectAsList();

    // Should only have 2 rows (id=2, id=3) after delete
    assertEquals(2, results.size());
    assertEquals(2, results.get(0).getInt(0));
    assertEquals(200, results.get(0).getInt(1)); // updated
    assertEquals(3, results.get(1).getInt(0));
    assertEquals(30, results.get(1).getInt(1)); // unchanged
  }
}
