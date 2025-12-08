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

public class AddColumnsBackfillTest extends BaseAddColumnsBackfillTest {
  // All test methods are inherited from BaseAddColumnsBackfillTest

  @Test
  public void testWithDeletedRecords() {
    prepareDataset();

    // Delete some rows
    spark.sql(String.format("delete from %s where id in (0, 1, 4, 8, 9);", fullTable));

    spark.sql(
        String.format(
            "create temporary view tmp_view as select _rowaddr, _fragid, id * 100 as new_col1, id * 2 as new_col2, id * 3 as new_col3 from %s;",
            fullTable));
    spark.sql(
        String.format("alter table %s add columns new_col1, new_col2 from tmp_view", fullTable));

    Assertions.assertEquals(
        "[[2,200,4,text_2], [3,300,6,text_3], [5,500,10,text_5], [6,600,12,text_6], [7,700,14,text_7]]",
        spark
            .sql(String.format("select id, new_col1, new_col2, text from %s", fullTable))
            .collectAsList()
            .toString());
  }
}
