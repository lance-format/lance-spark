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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnsignedIntWriteRoundTripTest extends BaseUnsignedIntWriteRoundTripTest {

  @Test
  public void testUpdateKeepsUnsignedTypes() throws Exception {
    String datasetUri = tempDir.resolve("uint_update.lance").toString();
    createUnsignedDataset(datasetUri);

    spark.sql("UPDATE lance.`" + datasetUri + "` SET u8 = 254 WHERE id = 1");

    assertUnsignedColumns(datasetUri);
    List<Row> rows =
        spark
            .read()
            .format(LanceDataSource.name)
            .load(datasetUri)
            .selectExpr("id", "u8", "u16", "u32", "u64")
            .collectAsList();
    assertEquals(
        Collections.singletonList(
            RowFactory.create(1, (short) 254, 65535, 4294967295L, 9000000000L)),
        rows);
  }
}
