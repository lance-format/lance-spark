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

import org.junit.jupiter.api.Test;

/** Concrete implementation of BaseBlobCreateTableTest for Spark 3.5. */
public class BlobCreateTableTest extends BaseBlobCreateTableTest {

  /** UPDATE and MERGE commit like DELETE does; Spark 3.4 cannot plan them, so they run here. */
  @Test
  public void testLance12BlobV1TableRejectsUpdateAndMerge() throws Exception {
    String uri = copyLance12Table("top_level");
    assertRejectedBeforeCommit(
        uri,
        catalogName + ".`" + uri + "`",
        "[data]",
        "UPDATE %s SET id = 11 WHERE id = 1",
        "MERGE INTO %s t USING (SELECT 1 AS id) s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET t.id = 11");
  }
}
