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
package org.lance.spark.read;

import org.lance.spark.TestUtils;

import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class LanceRuntimeFiltering42CompatibilityTest {
  @Test
  public void testReleased42CompatibilityMethodsDispatchToLanceScan() {
    LanceScan scan =
        (LanceScan)
            new LanceScanBuilder(
                    TestUtils.TestTable1Config.schema,
                    TestUtils.TestTable1Config.readOptions,
                    Collections.emptyMap(),
                    null,
                    Collections.emptyMap())
                .build();
    SupportsRuntimeV2Filtering runtimeScan = scan;

    assertFalse(runtimeScan.supportsIterativePushdown());
    assertEquals(0, runtimeScan.pushedPredicates().length);
  }
}
