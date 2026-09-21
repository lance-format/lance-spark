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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceRefTest {

  @Test
  public void refsUseValueEquality() {
    assertEquals(LanceRef.ofMain(), LanceRef.ofMain());
    assertEquals(LanceRef.ofMain(7), LanceRef.ofMain(7));
    assertEquals(LanceRef.ofBranch("dev"), LanceRef.ofBranch("dev"));
    assertEquals(LanceRef.ofBranch("dev", 7), LanceRef.ofBranch("dev", 7));
    assertEquals(LanceRef.ofTag("release"), LanceRef.ofTag("release"));

    assertNotEquals(LanceRef.ofMain(7), LanceRef.ofMain(8));
    assertNotEquals(LanceRef.ofBranch("dev"), LanceRef.ofBranch("prod"));
    assertNotEquals(LanceRef.ofTag("release"), LanceRef.ofTag("latest"));
  }

  @Test
  public void testRefKind() {
    assertTrue(LanceRef.ofMain().isMain());
    assertFalse(LanceRef.ofMain().isBranch());
    assertTrue(LanceRef.ofBranch("dev").isBranch());
    assertFalse(LanceRef.ofBranch("dev").isMain());
    assertTrue(LanceRef.ofTag("release").isTag());
    assertTrue(LanceRef.ofBranch("dev").isBranchOrTag());
    assertTrue(LanceRef.ofTag("release").isBranchOrTag());
    assertFalse(LanceRef.ofMain().isBranchOrTag());
    assertEquals("branch audit", LanceRef.ofBranch("audit").toString());
    assertEquals("branch audit version 2", LanceRef.ofBranch("audit", 2).toString());
    assertEquals("tag release", LanceRef.ofTag("release").toString());
    assertEquals("version 7", LanceRef.ofMain(7).toString());
    assertEquals("main", LanceRef.ofMain().toString());
  }

  @Test
  public void equalRefsHaveEqualHashCodes() {
    assertEquals(LanceRef.ofMain(7).hashCode(), LanceRef.ofMain(7).hashCode());
    assertEquals(LanceRef.ofBranch("dev", 7).hashCode(), LanceRef.ofBranch("dev", 7).hashCode());
    assertEquals(LanceRef.ofTag("release").hashCode(), LanceRef.ofTag("release").hashCode());
  }

  @Test
  public void optionsCompareRefsByValue() {
    LanceSparkReadOptions readLeft =
        LanceSparkReadOptions.builder()
            .datasetUri("file:///tmp/test")
            .ref(LanceRef.ofBranch("dev", 7))
            .build();
    LanceSparkReadOptions readRight =
        LanceSparkReadOptions.builder()
            .datasetUri("file:///tmp/test")
            .ref(LanceRef.ofBranch("dev", 7))
            .build();
    assertEquals(readLeft, readRight);
    assertEquals(readLeft.hashCode(), readRight.hashCode());

    LanceSparkWriteOptions writeLeft =
        LanceSparkWriteOptions.builder()
            .datasetUri("file:///tmp/test")
            .ref(LanceRef.ofTag("release"))
            .build();
    LanceSparkWriteOptions writeRight =
        LanceSparkWriteOptions.builder()
            .datasetUri("file:///tmp/test")
            .ref(LanceRef.ofTag("release"))
            .build();
    assertEquals(writeLeft, writeRight);
    assertEquals(writeLeft.hashCode(), writeRight.hashCode());
  }
}
