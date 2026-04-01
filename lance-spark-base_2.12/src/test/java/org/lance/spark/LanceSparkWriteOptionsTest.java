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

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceSparkWriteOptionsTest {

  @Test
  public void toReadOptionsOmitsVersionWhenVersionUnset() {
    LanceSparkWriteOptions opts = LanceSparkWriteOptions.from("file:///tmp/t");
    assertNull(opts.getVersion());
    assertFalse(
        opts.toReadOptions().getVersion().isPresent(),
        "Unset version must not populate ReadOptions.version");
    assertFalse(
        opts.toReadOptions(new HashMap<>(), null).getVersion().isPresent(),
        "Unset version must not populate ReadOptions.version");
  }

  @Test
  public void toReadOptionsSetsVersionWhenVersionSet() {
    LanceSparkWriteOptions opts =
        LanceSparkWriteOptions.builder().datasetUri("file:///tmp/t").version(7L).build();
    assertTrue(opts.toReadOptions().getVersion().isPresent());
    assertEquals(7L, opts.toReadOptions().getVersion().get());
    assertTrue(opts.toReadOptions(new HashMap<>(), null).getVersion().isPresent());
    assertEquals(7L, opts.toReadOptions(new HashMap<>(), null).getVersion().get());
  }

  @Test
  public void withVersionCopiesOptions() {
    LanceSparkWriteOptions base = LanceSparkWriteOptions.from("file:///tmp/t");
    LanceSparkWriteOptions pinned = base.withVersion(3L);
    assertEquals(3L, pinned.getVersion());
    assertNull(base.getVersion());
  }
}
