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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SparkVersionUtil#supportsMultiKeySpj(String)} — the pure-function overload
 * that the allowlist logic delegates to. Two cases: strings the allowlist accepts, and strings it
 * rejects (including malformed input).
 */
public class SparkVersionUtilTest {

  @Test
  public void acceptsAllowlistedVersions() {
    String[] accepted = {
      // Spark 3.5.x is the minimum version that reliably honors multi-key KGP.
      "3.5.0",
      "3.5.1",
      "3.5.10",
      // 3.5.x with snapshot / rc / vendor suffix: startsWith("3.5.") still matches.
      "3.5.0-SNAPSHOT",
      "3.5.1-rc1",
      "3.5.2-databricks",
      // Any 4.x+ build is accepted via the major-version branch.
      "4.0.0",
      "4.1.0",
      "4.0.0-preview",
      "5.0.0",
      "10.0.0",
    };
    for (String v : accepted) {
      assertTrue(SparkVersionUtil.supportsMultiKeySpj(v), "expected accepted: " + v);
    }
  }

  @Test
  public void rejectsUnsupportedAndMalformed() {
    String[] rejected = {
      // Pre-3.5 Spark lines: 3.4.x and earlier don't reliably honor multi-key KGP.
      "3.4.0",
      "3.4.1",
      "3.4.0-preview",
      "3.3.0",
      "3.0.0",
      "2.4.8",
      // 3.6+ through 3.x: conservatively rejected until the allowlist is explicitly updated.
      "3.6.0",
      "3.9.9",
      // null / empty / no-dot / leading-dot / non-numeric major: all unparseable → reject.
      null,
      "",
      "3",
      "custom",
      ".5.0",
      "vX.0.0",
      "custom-fork.1.0",
    };
    for (String v : rejected) {
      assertFalse(SparkVersionUtil.supportsMultiKeySpj(v), "expected rejected: " + v);
    }
  }
}
