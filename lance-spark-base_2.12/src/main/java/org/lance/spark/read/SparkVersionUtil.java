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

import org.apache.spark.package$;

/**
 * Runtime Spark-version gates for features whose behavior differs across supported Spark versions.
 *
 * <p>Kept in a single helper so pre-merge findings on multi-key SPJ (Spark 3.4 vs 3.5+) can be
 * adjusted in one place rather than scattered across the codebase.
 */
final class SparkVersionUtil {

  private SparkVersionUtil() {}

  /**
   * Whether the running Spark version reliably honors multi-key {@code KeyGroupedPartitioning}
   * without silently falling back to a shuffle. Uses an explicit allowlist (Spark 3.5.x, 4.x+)
   * rather than a denylist so custom forks and unexpected version strings default to the safe "fall
   * back to UnknownPartitioning" behavior.
   */
  static boolean supportsMultiKeySpj() {
    return supportsMultiKeySpj(package$.MODULE$.SPARK_VERSION());
  }

  /**
   * Package-private pure-function overload for unit tests. Inputs the version string directly so
   * the parse/allowlist logic can be exercised without a running SparkContext.
   */
  static boolean supportsMultiKeySpj(String version) {
    if (version == null) {
      return false;
    }
    // Accept "3.5.x" or any 4.x+ build. Reject everything else (3.4.x, 3.3.x, custom strings).
    if (version.startsWith("3.5.")) {
      return true;
    }
    int dot = version.indexOf('.');
    if (dot <= 0) {
      return false;
    }
    try {
      int major = Integer.parseInt(version.substring(0, dot));
      return major >= 4;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
