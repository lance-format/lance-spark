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
package org.lance.spark.bundle;

/**
 * Information about the Lance Spark Bundle for Spark 3.4 with Scala 2.12.
 *
 * <p>This is a bundled JAR containing all necessary dependencies for the Lance Spark connector.
 */
public final class BundleInfo {
  private BundleInfo() {
    // Utility class
  }

  /**
   * @return the bundle name
   */
  public static String getBundleName() {
    return "lance-spark-bundle-3.4_2.12";
  }
}
