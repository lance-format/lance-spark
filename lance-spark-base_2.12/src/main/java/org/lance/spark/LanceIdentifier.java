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

import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * A path-based identifier for Lance datasets. This stores the full location/path and provides
 * methods to access it.
 *
 * <p>Similar to Iceberg's PathIdentifier, this allows the catalog to recognize path-based
 * identifiers and use the full location to load/create tables.
 */
public class LanceIdentifier implements Identifier {
  private final String[] namespace;
  private final String location;
  private final String name;

  /**
   * Creates a LanceIdentifier from a full dataset location.
   *
   * @param location the full path to the Lance dataset (e.g., "s3://bucket/path/table.lance" or
   *     "/local/path/table.lance")
   */
  public LanceIdentifier(String location) {
    this.location = location;

    // Split the location into namespace (parent path) and name (dataset name)
    int lastSlashIndex = location.lastIndexOf('/');
    if (lastSlashIndex > 0) {
      this.namespace = new String[] {location.substring(0, lastSlashIndex)};
      this.name = location.substring(lastSlashIndex + 1);
    } else {
      this.namespace = new String[0];
      this.name = location;
    }
  }

  @Override
  public String[] namespace() {
    return this.namespace;
  }

  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the full location/path of the Lance dataset.
   *
   * @return the full dataset location
   */
  public String location() {
    return location;
  }
}
