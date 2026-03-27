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
package org.lance.spark.utils;

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Transaction;
import org.lance.operation.UpdateConfig;
import org.lance.operation.UpdateMap;

import java.util.Collections;

/** Utilities for reading and writing Lance dataset config entries. */
public final class DatasetConfigUtils {

  private DatasetConfigUtils() {}

  /**
   * Sets a single config entry on a dataset by committing an {@link UpdateConfig} operation. This
   * is the non-deprecated equivalent of {@code Dataset.updateConfig(Map)}.
   */
  public static void setConfigEntry(final Dataset dataset, final String key, final String value) {
    final UpdateMap configUpdate =
        UpdateMap.builder().updates(Collections.singletonMap(key, value)).build();
    final UpdateConfig operation = UpdateConfig.builder().configUpdates(configUpdate).build();
    final CommitBuilder builder = new CommitBuilder(dataset).writeParams(Collections.emptyMap());
    try (Transaction txn =
            new Transaction.Builder().readVersion(dataset.version()).operation(operation).build();
        Dataset committed = builder.execute(txn)) {
      // auto-close txn and committed dataset
    }
  }
}
