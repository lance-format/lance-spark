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
package org.lance.spark.write;

import org.lance.WriteParams;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.spark.annotation.Evolving;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The Lance write parameters {@link LanceBatchWrite#commit} expects for fragments an external
 * writer produces, from {@link LanceBatchWrite#getFragmentWriteSpec()}. It does not cover what
 * lance-spark's row writer does beyond those parameters, such as rolling fragments at sharding-key
 * boundaries or resolving blob references.
 *
 * <p>It is serializable, so a driver can send it to executors; Lance's {@link WriteParams} is not,
 * so build those on the executor with {@link #toWriteParams()}. It carries the storage options,
 * which can include credentials, so do not log or persist it.
 */
@Evolving
public final class FragmentWriteSpec implements Serializable {
  private static final long serialVersionUID = 1L;

  private final LanceSparkWriteOptions writeOptions;
  private final Map<String, String> initialStorageOptions;

  FragmentWriteSpec(
      LanceSparkWriteOptions writeOptions, Map<String, String> initialStorageOptions) {
    this.writeOptions = writeOptions;
    this.initialStorageOptions =
        initialStorageOptions == null ? null : new HashMap<>(initialStorageOptions);
  }

  /** Returns the URI of the dataset the fragments must be written to. */
  public String getDatasetUri() {
    return writeOptions.getDatasetUri();
  }

  /**
   * Returns the parameters to pass to {@code WriteFragmentBuilder.writeParams(...)}: the write mode
   * {@code commit} expects, the resolved {@code file_format_version}, the file and row group size
   * limits, the blob packing threshold, and the storage options, including credentials vended by a
   * namespace. Stable row ids are off, because {@code commit} assigns them.
   */
  public WriteParams toWriteParams() {
    return writeOptions.toWriteParams(initialStorageOptions);
  }
}
