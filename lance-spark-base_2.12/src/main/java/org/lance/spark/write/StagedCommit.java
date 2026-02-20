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

import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Holds the state needed to commit a staged table operation. This is used to defer the actual
 * commit from LanceBatchWrite.commit() to LanceDataset.commitStagedChanges().
 */
public class StagedCommit {
  private final Dataset dataset;
  private final List<FragmentMetadata> fragments;
  private final Schema schema;
  private final boolean overwrite;

  public StagedCommit(
      Dataset dataset, List<FragmentMetadata> fragments, Schema schema, boolean overwrite) {
    this.dataset = dataset;
    this.fragments = fragments;
    this.schema = schema;
    this.overwrite = overwrite;
  }

  /** Performs the actual commit using the stored dataset and fragments. */
  public void commit() {
    Operation operation;
    if (overwrite) {
      operation = Overwrite.builder().fragments(fragments).schema(schema).build();
    } else {
      operation = Append.builder().fragments(fragments).build();
    }
    dataset.newTransactionBuilder().operation(operation).build().commit();
  }

  /** Closes the dataset without committing. Used for abort scenarios. */
  public void close() {
    dataset.close();
  }
}
