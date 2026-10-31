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
import org.lance.operation.Overwrite;

import java.util.function.Supplier;

/**
 * The connector's commits to an existing table. Writes and commands commit through different Lance
 * calls, and some commands commit inside a Lance call such as {@code createIndex}, so each commit
 * goes through one of these methods. Before each commit call they check the data files of the
 * version a write or command started from, and the files it adds, so no write or command has to
 * check the table on its own. Creating a table through {@code Dataset.write} adds no data files and
 * commits directly.
 */
public final class LanceCommits {

  private LanceCommits() {}

  /** Commits {@code transaction} through {@code builder}, which was built on {@code dataset}. */
  public static Dataset execute(Dataset dataset, CommitBuilder builder, Transaction transaction) {
    BlobUtils.checkBlobV1Commit(dataset, transaction.operation());
    return builder.execute(transaction);
  }

  /**
   * Commits {@code transaction}, an overwrite that creates or replaces a table, through {@code
   * builder}. The overwrite keeps none of the table's data files, so no dataset is needed.
   */
  public static Dataset replace(CommitBuilder builder, Transaction transaction) {
    if (!(transaction.operation() instanceof Overwrite)) {
      throw new IllegalArgumentException(
          "Expected an overwrite, got " + transaction.operation().name());
    }
    BlobUtils.checkBlobV1Commit(null, transaction.operation());
    return builder.execute(transaction);
  }

  /**
   * Runs {@code commit}, a Lance call such as {@code dropIndex} that commits on {@code dataset}.
   * Lance writes the data files of such a call itself, as compaction does.
   */
  public static void run(Dataset dataset, Runnable commit) {
    BlobUtils.checkBlobV1DataFiles(dataset);
    commit.run();
  }

  /** Like {@link #run}, for a Lance call that returns a value, such as {@code createIndex}. */
  public static <T> T call(Dataset dataset, Supplier<T> commit) {
    BlobUtils.checkBlobV1DataFiles(dataset);
    return commit.get();
  }
}
