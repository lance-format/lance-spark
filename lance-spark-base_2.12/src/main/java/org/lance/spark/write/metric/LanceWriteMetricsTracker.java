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
package org.lance.spark.write.metric;

import org.lance.FragmentMetadata;
import org.lance.fragment.DataFile;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

import java.util.List;

/**
 * Accumulates write-path metrics on the executor side. Thread-confined (one instance per {@code
 * LanceDataWriter}, single-threaded access).
 *
 * <p>All values are <b>absolute task totals</b>, never deltas. This matters because Spark consumes
 * them absolutely on both paths: {@code SQLMetric.set} for the SQL tab, and {@code
 * OutputMetrics.setBytesWritten}/{@code setRecordsWritten} for the stage-level output metrics. Two
 * consequences:
 *
 * <ul>
 *   <li>Spark calls {@link #currentMetricsValues()} repeatedly (Spark 3.5 does so every 100 rows
 *       and again after the write loop), so repeated reporting is idempotent, not additive.
 *   <li>{@link #publishOutputMetrics()} can safely re-report the final totals after commit without
 *       double counting what {@code currentMetricsValues()} already reported.
 * </ul>
 *
 * <p>The post-commit publish exists because Spark calls {@code DataWriter.currentMetricsValues()}
 * <i>before</i> {@code DataWriter.commit()}, while a Lance fragment's byte size only becomes known
 * once its creation task resolves — which for the last fragment happens inside {@code commit()}.
 * Without it the final fragment's bytes would be dropped from every write.
 */
public class LanceWriteMetricsTracker {
  private long bytesWritten;
  private long recordsWritten;

  private final CustomTaskMetric[] taskMetrics =
      new CustomTaskMetric[] {
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceWriteMetrics.BYTES_WRITTEN;
          }

          @Override
          public long value() {
            return bytesWritten;
          }
        },
        new CustomTaskMetric() {
          @Override
          public String name() {
            return LanceWriteMetrics.RECORDS_WRITTEN;
          }

          @Override
          public long value() {
            return recordsWritten;
          }
        },
      };

  /** Counts one row handed to the writer. Rows are counted exactly, not derived from fragments. */
  public void incrementRecordsWritten() {
    recordsWritten++;
  }

  /** Adds the data-file bytes of newly completed fragments to the running total. */
  public void addFragments(List<FragmentMetadata> fragments) {
    for (FragmentMetadata fragment : fragments) {
      for (DataFile file : fragment.getFiles()) {
        Long size = file.getFileSizeBytes();
        if (size != null) {
          bytesWritten += size;
        }
      }
    }
  }

  /** Absolute task totals. Allocation-free: the metric instances are created once per tracker. */
  public CustomTaskMetric[] currentMetricsValues() {
    return taskMetrics;
  }

  /**
   * Sets the task's output metrics directly, so the totals include the fragment completed during
   * {@code commit()}. No-op outside a Spark task (e.g. unit tests).
   */
  public void publishOutputMetrics() {
    TaskContext context = TaskContext.get();
    if (context == null) {
      return;
    }
    context.taskMetrics().outputMetrics().setBytesWritten(bytesWritten);
    context.taskMetrics().outputMetrics().setRecordsWritten(recordsWritten);
  }

  /** Visible for testing. */
  public long getBytesWritten() {
    return bytesWritten;
  }

  /** Visible for testing. */
  public long getRecordsWritten() {
    return recordsWritten;
  }
}
