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
 * <p>All values are absolute task totals, never deltas, because Spark consumes them absolutely on
 * both paths ({@code SQLMetric.set} and {@code OutputMetrics.setBytesWritten}/{@code
 * setRecordsWritten}). Spark polls {@link #currentMetricsValues()} repeatedly during the write
 * loop, and {@link #publishOutputMetrics()} re-reports the final totals after commit; neither can
 * double count.
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

  /** Counts one row handed to the writer, rather than deriving the count from fragments. */
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

  /** Absolute task totals. The metric instances are allocated once per tracker, not per call. */
  public CustomTaskMetric[] currentMetricsValues() {
    return taskMetrics;
  }

  /**
   * Sets the task's output metrics directly, so the totals include the fragment completed during
   * {@code commit()}, after Spark's last {@code currentMetricsValues()} poll. No-op outside a Spark
   * task.
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
