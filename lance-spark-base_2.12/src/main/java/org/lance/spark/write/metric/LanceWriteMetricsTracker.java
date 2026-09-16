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
 * <p>Values are absolute task totals, not deltas. Spark consumes them absolutely on both paths
 * ({@code SQLMetric.set} and {@code OutputMetrics.setBytesWritten}), so within one attempt the
 * repeated polls and the post-commit publish cannot double count. Across attempts they can, which
 * is what {@link #clearOutputMetrics()} handles.
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

  /** Counts a row as it is handed to the writer, rather than deriving it from fragments. */
  public void incrementRecordsWritten() {
    recordsWritten++;
  }

  /** Sums known fragment data-file sizes. */
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

  /** Metric instances are allocated once per tracker, not per call. */
  public CustomTaskMetric[] currentMetricsValues() {
    return taskMetrics;
  }

  /**
   * Sets the task's output metrics directly, so the totals include the fragments that complete
   * inside {@code commit()}, after Spark's last poll. No-op outside a Spark task.
   */
  public void publishOutputMetrics() {
    setOutputMetrics(bytesWritten, recordsWritten);
  }

  /**
   * Zeroes the task's output metrics. Spark routes the reserved names into output metrics from
   * inside the write loop, and {@code AppStatusListener} folds a task's metrics into the stage
   * totals whatever its end reason, so a failed attempt would otherwise leave its partial row count
   * in stage {@code outputRecords} and the retry would add a full count on top.
   */
  public void clearOutputMetrics() {
    setOutputMetrics(0L, 0L);
  }

  private void setOutputMetrics(long bytes, long records) {
    TaskContext context = TaskContext.get();
    if (context == null) {
      return;
    }
    context.taskMetrics().outputMetrics().setBytesWritten(bytes);
    context.taskMetrics().outputMetrics().setRecordsWritten(records);
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
