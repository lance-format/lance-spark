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
package org.lance.spark.benchmark;

public class QueryMetrics {

  private long totalTaskTimeMs;
  private long executorCpuTimeNs;
  private long jvmGcTimeMs;
  private long bytesRead;
  private long recordsRead;
  private long bytesWritten;
  private long shuffleReadBytes;
  private long shuffleWriteBytes;
  private int numTasks;
  private int numStages;

  public long getTotalTaskTimeMs() {
    return totalTaskTimeMs;
  }

  public void setTotalTaskTimeMs(long totalTaskTimeMs) {
    this.totalTaskTimeMs = totalTaskTimeMs;
  }

  public long getExecutorCpuTimeNs() {
    return executorCpuTimeNs;
  }

  public void setExecutorCpuTimeNs(long executorCpuTimeNs) {
    this.executorCpuTimeNs = executorCpuTimeNs;
  }

  public long getJvmGcTimeMs() {
    return jvmGcTimeMs;
  }

  public void setJvmGcTimeMs(long jvmGcTimeMs) {
    this.jvmGcTimeMs = jvmGcTimeMs;
  }

  public long getBytesRead() {
    return bytesRead;
  }

  public void setBytesRead(long bytesRead) {
    this.bytesRead = bytesRead;
  }

  public long getRecordsRead() {
    return recordsRead;
  }

  public void setRecordsRead(long recordsRead) {
    this.recordsRead = recordsRead;
  }

  public long getBytesWritten() {
    return bytesWritten;
  }

  public void setBytesWritten(long bytesWritten) {
    this.bytesWritten = bytesWritten;
  }

  public long getShuffleReadBytes() {
    return shuffleReadBytes;
  }

  public void setShuffleReadBytes(long shuffleReadBytes) {
    this.shuffleReadBytes = shuffleReadBytes;
  }

  public long getShuffleWriteBytes() {
    return shuffleWriteBytes;
  }

  public void setShuffleWriteBytes(long shuffleWriteBytes) {
    this.shuffleWriteBytes = shuffleWriteBytes;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public void setNumTasks(int numTasks) {
    this.numTasks = numTasks;
  }

  public int getNumStages() {
    return numStages;
  }

  public void setNumStages(int numStages) {
    this.numStages = numStages;
  }

  public String toSummaryString() {
    return String.format(
        "tasks=%d cpu=%dms gc=%dms read=%s shuffle_r=%s shuffle_w=%s",
        numTasks,
        executorCpuTimeNs / 1_000_000,
        jvmGcTimeMs,
        formatBytes(bytesRead),
        formatBytes(shuffleReadBytes),
        formatBytes(shuffleWriteBytes));
  }

  private static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + "B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.0fKB", bytes / 1024.0);
    } else if (bytes < 1024L * 1024 * 1024) {
      return String.format("%.0fMB", bytes / (1024.0 * 1024));
    } else {
      return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
    }
  }

  public String toCsvFragment() {
    return String.join(
        ",",
        String.valueOf(numTasks),
        String.valueOf(numStages),
        String.valueOf(totalTaskTimeMs),
        String.valueOf(executorCpuTimeNs / 1_000_000),
        String.valueOf(jvmGcTimeMs),
        String.valueOf(bytesRead),
        String.valueOf(recordsRead),
        String.valueOf(bytesWritten),
        String.valueOf(shuffleReadBytes),
        String.valueOf(shuffleWriteBytes));
  }

  public static String csvHeaderFragment() {
    return "num_tasks,num_stages,total_task_time_ms,cpu_time_ms,gc_time_ms,"
        + "bytes_read,records_read,bytes_written,shuffle_read_bytes,shuffle_write_bytes";
  }
}
