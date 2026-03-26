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

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class QueryMetricsListener extends SparkListener {

  private String currentJobGroup;
  private final Set<Integer> activeJobIds = new HashSet<>();
  private final Set<Integer> completedStageIds = new HashSet<>();

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

  public synchronized void reset(String jobGroup) {
    this.currentJobGroup = jobGroup;
    activeJobIds.clear();
    completedStageIds.clear();
    totalTaskTimeMs = 0;
    executorCpuTimeNs = 0;
    jvmGcTimeMs = 0;
    bytesRead = 0;
    recordsRead = 0;
    bytesWritten = 0;
    shuffleReadBytes = 0;
    shuffleWriteBytes = 0;
    numTasks = 0;
    numStages = 0;
  }

  @Override
  public synchronized void onJobStart(SparkListenerJobStart jobStart) {
    Properties props = jobStart.properties();
    if (props != null) {
      String group = props.getProperty("spark.jobGroup.id");
      if (group != null && group.equals(currentJobGroup)) {
        activeJobIds.add(jobStart.jobId());
      }
    }
  }

  @Override
  public synchronized void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    int stageId = stageCompleted.stageInfo().stageId();
    if (!completedStageIds.contains(stageId)) {
      completedStageIds.add(stageId);
      numStages++;
    }
  }

  @Override
  public synchronized void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    if (!activeJobIds.contains(taskEnd.stageId())) {
      // We track by stage association with active jobs indirectly.
      // Since stage IDs aren't directly tied to job IDs in the task end event,
      // we accept all task ends while we have active jobs.
    }
    if (activeJobIds.isEmpty()) {
      return;
    }

    TaskMetrics metrics = taskEnd.taskMetrics();
    if (metrics == null) {
      return;
    }

    numTasks++;
    totalTaskTimeMs += metrics.executorRunTime();
    executorCpuTimeNs += metrics.executorCpuTime();
    jvmGcTimeMs += metrics.jvmGCTime();
    bytesRead += metrics.inputMetrics().bytesRead();
    recordsRead += metrics.inputMetrics().recordsRead();
    bytesWritten += metrics.outputMetrics().bytesWritten();
    shuffleReadBytes += metrics.shuffleReadMetrics().totalBytesRead();
    shuffleWriteBytes += metrics.shuffleWriteMetrics().bytesWritten();
  }

  public synchronized QueryMetrics getMetrics() {
    QueryMetrics m = new QueryMetrics();
    m.setTotalTaskTimeMs(totalTaskTimeMs);
    m.setExecutorCpuTimeNs(executorCpuTimeNs);
    m.setJvmGcTimeMs(jvmGcTimeMs);
    m.setBytesRead(bytesRead);
    m.setRecordsRead(recordsRead);
    m.setBytesWritten(bytesWritten);
    m.setShuffleReadBytes(shuffleReadBytes);
    m.setShuffleWriteBytes(shuffleWriteBytes);
    m.setNumTasks(numTasks);
    m.setNumStages(numStages);
    return m;
  }
}
