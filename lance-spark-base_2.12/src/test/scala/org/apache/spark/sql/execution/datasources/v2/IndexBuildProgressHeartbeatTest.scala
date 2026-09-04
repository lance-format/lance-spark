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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorMetricsUpdate, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.Optional
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

private object IndexBuildProgressHeartbeatState {
  @volatile var progressEmitted: CountDownLatch = _
  @volatile var releaseTask: CountDownLatch = _

  def reset(): Unit = {
    progressEmitted = new CountDownLatch(1)
    releaseTask = new CountDownLatch(1)
  }
}

class IndexBuildProgressHeartbeatTest {

  @Test
  def executorHeartbeatPublishesProgressBeforeTaskCompletion(): Unit = {
    IndexBuildProgressHeartbeatState.reset()
    val spark = SparkSession.builder()
      .appName("index-build-progress-heartbeat-test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.executor.heartbeatInterval", "100ms")
      .getOrCreate()
    val sc = spark.sparkContext
    val progressUpdates = SQLMetrics.createMetric(sc, "index build forward progress updates")
    val completedStages = SQLMetrics.createMetric(sc, "index build completed stages")
    assertFalse(
      progressUpdates.countFailedValues,
      "failed task attempts must not contribute to final progress activity")
    assertFalse(
      completedStages.countFailedValues,
      "failed task attempts must not contribute to final completed-stage activity")
    val observedValues = new CopyOnWriteArrayList[Long]()
    val observedProgress = new CountDownLatch(1)
    val completedTasks = new AtomicInteger(0)

    val listener = new SparkListener {
      override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
        event.accumUpdates.foreach { taskUpdate =>
          taskUpdate._4.foreach { update =>
            if (update.id == progressUpdates.id) {
              update.update.foreach { value =>
                val observed = value.asInstanceOf[Number].longValue()
                observedValues.add(observed)
                if (observed > 0L) observedProgress.countDown()
              }
            }
          }
        }
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        completedTasks.incrementAndGet()
      }
    }
    sc.addSparkListener(listener)

    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val job = Future {
      sc.parallelize(Seq(1), 1).mapPartitions { _ =>
        val progress = SparkIndexBuildProgress.forCurrentTask(
          "idx_heartbeat",
          IndexBuildProgressMetrics(progressUpdates, completedStages))
        progress.stageStart(
          "tokenize_docs",
          Optional.of(java.lang.Long.valueOf(10L)),
          "rows")
        progress.stageProgress("tokenize_docs", 1L)
        IndexBuildProgressHeartbeatState.progressEmitted.countDown()
        if (!IndexBuildProgressHeartbeatState.releaseTask.await(15, TimeUnit.SECONDS)) {
          throw new TimeoutException("driver did not release the progress test task")
        }
        progress.stageComplete("tokenize_docs")
        Iterator.single(1)
      }.collect()
    }

    try {
      assertTrue(
        IndexBuildProgressHeartbeatState.progressEmitted.await(10, TimeUnit.SECONDS),
        "executor task did not emit adapter progress")
      assertTrue(
        observedProgress.await(10, TimeUnit.SECONDS),
        s"executor heartbeat did not publish metric ${progressUpdates.id}; " +
          s"observed snapshots: ${observedValues.asScala.toSeq}")
      assertFalse(job.isCompleted, "the task must still be held open when progress is observed")
      assertEquals(
        0,
        completedTasks.get(),
        "progress must be observable while Spark's completed-task count remains unchanged")

      IndexBuildProgressHeartbeatState.releaseTask.countDown()
      assertArrayEquals(Array(1), Await.result(job, Duration(10, TimeUnit.SECONDS)))
      try {
        sc.listenerBus.waitUntilEmpty(10000)
      } catch {
        case timeout: TimeoutException =>
          fail(
            s"timed out draining listener bus; observed snapshots: " +
              s"${observedValues.asScala.toSeq}",
            timeout)
      }
    } finally {
      IndexBuildProgressHeartbeatState.releaseTask.countDown()
      sc.removeSparkListener(listener)
      spark.stop()
    }
  }
}
