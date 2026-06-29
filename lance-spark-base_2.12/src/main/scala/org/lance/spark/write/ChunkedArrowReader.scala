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
package org.lance.spark.write

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema

/**
 * An [[ArrowReader]] that emits a fixed sequence of pre-built [[VectorSchemaRoot]] batches, one
 * per [[loadNextBatch]] call. Generalizes [[SingleBatchArrowReader]] from one batch to N so an
 * index build can hand a partition to lance-core split across several sub-2-GiB Arrow batches
 * rather than one, sidestepping Arrow's 32-bit-offset (2 GiB) cap on variable-width columns.
 * lance-core consumes the whole stream per index, so the built index matches a single-batch
 * handoff.
 *
 * Index-type agnostic: any build that pre-processes rows into Arrow batches can use it. The
 * batches and `schema` are owned by the caller (mirroring [[SingleBatchArrowReader]]); this
 * reader closes neither.
 */
class ChunkedArrowReader(
    allocator: BufferAllocator,
    schema: Schema,
    batches: Array[VectorSchemaRoot])
  extends ArrowReader(allocator) {

  private var nextBatch: Int = 0

  override protected def readSchema(): Schema = schema

  override def loadNextBatch(): Boolean = {
    if (nextBatch >= batches.length) {
      return false
    }
    prepareLoadNextBatch()
    val source = batches(nextBatch)
    val recordBatch = new VectorUnloader(source).getRecordBatch()
    try {
      new VectorLoader(getVectorSchemaRoot()).load(recordBatch)
    } finally {
      recordBatch.close()
    }
    nextBatch += 1
    true
  }

  override def bytesRead(): Long = 0L

  override protected def closeReadSource(): Unit = {}
}
