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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Objects;

/**
 * An ArrowReader implementation that provides a single ArrowRecordBatch from a given
 * VectorSchemaRoot.
 *
 * <p>This reader will return the batch exactly once when {@link #loadNextBatch()} is called, and
 * subsequent calls will return false indicating no more batches are available.
 */
public class SingleBatchArrowReader extends ArrowReader {
  private final VectorSchemaRoot source;
  private boolean returned = false;

  public SingleBatchArrowReader(BufferAllocator allocator, VectorSchemaRoot source) {
    super(allocator);
    this.source = Objects.requireNonNull(source, "source must not be null");
  }

  @Override
  protected Schema readSchema() {
    return source.getSchema();
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    if (returned) {
      return false;
    }

    super.prepareLoadNextBatch();

    try (ArrowRecordBatch rb = new VectorUnloader(source).getRecordBatch()) {
      new VectorLoader(getVectorSchemaRoot()).load(rb);
    }

    returned = true;
    return true;
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() {}
}
