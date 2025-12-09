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
package org.lance.spark.read;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.LocalScan;
import org.apache.spark.sql.types.StructType;

/**
 * A LocalScan implementation for Lance that returns pre-computed results without scanning data.
 * This is used for metadata-based operations like COUNT(*) without filters.
 *
 * <p>LocalScan executes on the driver only, avoiding distributed execution overhead for simple
 * metadata lookups.
 */
public class LanceLocalScan implements LocalScan {
  private final StructType schema;
  private final InternalRow[] rows;
  private final String datasetUri;

  public LanceLocalScan(StructType schema, InternalRow[] rows, String datasetUri) {
    this.schema = schema;
    this.rows = rows;
    this.datasetUri = datasetUri;
  }

  @Override
  public InternalRow[] rows() {
    return rows;
  }

  @Override
  public StructType readSchema() {
    return schema;
  }

  @Override
  public String description() {
    return String.format("LanceLocalScan[%s]", datasetUri);
  }

  @Override
  public String toString() {
    return String.format("LanceLocalScan(uri=%s, schema=%s)", datasetUri, schema);
  }
}
