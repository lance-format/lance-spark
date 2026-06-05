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
package org.lance.spark.search;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class LanceSearchScan implements Scan, Batch, Serializable {
  private static final long serialVersionUID = -120398471239847123L;

  private final StructType schema;
  private final LanceSearchQuery query;

  public LanceSearchScan(StructType schema, LanceSearchQuery query) {
    this.schema = schema;
    this.query = query;
  }

  @Override
  public StructType readSchema() {
    return schema;
  }

  @Override
  public String description() {
    return "LanceSearchScan";
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return new InputPartition[] {new LanceSearchInputPartition(schema, query)};
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceSearchPartitionReaderFactory();
  }
}
