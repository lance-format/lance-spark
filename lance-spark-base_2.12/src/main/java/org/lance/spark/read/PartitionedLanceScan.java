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

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Scan that aggregates multiple child LanceScan instances. */
public class PartitionedLanceScan implements Batch, Scan, Serializable {
  private static final long serialVersionUID = 77452349857239857L;

  private final StructType schema;
  private final List<LanceScan> childScans;

  public PartitionedLanceScan(StructType schema, List<LanceScan> childScans) {
    this.schema = schema;
    this.childScans = childScans == null ? new ArrayList<>() : childScans;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<InputPartition> parts = new ArrayList<>();
    for (LanceScan cs : childScans) {
      for (InputPartition p : cs.planInputPartitions()) {
        parts.add(p);
      }
    }
    return parts.toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceReaderFactory();
  }

  @Override
  public StructType readSchema() {
    return schema;
  }
}
