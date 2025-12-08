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

import org.lance.spark.LanceConfig;

import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.types.StructType;

public class SparkPositionDeltaWriteBuilder implements DeltaWriteBuilder {
  private final StructType sparkSchema;
  private final LanceConfig config;

  public SparkPositionDeltaWriteBuilder(StructType sparkSchema, LanceConfig config) {
    this.sparkSchema = sparkSchema;
    this.config = config;
  }

  public DeltaWrite build() {
    return new SparkPositionDeltaWrite(sparkSchema, config);
  }
}
