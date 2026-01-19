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
package org.lance.spark.partition;

import org.lance.spark.LanceIdentifier;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class LanceSparkPartitionedDataSource implements SupportsCatalogOptions, DataSourceRegister {
  public static final String name = "lance";

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    LancePartitionConfig config = LancePartitionConfig.from(options);
    try {
      PartitionedLanceDataset dataset = new PartitionedLanceDataset(config);
      return dataset.schema();
    } catch (NoSuchTableException e) {
      return null;
    }
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    LancePartitionConfig config = LancePartitionConfig.from(properties);
    PartitionedLanceDataset dataset;
    try {
      dataset = new PartitionedLanceDataset(config);
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Table not found.", e);
    }

    StructType realSchema = dataset.schema();
    for (StructField field : schema.fields()) {
      StructField realField = realSchema.fields()[realSchema.fieldIndex(field.name())];
      if (field.dataType() != realField.dataType()) {
        throw new RuntimeException(
            String.format(
                "Schema inconsistent with dataset. input=%s, dataset=%s", schema, realSchema));
      }
    }

    return dataset;
  }

  @Override
  public String shortName() {
    return name;
  }

  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    return new LanceIdentifier(LancePartitionConfig.from(options).location());
  }

  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    return "lance";
  }
}
