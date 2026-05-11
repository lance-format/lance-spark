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

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadSchemaNestedStructWideningTest {

  /**
   * Simulates selecting only the second nested field from a struct. The widened schema must restore
   * both fields in table order so that {@code LanceStructAccessor} child ordinals remain correct.
   */
  @Test
  public void widensSubsetNestedStructToFullTableFieldOrderAndFields() {
    StructType fullMetadataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("first", DataTypes.StringType, true),
              DataTypes.createStructField("second", DataTypes.LongType, true),
            });
    StructType fullTableSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("metadata", fullMetadataSchema, true)});

    StructType prunedMetadataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("second", DataTypes.LongType, true),
            });
    StructType prunedTableSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("metadata", prunedMetadataSchema, true)
            });

    StructType widenedSchema =
        ReadSchemaNestedStructWidening.widenRequiredSchema(prunedTableSchema, fullTableSchema);

    assertEquals(fullTableSchema, widenedSchema);
  }

  /**
   * Verifies that widening recurses into array element types: a pruned struct inside an array
   * column is restored to the full struct so Arrow child ordinals are preserved.
   */
  @Test
  public void widensSubsetStructNestedInsideArrayElementType() {
    StructType fullEventSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("eventName", DataTypes.StringType, true),
              DataTypes.createStructField("eventTimestamp", DataTypes.LongType, true),
            });
    StructType prunedEventSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("eventTimestamp", DataTypes.LongType, true),
            });

    StructType fullTableSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("events", new ArrayType(fullEventSchema, true), true),
            });
    StructType prunedTableSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("events", new ArrayType(prunedEventSchema, true), true),
            });

    StructType widenedSchema =
        ReadSchemaNestedStructWidening.widenRequiredSchema(prunedTableSchema, fullTableSchema);

    assertEquals(fullTableSchema, widenedSchema);
  }

  /**
   * Top-level column pruning (dropping entire columns) must not be reversed: if a query selects
   * only {@code userId} from a table with {@code userId} and {@code sessionId}, the widened schema
   * should still contain only {@code userId}. Lance handles top-level projection natively.
   */
  @Test
  public void doesNotWidenTopLevelColumnPruning() {
    StructType fullTableSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("userId", DataTypes.LongType, true),
              DataTypes.createStructField("sessionId", DataTypes.LongType, true),
            });
    StructType prunedTableSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("userId", DataTypes.LongType, true),
            });

    StructType widenedSchema =
        ReadSchemaNestedStructWidening.widenRequiredSchema(prunedTableSchema, fullTableSchema);

    assertEquals(prunedTableSchema, widenedSchema);
  }

  /**
   * When the required schema contains a field not present in the table schema (e.g. schema
   * evolution produced a diverged field name), the nested struct is left unchanged rather than
   * widened, avoiding silent data corruption.
   */
  @Test
  public void doesNotWidenWhenRequiredFieldIsAbsentFromTableSchema() {
    StructType fullMetadataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("first", DataTypes.StringType, true),
              DataTypes.createStructField("second", DataTypes.LongType, true),
            });
    StructType fullTableSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("metadata", fullMetadataSchema, true)});

    // requiredField "unknownField" does not exist in the table struct
    StructType divergedMetadataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("unknownField", DataTypes.StringType, true),
            });
    StructType divergedTableSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("metadata", divergedMetadataSchema, true)
            });

    StructType widenedSchema =
        ReadSchemaNestedStructWidening.widenRequiredSchema(divergedTableSchema, fullTableSchema);

    assertEquals(divergedTableSchema, widenedSchema);
  }
}
