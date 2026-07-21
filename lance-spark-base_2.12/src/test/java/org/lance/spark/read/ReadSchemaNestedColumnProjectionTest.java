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

import org.lance.spark.LanceConstant;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReadSchemaNestedColumnProjectionTest {
  @Test
  public void shouldReturnEmptyProjectionForNullRequiredSchema() {
    StructType fullSchema = new StructType().add("record_id", DataTypes.IntegerType);

    assertEquals(
        Collections.emptyList(),
        ReadSchemaNestedColumnProjection.buildProjectedColumns(null, fullSchema));
  }

  @Test
  public void shouldProjectNestedStructSubsetAsLeafPaths() {
    StructType fullSchema =
        new StructType()
            .add("record_id", DataTypes.IntegerType)
            .add(
                "usage_metrics",
                new StructType()
                    .add("auxiliary_count", DataTypes.LongType)
                    .add("token_total", DataTypes.LongType),
                false)
            .add("business_domain", DataTypes.StringType);
    StructType requiredSchema =
        new StructType()
            .add("usage_metrics", new StructType().add("token_total", DataTypes.LongType), false)
            .add("business_domain", DataTypes.StringType);

    assertEquals(
        Arrays.asList("usage_metrics.token_total", "business_domain"),
        ReadSchemaNestedColumnProjection.buildProjectedColumns(requiredSchema, fullSchema));
  }

  @Test
  public void shouldProjectDeeplyNestedStructSubsetAsLeafPaths() {
    StructType fullSchema =
        new StructType()
            .add(
                "profile",
                new StructType()
                    .add(
                        "usage_metrics",
                        new StructType()
                            .add("token_prompt", DataTypes.LongType)
                            .add("token_total", DataTypes.LongType),
                        false)
                    .add("region", DataTypes.StringType),
                false);
    StructType requiredSchema =
        new StructType()
            .add(
                "profile",
                new StructType()
                    .add(
                        "usage_metrics",
                        new StructType().add("token_total", DataTypes.LongType),
                        false),
                false);

    assertEquals(
        Collections.singletonList("profile.usage_metrics.token_total"),
        ReadSchemaNestedColumnProjection.buildProjectedColumns(requiredSchema, fullSchema));
  }

  @Test
  public void shouldQuoteNestedFieldPathParts() {
    StructType fullSchema =
        new StructType()
            .add(
                "usage.metrics",
                new StructType()
                    .add("token.total", DataTypes.LongType)
                    .add("token_prompt", DataTypes.LongType),
                false);
    StructType requiredSchema =
        new StructType()
            .add("usage.metrics", new StructType().add("token.total", DataTypes.LongType), false);

    assertEquals(
        Collections.singletonList("`usage.metrics`.`token.total`"),
        ReadSchemaNestedColumnProjection.buildProjectedColumns(requiredSchema, fullSchema));
  }

  @Test
  public void shouldKeepFullStructProjectionAsTopLevelColumn() {
    StructType fullSchema =
        new StructType()
            .add("record_id", DataTypes.IntegerType)
            .add(
                "usage_metrics",
                new StructType()
                    .add("auxiliary_count", DataTypes.LongType)
                    .add("token_total", DataTypes.LongType));

    assertEquals(
        Arrays.asList("record_id", "usage_metrics"),
        ReadSchemaNestedColumnProjection.buildProjectedColumns(fullSchema, fullSchema));
  }

  @Test
  public void shouldKeepNullableStructSubsetAsTopLevelProjection() {
    StructType fullSchema =
        new StructType()
            .add("record_id", DataTypes.IntegerType)
            .add(
                "usage_metrics",
                new StructType()
                    .add("auxiliary_count", DataTypes.LongType)
                    .add("token_total", DataTypes.LongType),
                true);
    StructType requiredSchema =
        new StructType()
            .add("usage_metrics", new StructType().add("token_total", DataTypes.LongType), true);

    assertEquals(
        Collections.singletonList("usage_metrics"),
        ReadSchemaNestedColumnProjection.buildProjectedColumns(requiredSchema, fullSchema));
  }

  @Test
  public void shouldLeaveArrayOfStructAsTopLevelProjection() {
    // Nested child pruning currently only expands plain StructType projections. Array/Map
    // containers remain top-level so the scanner behavior stays explicit and predictable.
    StructType fullSchema =
        new StructType()
            .add(
                "event_groups",
                DataTypes.createArrayType(
                    new StructType()
                        .add("reserved_count", DataTypes.LongType)
                        .add("total_events", DataTypes.LongType)));
    StructType requiredSchema =
        new StructType()
            .add(
                "event_groups",
                DataTypes.createArrayType(
                    new StructType().add("total_events", DataTypes.LongType)));

    assertEquals(
        Collections.singletonList("event_groups"),
        ReadSchemaNestedColumnProjection.buildProjectedColumns(requiredSchema, fullSchema));
  }

  @Test
  public void shouldSkipScannerSpecialFields() {
    StructType schema =
        new StructType()
            .add("record_id", DataTypes.IntegerType)
            .add(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType)
            .add(LanceConstant.ROW_ID, DataTypes.LongType)
            .add(LanceConstant.ROW_ADDRESS, DataTypes.LongType)
            .add(LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType)
            .add(LanceConstant.ROW_LAST_UPDATED_AT_VERSION, DataTypes.LongType)
            .add("payload" + LanceConstant.BLOB_POSITION_SUFFIX, DataTypes.LongType)
            .add("payload" + LanceConstant.BLOB_SIZE_SUFFIX, DataTypes.LongType);

    assertEquals(
        Collections.singletonList("record_id"),
        ReadSchemaNestedColumnProjection.buildProjectedColumns(schema, schema));
  }

  @Test
  public void shouldFailWhenRequiredFieldIsMissingFromFullSchema() {
    StructType fullSchema = new StructType().add("record_id", DataTypes.IntegerType);
    StructType requiredSchema = new StructType().add("missing_field", DataTypes.StringType);

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ReadSchemaNestedColumnProjection.buildProjectedColumns(requiredSchema, fullSchema));

    assertEquals(
        "Required projection column 'missing_field' with type string does not exist in the full schema",
        error.getMessage());
  }
}
