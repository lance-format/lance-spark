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
package org.lance.spark.sharding;

import org.lance.Dataset;
import org.lance.index.scalar.ZoneStats;
import org.lance.memwal.InitializeMemWalParams;
import org.lance.memwal.MemWalIndexDetails;
import org.lance.memwal.ShardingField;
import org.lance.memwal.ShardingSpec;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.lance.spark.utils.BucketHashUtil;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Spark-facing helpers for Lance MemWAL sharding specs. */
public final class SparkLanceShardingUtils {
  private static final int DEFAULT_SPEC_ID = 0;

  private SparkLanceShardingUtils() {}

  public static boolean isEmpty(ShardingSpec spec) {
    return spec == null || spec.fields().isEmpty();
  }

  public static ShardingSpec firstShardingSpec(Dataset dataset) {
    Optional<MemWalIndexDetails> details = dataset.memWalIndexDetails();
    if (details.isPresent() && !details.get().shardingSpecs().isEmpty()) {
      return details.get().shardingSpecs().get(0);
    }
    return null;
  }

  public static ShardingSpec fromSparkTransforms(Transform[] transforms) {
    return fromSparkTransforms(transforms, null);
  }

  public static ShardingSpec fromSparkTransforms(Transform[] transforms, LanceSchema schema) {
    if (transforms == null || transforms.length == 0) {
      return null;
    }

    List<ShardingField> fields = new ArrayList<>();
    for (Transform transform : transforms) {
      fields.add(fromSparkTransform(transform, schema));
    }
    return new ShardingSpec(DEFAULT_SPEC_ID, fields);
  }

  public static void initializeMemWal(Dataset dataset, ShardingSpec spec) {
    if (isEmpty(spec) || dataset.memWalIndexDetails().isPresent()) {
      return;
    }
    if (spec.fields().size() > 1) {
      throw new UnsupportedOperationException(
          "Lance MemWAL sharding supports one Spark sharding field, got: " + spec.fields().size());
    }

    LanceSchema schema = dataset.getLanceSchema();
    ShardingField field = spec.fields().get(0);
    String transform = field.transform().orElse(null);
    String column = columnName(field, schema);
    InitializeMemWalParams params = new InitializeMemWalParams();
    if ("bucket".equals(transform)) {
      params.withBucketSharding(column, numBuckets(field));
    } else if ("identity".equals(transform)) {
      params.withIdentitySharding(column);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported MemWAL sharding transform: " + transform);
    }
    dataset.initializeMemWal(params);
  }

  public static List<ShardingField> fields(ShardingSpec spec) {
    return isEmpty(spec) ? Collections.emptyList() : spec.fields();
  }

  public static NamedReference toClusteringRef(ShardingField field, LanceSchema schema) {
    return Expressions.column(columnName(field, schema));
  }

  public static SortOrder toSortOrder(ShardingField field, LanceSchema schema) {
    String column = columnName(field, schema);
    return Expressions.sort(
        Expressions.column(column), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
  }

  public static Expression toSparkExpression(ShardingField field, LanceSchema schema) {
    String transform = field.transform().orElse(null);
    String column = columnName(field, schema);
    if ("bucket".equals(transform)) {
      return Expressions.bucket(numBuckets(field), column);
    } else if ("identity".equals(transform)) {
      return FieldReference.apply(column);
    }
    throw new UnsupportedOperationException("Unsupported sharding transform: " + transform);
  }

  public static Optional<Map<Integer, Object>> detectFragmentKeys(
      ShardingField field, LanceSchema schema, List<ZoneStats> zones) {
    columnName(field, schema);
    Map<Integer, Object> result = new HashMap<>();
    for (ZoneStats zone : zones) {
      result.putIfAbsent(zone.getFragmentId(), null);
    }
    for (int fragmentId : new ArrayList<>(result.keySet())) {
      Optional<Object> key = fragmentKeyFromZones(field, schema, zones, fragmentId);
      if (!key.isPresent()) {
        return Optional.empty();
      }
      result.put(fragmentId, key.get());
    }
    return Optional.of(result);
  }

  public static InternalRow partitionKeyRow(Object value) {
    Object sparkValue = value instanceof String ? UTF8String.fromString((String) value) : value;
    return new GenericInternalRow(new Object[] {sparkValue});
  }

  public static int partitionCount(Map<Integer, Object> fragmentKeys) {
    return fragmentKeys.size();
  }

  public static String columnName(ShardingField field, LanceSchema schema) {
    if (!field.sourceIds().isEmpty()) {
      if (schema == null) {
        throw new IllegalArgumentException(
            "MemWAL sharding field "
                + field.fieldId()
                + " requires Lance schema to resolve source field ids");
      }
      String resolved = columnNameByFieldId(schema.fields(), field.sourceIds().get(0));
      if (resolved != null && !resolved.trim().isEmpty()) {
        return resolved;
      }
      throw new IllegalArgumentException(
          "MemWAL sharding field "
              + field.fieldId()
              + " references missing Lance field id "
              + field.sourceIds().get(0));
    }
    String column = field.parameters().get("column");
    if (column != null && !column.trim().isEmpty()) {
      if (schema != null) {
        LanceField resolved = fieldByColumnName(schema.fields(), column);
        if (resolved == null) {
          throw new IllegalArgumentException("Missing Lance field for sharding column: " + column);
        }
        String canonical = columnNameByFieldId(schema.fields(), resolved.getId());
        if (canonical != null && !canonical.trim().isEmpty()) {
          return canonical;
        }
      }
      return column;
    }
    throw new IllegalArgumentException(
        "MemWAL sharding field " + field.fieldId() + " missing source column");
  }

  private static ShardingField fromSparkTransform(Transform transform, LanceSchema schema) {
    if (transform instanceof BucketTransform) {
      BucketTransform bucketTransform = (BucketTransform) transform;
      int numBuckets = (int) bucketTransform.numBuckets().value();
      if (numBuckets <= 0) {
        throw new IllegalArgumentException(
            "Number of buckets must be positive, got: " + numBuckets);
      }
      List<NamedReference> columns = JavaConverters.seqAsJavaList(bucketTransform.columns());
      if (columns.size() > 1) {
        throw new UnsupportedOperationException(
            "Lance only supports bucketing on a single column, got: " + columns);
      }
      String column = String.join(".", columns.get(0).fieldNames());
      Map<String, String> parameters = new HashMap<>();
      parameters.put("column", column);
      parameters.put("num_buckets", Integer.toString(numBuckets));
      String fieldId = "bucket(" + numBuckets + ", " + column + ")";
      return new ShardingField(
          fieldId, sourceIds(column, schema), "bucket", fieldId, "int32", parameters);
    } else if (transform instanceof IdentityTransform) {
      IdentityTransform identityTransform = (IdentityTransform) transform;
      String column = String.join(".", identityTransform.ref().fieldNames());
      Map<String, String> parameters = new HashMap<>();
      parameters.put("column", column);
      String fieldId = "identity(" + column + ")";
      return new ShardingField(
          fieldId, sourceIds(column, schema), "identity", fieldId, "utf8", parameters);
    }
    throw new UnsupportedOperationException(
        "Unsupported Spark sharding input: "
            + transform.describe()
            + ". Lance supports bucket(N, col) and identity(col).");
  }

  private static Optional<Object> fragmentKeyFromZones(
      ShardingField field, LanceSchema schema, List<ZoneStats> zones, int fragmentId) {
    Comparable<?> value = null;
    for (ZoneStats zone : zones) {
      if (zone.getFragmentId() != fragmentId) {
        continue;
      }
      Comparable<?> min = zone.getMin();
      Comparable<?> max = zone.getMax();
      if (min == null || max == null || !min.equals(max)) {
        return Optional.empty();
      }
      if (value != null && !value.equals(min)) {
        return Optional.empty();
      }
      value = min;
    }
    if (value == null) {
      return Optional.empty();
    }
    if ("bucket".equals(field.transform().orElse(null))) {
      return Optional.of(BucketHashUtil.computeBucketIdFromValue(value, numBuckets(field)));
    }
    if ("identity".equals(field.transform().orElse(null))) {
      return Optional.of(value);
    }
    throw new UnsupportedOperationException(
        "Unsupported sharding transform: " + field.transform().orElse(null));
  }

  private static int numBuckets(ShardingField field) {
    String value = field.parameters().get("num_buckets");
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "MemWAL sharding field " + field.fieldId() + " missing parameter num_buckets");
    }
    return Integer.parseInt(value);
  }

  private static List<Integer> sourceIds(String column, LanceSchema schema) {
    if (schema == null) {
      return Collections.emptyList();
    }
    LanceField field = fieldByColumnName(schema.fields(), column);
    if (field == null) {
      throw new IllegalArgumentException("Missing Lance field for sharding column: " + column);
    }
    return Collections.singletonList(field.getId());
  }

  private static LanceField fieldByColumnName(List<LanceField> fields, String column) {
    for (LanceField field : fields) {
      LanceField match = fieldByColumnName(field, column, "");
      if (match != null) {
        return match;
      }
    }
    return null;
  }

  private static LanceField fieldByColumnName(LanceField field, String column, String prefix) {
    String fullName = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
    if (fullName.equals(column)) {
      return field;
    }
    for (LanceField child : field.getChildren()) {
      LanceField match = fieldByColumnName(child, column, fullName);
      if (match != null) {
        return match;
      }
    }
    return null;
  }

  private static String columnNameByFieldId(List<LanceField> fields, int fieldId) {
    for (LanceField field : fields) {
      String name = columnNameByFieldId(field, fieldId, "");
      if (name != null) {
        return name;
      }
    }
    return null;
  }

  private static String columnNameByFieldId(LanceField field, int fieldId, String prefix) {
    String fullName = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
    if (field.getId() == fieldId) {
      return fullName;
    }
    for (LanceField child : field.getChildren()) {
      String name = columnNameByFieldId(child, fieldId, fullName);
      if (name != null) {
        return name;
      }
    }
    return null;
  }
}
