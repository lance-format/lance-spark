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
import org.lance.spark.LanceConstant;
import org.lance.spark.utils.BucketHashUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Spark-facing adapter for one Lance MemWAL sharding field. */
public abstract class SparkLanceShardingAdapter implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String transform;
  private final String col;

  protected SparkLanceShardingAdapter(String transform, String col) {
    this.transform = transform;
    this.col = col;
  }

  @JsonProperty("transform")
  public String getTransform() {
    return transform;
  }

  @JsonProperty("col")
  public String getCol() {
    return col;
  }

  public abstract Expression toSparkExpression();

  public abstract SortOrder toSortOrder();

  public abstract NamedReference toClusteringRef();

  /**
   * Computes the partition key for a fragment from zonemap stats. Returns empty if the fragment is
   * not partition-compatible (e.g. min != max for identity).
   */
  public abstract Optional<Object> fragmentKeyFromZones(List<ZoneStats> zones, int fragmentId);

  /** Builds an {@link InternalRow} partition key from a computed partition value. */
  public InternalRow partitionKeyRow(Object value) {
    Object sparkValue = toSparkValue(value);
    return new GenericInternalRow(new Object[] {sparkValue});
  }

  /**
   * Returns the number of distinct partition values across all fragments, given a fragment→key map.
   */
  public int partitionCount(Map<Integer, Object> fragmentKeys) {
    return fragmentKeys.size();
  }

  private static Object toSparkValue(Object value) {
    if (value instanceof String) {
      return UTF8String.fromString((String) value);
    }
    return value;
  }

  @JsonCreator
  public static SparkLanceShardingAdapter fromJson(
      @JsonProperty("transform") String transform,
      @JsonProperty("col") String col,
      @JsonProperty("num_buckets") Integer numBuckets) {
    switch (transform) {
      case "identity":
        return new Identity(col);
      case "bucket":
        if (numBuckets == null || numBuckets <= 0) {
          throw new IllegalArgumentException("bucket sharding requires positive num_buckets");
        }
        return new Bucket(col, numBuckets);
      default:
        throw new UnsupportedOperationException("Unsupported sharding transform: " + transform);
    }
  }

  /** Identity sharding adapter: data grouped by raw column value. */
  public static final class Identity extends SparkLanceShardingAdapter {
    private static final long serialVersionUID = 1L;

    public Identity(String col) {
      super("identity", col);
    }

    @Override
    public Expression toSparkExpression() {
      return FieldReference.apply(getCol());
    }

    @Override
    public SortOrder toSortOrder() {
      return Expressions.sort(
          Expressions.column(getCol()), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    }

    @Override
    public NamedReference toClusteringRef() {
      return Expressions.column(getCol());
    }

    @Override
    public Optional<Object> fragmentKeyFromZones(List<ZoneStats> zones, int fragmentId) {
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
      return Optional.ofNullable(value);
    }
  }

  /** Bucket sharding adapter: data grouped by hash(col) % N. */
  public static final class Bucket extends SparkLanceShardingAdapter {
    private static final long serialVersionUID = 1L;

    private final int numBuckets;

    public Bucket(String col, int numBuckets) {
      super("bucket", col);
      this.numBuckets = numBuckets;
    }

    @JsonProperty("num_buckets")
    public int getNumBuckets() {
      return numBuckets;
    }

    @Override
    public Expression toSparkExpression() {
      return Expressions.bucket(numBuckets, getCol());
    }

    @Override
    public SortOrder toSortOrder() {
      // Sort by raw column value; rows with same bucket ID
      // but different values may not be contiguous.
      // TODO: sort by computed bucket ID for optimal
      // fragment sizing.
      return Expressions.sort(
          Expressions.column(getCol()), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    }

    @Override
    public NamedReference toClusteringRef() {
      return Expressions.column(getCol());
    }

    @Override
    public Optional<Object> fragmentKeyFromZones(List<ZoneStats> zones, int fragmentId) {
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
      int bucketId = BucketHashUtil.computeBucketIdFromValue(value, numBuckets);
      return Optional.of(bucketId);
    }
  }

  public static String toJson(List<SparkLanceShardingAdapter> spec) {
    try {
      return MAPPER.writeValueAsString(spec);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize Spark sharding adapter spec", e);
    }
  }

  public static List<SparkLanceShardingAdapter> fromJsonString(String json) {
    if (json == null || json.trim().isEmpty()) {
      return Collections.emptyList();
    }
    try {
      return MAPPER.readValue(
          json,
          MAPPER
              .getTypeFactory()
              .constructCollectionType(List.class, SparkLanceShardingAdapter.class));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse Spark sharding adapter spec: " + json, e);
    }
  }

  public static ShardingSpec toShardingSpec(List<SparkLanceShardingAdapter> spec) {
    List<ShardingField> fields = new ArrayList<>();
    for (SparkLanceShardingAdapter adapter : spec) {
      fields.add(adapter.toShardingField());
    }
    return new ShardingSpec(0, fields);
  }

  public static String toShardingSpecJson(List<SparkLanceShardingAdapter> spec) {
    return toJson(toShardingSpec(spec));
  }

  public static List<SparkLanceShardingAdapter> fromShardingSpecJson(String json) {
    if (json == null || json.trim().isEmpty()) {
      return Collections.emptyList();
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      ShardingSpec shardingSpec = fromJson(root);
      return fromShardingSpec(shardingSpec, id -> null);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse MemWAL sharding spec: " + json, e);
    }
  }

  public static List<SparkLanceShardingAdapter> fromShardingSpec(
      ShardingSpec shardingSpec, Function<Integer, String> sourceIdToColumn) {
    List<SparkLanceShardingAdapter> spec = new ArrayList<>();
    for (ShardingField field : shardingSpec.fields()) {
      spec.add(fromShardingField(field, sourceIdToColumn));
    }
    return spec;
  }

  /** Converts Spark catalog transforms into Spark-facing Lance sharding adapters. */
  public static List<SparkLanceShardingAdapter> toSpec(Transform[] transforms) {
    if (transforms == null || transforms.length == 0) {
      return Collections.emptyList();
    }

    List<SparkLanceShardingAdapter> spec = new ArrayList<>();

    for (Transform t : transforms) {
      if (t instanceof BucketTransform) {
        BucketTransform bt = (BucketTransform) t;
        int numBuckets = (int) bt.numBuckets().value();
        if (numBuckets <= 0) {
          throw new IllegalArgumentException(
              "Number of buckets must be positive, got: " + numBuckets);
        }
        List<NamedReference> cols = JavaConverters.seqAsJavaList(bt.columns());
        if (cols.size() > 1) {
          throw new UnsupportedOperationException(
              "Lance only supports bucketing on a single column, got: " + cols);
        }
        String colName = String.join(".", cols.get(0).fieldNames());
        spec.add(new Bucket(colName, numBuckets));
      } else if (t instanceof IdentityTransform) {
        IdentityTransform it = (IdentityTransform) t;
        String colName = String.join(".", it.ref().fieldNames());
        spec.add(new Identity(colName));
      } else {
        throw new UnsupportedOperationException(
            "Unsupported Spark sharding adapter input: "
                + t.describe()
                + ". Lance supports bucket(N, col) and identity(col).");
      }
    }

    return spec;
  }

  /**
   * Parses sharding adapters from MemWAL index metadata, falling back to older partition properties
   * for backward compatibility.
   */
  public static List<SparkLanceShardingAdapter> parseSpec(
      Dataset dataset, Map<String, String> tableProperties) {
    Optional<MemWalIndexDetails> details = dataset.memWalIndexDetails();
    if (details.isPresent() && !details.get().shardingSpecs().isEmpty()) {
      return fromMemWalIndexDetails(dataset, details.get());
    }
    return parseSpec(tableProperties);
  }

  /** Parses legacy partitioning table properties for backward compatibility. */
  public static List<SparkLanceShardingAdapter> parseSpec(Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return Collections.emptyList();
    }

    String shardingSpecJson = tableProperties.get(LanceConstant.TABLE_OPT_SHARDING_SPEC);
    if (shardingSpecJson != null && !shardingSpecJson.trim().isEmpty()) {
      return fromShardingSpecJson(shardingSpecJson);
    }

    String specJson = tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_SPEC);
    if (specJson != null && !specJson.trim().isEmpty()) {
      return fromJsonString(specJson);
    }

    List<SparkLanceShardingAdapter> spec = new ArrayList<>();

    String bucketCol = tableProperties.get(LanceConstant.TABLE_OPT_BUCKET_COLUMNS);
    String bucketNumStr = tableProperties.get(LanceConstant.TABLE_OPT_BUCKET_NUM_BUCKETS);
    if (bucketCol != null
        && !bucketCol.trim().isEmpty()
        && bucketNumStr != null
        && !bucketNumStr.trim().isEmpty()) {
      int numBuckets = Integer.parseInt(bucketNumStr.trim());
      if (numBuckets > 0) {
        spec.add(new Bucket(bucketCol.trim(), numBuckets));
        return spec;
      }
    }

    String partCol = tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
    if (partCol != null && !partCol.trim().isEmpty()) {
      spec.add(new Identity(partCol.trim()));
    }

    return spec;
  }

  public static void initializeMemWal(Dataset dataset, List<SparkLanceShardingAdapter> spec) {
    if (spec == null || spec.isEmpty() || dataset.memWalIndexDetails().isPresent()) {
      return;
    }
    if (spec.size() > 1) {
      throw new UnsupportedOperationException(
          "Lance MemWAL sharding supports one Spark sharding adapter, got: " + spec.size());
    }

    SparkLanceShardingAdapter adapter = spec.get(0);
    InitializeMemWalParams params = new InitializeMemWalParams();
    if (adapter instanceof Bucket) {
      Bucket bucket = (Bucket) adapter;
      params.withBucketSharding(bucket.getCol(), bucket.getNumBuckets());
    } else if (adapter instanceof Identity) {
      params.withIdentitySharding(adapter.getCol());
    } else {
      throw new UnsupportedOperationException(
          "Unsupported MemWAL sharding transform: " + adapter.getTransform());
    }
    dataset.initializeMemWal(params);
  }

  private static List<SparkLanceShardingAdapter> fromMemWalIndexDetails(
      Dataset dataset, MemWalIndexDetails details) {
    List<SparkLanceShardingAdapter> spec = new ArrayList<>();
    for (ShardingSpec shardingSpec : details.shardingSpecs()) {
      spec.addAll(
          fromShardingSpec(
              shardingSpec,
              sourceId -> columnNameByFieldId(dataset.getLanceSchema().fields(), sourceId)));
    }
    return spec;
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

  private static String toJson(ShardingSpec shardingSpec) {
    try {
      ObjectNode root = MAPPER.createObjectNode();
      root.put("spec_id", shardingSpec.specId());
      ArrayNode fields = root.putArray("fields");
      for (ShardingField field : shardingSpec.fields()) {
        ObjectNode fieldNode = fields.addObject();
        fieldNode.put("field_id", field.fieldId());
        ArrayNode sourceIds = fieldNode.putArray("source_ids");
        for (Integer sourceId : field.sourceIds()) {
          sourceIds.add(sourceId);
        }
        field.transform().ifPresent(transform -> fieldNode.put("transform", transform));
        field.expression().ifPresent(expression -> fieldNode.put("expression", expression));
        fieldNode.put("result_type", field.resultType());
        ObjectNode parameters = fieldNode.putObject("parameters");
        for (Map.Entry<String, String> entry : field.parameters().entrySet()) {
          parameters.put(entry.getKey(), entry.getValue());
        }
      }
      return MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize MemWAL sharding spec", e);
    }
  }

  private static ShardingSpec fromJson(JsonNode root) {
    int specId = intValue(root, "spec_id", "specId", 0);
    List<ShardingField> fields = new ArrayList<>();
    JsonNode fieldsNode = root.path("fields");
    if (fieldsNode.isArray()) {
      for (JsonNode fieldNode : fieldsNode) {
        fields.add(shardingFieldFromJson(fieldNode));
      }
    }
    return new ShardingSpec(specId, fields);
  }

  private static ShardingField shardingFieldFromJson(JsonNode fieldNode) {
    String fieldId = textValue(fieldNode, "field_id", "fieldId", "");
    List<Integer> sourceIds = new ArrayList<>();
    JsonNode sourceIdsNode = firstPresent(fieldNode, "source_ids", "sourceIds");
    if (sourceIdsNode != null && sourceIdsNode.isArray()) {
      for (JsonNode sourceId : sourceIdsNode) {
        sourceIds.add(sourceId.asInt());
      }
    }
    return new ShardingField(
        fieldId,
        sourceIds,
        nullableTextValue(fieldNode, "transform"),
        nullableTextValue(fieldNode, "expression"),
        textValue(fieldNode, "result_type", "resultType", "int32"),
        stringMap(firstPresent(fieldNode, "parameters")));
  }

  private ShardingField toShardingField() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("column", getCol());
    String fieldId = getTransform() + "(" + getCol() + ")";
    String expression = getTransform() + "(" + getCol() + ")";
    String resultType = "utf8";
    if (this instanceof Bucket) {
      int numBuckets = ((Bucket) this).getNumBuckets();
      parameters.put("num_buckets", Integer.toString(numBuckets));
      fieldId = "bucket(" + numBuckets + ", " + getCol() + ")";
      expression = fieldId;
      resultType = "int32";
    }
    return new ShardingField(
        fieldId, Collections.emptyList(), getTransform(), expression, resultType, parameters);
  }

  private static SparkLanceShardingAdapter fromShardingField(
      ShardingField field, Function<Integer, String> sourceIdToColumn) {
    String transform = field.transform().orElse(null);
    if ("bucket".equals(transform)) {
      String column = columnName(field, sourceIdToColumn);
      String numBuckets = requiredParameter(field, "num_buckets");
      return new Bucket(column, Integer.parseInt(numBuckets));
    } else if ("identity".equals(transform)) {
      return new Identity(columnName(field, sourceIdToColumn));
    }
    throw new UnsupportedOperationException("Unsupported sharding transform: " + transform);
  }

  private static String columnName(
      ShardingField field, Function<Integer, String> sourceIdToColumn) {
    String column = field.parameters().get("column");
    if (column != null && !column.trim().isEmpty()) {
      return column;
    }
    if (!field.sourceIds().isEmpty()) {
      String resolved = sourceIdToColumn.apply(field.sourceIds().get(0));
      if (resolved != null && !resolved.trim().isEmpty()) {
        return resolved;
      }
    }
    throw new IllegalArgumentException(
        "MemWAL sharding field " + field.fieldId() + " missing source column");
  }

  private static String requiredParameter(ShardingField field, String key) {
    String value = field.parameters().get(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "MemWAL sharding field " + field.fieldId() + " missing parameter " + key);
    }
    return value;
  }

  private static JsonNode firstPresent(JsonNode node, String... names) {
    for (String name : names) {
      JsonNode child = node.get(name);
      if (child != null && !child.isNull()) {
        return child;
      }
    }
    return null;
  }

  private static String textValue(JsonNode node, String first, String second, String defaultValue) {
    JsonNode child = firstPresent(node, first, second);
    return child == null ? defaultValue : child.asText();
  }

  private static String nullableTextValue(JsonNode node, String name) {
    JsonNode child = firstPresent(node, name);
    return child == null ? null : child.asText();
  }

  private static int intValue(JsonNode node, String first, String second, int defaultValue) {
    JsonNode child = firstPresent(node, first, second);
    return child == null ? defaultValue : child.asInt();
  }

  private static Map<String, String> stringMap(JsonNode node) {
    Map<String, String> result = new HashMap<>();
    if (node != null && node.isObject()) {
      node.fields()
          .forEachRemaining(entry -> result.put(entry.getKey(), entry.getValue().asText()));
    }
    return result;
  }

  /**
   * Detects partition keys for all fragments from zonemap stats. Returns a map from fragment ID to
   * partition key, or empty if the transform is not compatible with the data.
   */
  public Optional<Map<Integer, Object>> detectFragmentKeys(List<ZoneStats> zones) {
    Map<Integer, Object> result = new HashMap<>();
    // Collect all fragment IDs
    for (ZoneStats zone : zones) {
      result.putIfAbsent(zone.getFragmentId(), null);
    }
    // Compute key per fragment
    for (int fragId : new java.util.ArrayList<>(result.keySet())) {
      Optional<Object> key = fragmentKeyFromZones(zones, fragId);
      if (!key.isPresent()) {
        return Optional.empty();
      }
      result.put(fragId, key.get());
    }
    return Optional.of(result);
  }
}
