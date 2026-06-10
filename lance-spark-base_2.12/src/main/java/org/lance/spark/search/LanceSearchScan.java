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

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.search.LanceSearchQuery.SearchType;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public class LanceSearchScan implements Scan, Batch, Serializable {
  private static final long serialVersionUID = -120398471239847123L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceSearchScan.class);
  private static final Set<String> VECTOR_INDEX_TYPES =
      new HashSet<>(
          Arrays.asList(
              "VECTOR",
              "IVF_FLAT",
              "IVF_PQ",
              "IVF_SQ",
              "IVF_HNSW_FLAT",
              "IVF_HNSW_SQ",
              "IVF_HNSW_PQ",
              "IVF_RQ"));

  private final StructType schema;
  private final LanceSearchQuery query;
  private final boolean distributed;
  private final LanceSparkReadOptions readOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final Map<String, String> initialStorageOptions;

  public LanceSearchScan(
      StructType schema,
      LanceSearchQuery query,
      boolean distributed,
      LanceSparkReadOptions readOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      Map<String, String> initialStorageOptions) {
    this.schema = schema;
    this.query = query;
    this.distributed = distributed;
    this.readOptions = readOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties =
        namespaceProperties == null ? Collections.emptyMap() : namespaceProperties;
    this.initialStorageOptions =
        initialStorageOptions == null ? Collections.emptyMap() : initialStorageOptions;
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
    if (!distributed) {
      return new InputPartition[] {new LanceSearchInputPartition(schema, query)};
    }
    return planDistributed();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceSearchPartitionReaderFactory();
  }

  private InputPartition[] planDistributed() {
    Dataset dataset =
        Utils.openDatasetBuilder(readOptions).initialStorageOptions(initialStorageOptions).build();
    try {
      Set<Integer> existingFragments = new HashSet<>();
      for (Fragment fragment : dataset.getFragments()) {
        existingFragments.add(fragment.getId());
      }
      if (existingFragments.isEmpty()) {
        LOG.info("Lance distributed vector search: empty dataset, returning empty result");
        return new InputPartition[0];
      }

      String column = resolveVectorColumn(dataset);
      Optional<VectorIndexInfo> vectorIndex = selectVectorIndex(dataset, column);
      boolean fastSearch = Boolean.TRUE.equals(query.getFastSearch());

      List<PlannedUnit> units = planUnits(existingFragments, vectorIndex, fastSearch);

      long indexedCount = units.stream().filter(u -> !u.indexSegments.isEmpty()).count();
      LOG.info(
          "Lance distributed vector search: column={}, indexName={}, units={} "
              + "(indexed={}, fallback={}), candidateK={}",
          column,
          vectorIndex.map(VectorIndexInfo::getIndexName).orElse("none"),
          units.size(),
          indexedCount,
          units.size() - indexedCount,
          query.getK());

      if (units.isEmpty()) {
        return new InputPartition[0];
      }

      LanceSearchQuery resolvedQuery = withResolvedVectorColumn(query, column);
      InputPartition[] result = new InputPartition[units.size()];
      for (int i = 0; i < units.size(); i++) {
        PlannedUnit u = units.get(i);
        result[i] =
            new LanceSearchInputPartition(
                schema,
                resolvedQuery,
                u.fragmentIds,
                u.indexSegments,
                readOptions,
                namespaceImpl,
                namespaceProperties,
                initialStorageOptions);
      }
      return result;
    } finally {
      dataset.close();
    }
  }

  private static List<PlannedUnit> planUnits(
      Set<Integer> existingFragments, Optional<VectorIndexInfo> vectorIndex, boolean fastSearch) {
    if (existingFragments.isEmpty()) {
      return Collections.emptyList();
    }
    List<PlannedUnit> units = new ArrayList<>();
    Set<Integer> indexedFragments = new HashSet<>();
    if (vectorIndex.isPresent()) {
      for (VectorIndexSegment segment : vectorIndex.get().getSegments()) {
        Set<Integer> covered = new HashSet<>(segment.getFragmentIds());
        covered.retainAll(existingFragments);
        if (covered.isEmpty()) {
          continue;
        }
        indexedFragments.addAll(covered);
        units.add(PlannedUnit.indexed(segment.getUuid()));
      }
    }
    if (!fastSearch) {
      Set<Integer> uncovered = new TreeSet<>(existingFragments);
      uncovered.removeAll(indexedFragments);
      for (Integer fragmentId : uncovered) {
        units.add(PlannedUnit.fallback(fragmentId));
      }
    }
    return units;
  }

  private String resolveVectorColumn(Dataset dataset) {
    String declared = query.getVectorColumn();
    if (declared != null && !declared.isEmpty()) {
      return declared;
    }
    for (LanceField field : dataset.getLanceSchema().fields()) {
      if (field.getType() instanceof ArrowType.FixedSizeList) {
        return field.getName();
      }
    }
    throw new IllegalArgumentException(
        "VECTOR_SEARCH could not auto-detect a vector column; pass vector_column explicitly");
  }

  private static Optional<VectorIndexInfo> selectVectorIndex(Dataset dataset, String column) {
    List<IndexDescription> indices;
    try {
      indices = dataset.describeIndices(new IndexCriteria.Builder().build());
    } catch (Exception e) {
      LOG.warn("describeIndices failed, falling back to flat search: {}", e.getMessage());
      return Optional.empty();
    }

    Map<Integer, String> fieldIdToName = new HashMap<>();
    for (LanceField field : dataset.getLanceSchema().fields()) {
      fieldIdToName.put(field.getId(), field.getName());
    }

    for (IndexDescription idx : indices) {
      if (!isVectorIndex(idx)) {
        continue;
      }
      List<String> fieldNames = new ArrayList<>();
      for (Integer fieldId : idx.getFieldIds()) {
        String name = fieldIdToName.get(fieldId);
        if (name != null) {
          fieldNames.add(name);
        }
      }
      if (!fieldNames.contains(column)) {
        continue;
      }
      List<VectorIndexSegment> segments = new ArrayList<>();
      for (Index segment : idx.getSegments()) {
        UUID uuid = segment.uuid();
        Set<Integer> fragmentIds = segment.fragments().map(HashSet::new).orElseGet(HashSet::new);
        segments.add(new VectorIndexSegment(uuid, fragmentIds));
      }
      return Optional.of(new VectorIndexInfo(idx.getName(), segments));
    }
    return Optional.empty();
  }

  private static boolean isVectorIndex(IndexDescription idx) {
    String type = idx.getIndexType();
    if (type == null) {
      return false;
    }
    return VECTOR_INDEX_TYPES.contains(type.toUpperCase(Locale.ROOT));
  }

  private static LanceSearchQuery withResolvedVectorColumn(
      LanceSearchQuery base, String resolvedColumn) {
    if (base.getVectorColumn() != null && !base.getVectorColumn().isEmpty()) {
      return base;
    }
    return LanceSearchQuery.builder(SearchType.VECTOR)
        .tableId(base.getTableId())
        .namespaceImpl(base.getNamespaceImpl())
        .namespaceProperties(base.getNamespaceProperties())
        .outputColumns(base.getOutputColumns())
        .topK(base.getK())
        .offset(base.getOffset())
        .version(base.getVersion())
        .filter(base.getFilter())
        .withRowId(base.getWithRowId())
        .vector(base.getVector())
        .vectorColumn(resolvedColumn)
        .distanceType(base.getDistanceType())
        .nprobes(base.getNprobes())
        .ef(base.getEf())
        .refineFactor(base.getRefineFactor())
        .lowerBound(base.getLowerBound())
        .upperBound(base.getUpperBound())
        .bypassVectorIndex(base.getBypassVectorIndex())
        .fastSearch(base.getFastSearch())
        .prefilter(base.getPrefilter())
        .build();
  }

  /** One planned execution unit: either a single-segment indexed scan, or a single-fragment KNN. */
  private static final class PlannedUnit {
    final List<Integer> fragmentIds;
    final List<UUID> indexSegments;

    private PlannedUnit(List<Integer> fragmentIds, List<UUID> indexSegments) {
      this.fragmentIds = fragmentIds;
      this.indexSegments = indexSegments;
    }

    static PlannedUnit indexed(UUID segmentUuid) {
      return new PlannedUnit(Collections.emptyList(), Collections.singletonList(segmentUuid));
    }

    static PlannedUnit fallback(Integer fragmentId) {
      return new PlannedUnit(Collections.singletonList(fragmentId), Collections.emptyList());
    }
  }

  /** Lightweight view of a vector index for planning. */
  private static final class VectorIndexInfo {
    private final String indexName;
    private final List<VectorIndexSegment> segments;

    VectorIndexInfo(String indexName, List<VectorIndexSegment> segments) {
      this.indexName = indexName;
      this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
    }

    String getIndexName() {
      return indexName;
    }

    List<VectorIndexSegment> getSegments() {
      return segments;
    }
  }

  /** One physical segment of a vector index. */
  private static final class VectorIndexSegment {
    private final UUID uuid;
    private final Set<Integer> fragmentIds;

    VectorIndexSegment(UUID uuid, Set<Integer> fragmentIds) {
      this.uuid = uuid;
      this.fragmentIds = Collections.unmodifiableSet(new HashSet<>(fragmentIds));
    }

    UUID getUuid() {
      return uuid;
    }

    Set<Integer> getFragmentIds() {
      return fragmentIds;
    }
  }
}
