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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Plans a vector search as one Spark task per searchable unit of the dataset: one task per segment
 * of the vector index, plus one flat-KNN task per fragment no segment covers (unless {@code
 * fast_search} asked for indexed data only).
 *
 * <p>Each unit returns its own top {@code k}; merging them into the global top {@code k} is the
 * caller's job - {@code LanceSearchTableFunctions} wraps this scan in a global sort and limit.
 */
public class LanceDistributedSearchScan implements Scan, Batch, Serializable {
  private static final long serialVersionUID = -917364523098172364L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceDistributedSearchScan.class);
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

  public LanceDistributedSearchScan(StructType schema, LanceSearchQuery query) {
    Objects.requireNonNull(
        query.getReadOptions(), "query.readOptions is required for distributed search");
    this.schema = schema;
    this.query = query;
  }

  @Override
  public StructType readSchema() {
    return schema;
  }

  @Override
  public String description() {
    return "LanceDistributedSearchScan";
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceDistributedSearchPartitionReaderFactory();
  }

  @Override
  public InputPartition[] planInputPartitions() {
    Dataset dataset =
        Utils.openDatasetBuilder(query.getReadOptions())
            .initialStorageOptions(query.getInitialStorageOptions())
            .build();
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

      List<LanceDistributedSearchInputPartition> units =
          planUnits(
              withResolvedVectorColumn(query, column), existingFragments, vectorIndex, fastSearch);

      long indexedCount = units.stream().filter(u -> !u.getIndexSegments().isEmpty()).count();
      LOG.info(
          "Lance distributed vector search: column={}, indexName={}, units={} "
              + "(indexed={}, fallback={}), candidateK={}",
          column,
          vectorIndex.map(VectorIndexInfo::getIndexName).orElse("none"),
          units.size(),
          indexedCount,
          units.size() - indexedCount,
          query.getK());

      return units.toArray(new InputPartition[0]);
    } finally {
      dataset.close();
    }
  }

  private List<LanceDistributedSearchInputPartition> planUnits(
      LanceSearchQuery resolvedQuery,
      Set<Integer> existingFragments,
      Optional<VectorIndexInfo> vectorIndex,
      boolean fastSearch) {
    List<LanceDistributedSearchInputPartition> units = new ArrayList<>();
    // Fragments still waiting for an owner; each index segment claims the ones it covers.
    Set<Integer> uncovered = new TreeSet<>(existingFragments);
    if (vectorIndex.isPresent()) {
      for (VectorIndexSegment segment : vectorIndex.get().getSegments()) {
        // A segment whose fragments are all gone (compaction, deletion) is stale: no unit for it.
        if (Collections.disjoint(segment.getFragmentIds(), existingFragments)) {
          continue;
        }
        uncovered.removeAll(segment.getFragmentIds());
        units.add(
            LanceDistributedSearchInputPartition.forIndexSegment(
                schema, resolvedQuery, segment.getUuid()));
      }
    }
    if (!fastSearch) {
      for (Integer fragmentId : uncovered) {
        units.add(
            LanceDistributedSearchInputPartition.forFragment(schema, resolvedQuery, fragmentId));
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
        .readOptions(base.getReadOptions())
        .initialStorageOptions(base.getInitialStorageOptions())
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
