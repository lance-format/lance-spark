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

import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableRequestColumns;
import org.lance.namespace.model.QueryTableRequestFullTextQuery;
import org.lance.namespace.model.QueryTableRequestVector;
import org.lance.namespace.model.StringFtsQuery;
import org.lance.spark.utils.FullTextQueryConverter;
import org.lance.spark.utils.FullTextQueryUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceSearchQuery implements Serializable {
  private static final long serialVersionUID = 538912738912739128L;

  public enum SearchType {
    VECTOR,
    FULL_TEXT
  }

  private final SearchType searchType;
  private final List<String> tableId;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> outputColumns;
  private final Integer k;
  private final Integer offset;
  private final Long version;
  private final String filter;
  private final Boolean withRowId;
  private final List<Float> vector;
  private final String vectorColumn;
  private final String distanceType;
  private final Integer nprobes;
  private final Integer ef;
  private final Integer refineFactor;
  private final Float lowerBound;
  private final Float upperBound;
  private final Boolean bypassVectorIndex;
  private final Boolean fastSearch;
  private final Boolean prefilter;
  private final String textQuery;
  private final List<String> searchColumns;
  private final String fullTextQueryJson;

  private LanceSearchQuery(Builder builder) {
    this.searchType = builder.searchType;
    this.tableId = immutableList(builder.tableId);
    this.namespaceImpl = builder.namespaceImpl;
    this.namespaceProperties = immutableMap(builder.namespaceProperties);
    this.outputColumns = immutableList(builder.outputColumns);
    this.k = builder.k;
    this.offset = builder.offset;
    this.version = builder.version;
    this.filter = builder.filter;
    this.withRowId = builder.withRowId;
    this.vector = immutableList(builder.vector);
    this.vectorColumn = builder.vectorColumn;
    this.distanceType = builder.distanceType;
    this.nprobes = builder.nprobes;
    this.ef = builder.ef;
    this.refineFactor = builder.refineFactor;
    this.lowerBound = builder.lowerBound;
    this.upperBound = builder.upperBound;
    this.bypassVectorIndex = builder.bypassVectorIndex;
    this.fastSearch = builder.fastSearch;
    this.prefilter = builder.prefilter;
    this.textQuery = builder.textQuery;
    this.searchColumns = immutableList(builder.searchColumns);
    this.fullTextQueryJson = builder.fullTextQueryJson;
  }

  public static Builder builder(SearchType searchType) {
    return new Builder(searchType);
  }

  public SearchType getSearchType() {
    return searchType;
  }

  public List<String> getTableId() {
    return tableId;
  }

  public String getNamespaceImpl() {
    return namespaceImpl;
  }

  public Map<String, String> getNamespaceProperties() {
    return namespaceProperties;
  }

  public QueryTableRequest toQueryTableRequest() {
    QueryTableRequest request = new QueryTableRequest().id(tableId).k(k);
    request.vector(new QueryTableRequestVector());

    if (!outputColumns.isEmpty()) {
      request.columns(new QueryTableRequestColumns().columnNames(outputColumns));
    }
    if (offset != null) {
      request.offset(offset);
    }
    if (version != null) {
      request.version(version);
    }
    if (filter != null) {
      request.filter(filter);
    }
    if (withRowId != null) {
      request.withRowId(withRowId);
    }

    if (searchType == SearchType.VECTOR) {
      request.vector(new QueryTableRequestVector().singleVector(vector));
      if (vectorColumn != null) {
        request.vectorColumn(vectorColumn);
      }
      if (distanceType != null) {
        request.distanceType(distanceType);
      }
      if (nprobes != null) {
        request.nprobes(nprobes);
      }
      if (ef != null) {
        request.ef(ef);
      }
      if (refineFactor != null) {
        request.refineFactor(refineFactor);
      }
      if (lowerBound != null) {
        request.lowerBound(lowerBound);
      }
      if (upperBound != null) {
        request.upperBound(upperBound);
      }
      if (bypassVectorIndex != null) {
        request.bypassVectorIndex(bypassVectorIndex);
      }
      if (fastSearch != null) {
        request.fastSearch(fastSearch);
      }
      if (prefilter != null) {
        request.prefilter(prefilter);
      }
    } else if (fullTextQueryJson != null) {
      // Unified path: a structured FullTextQuery carried through the shared scan spec.
      request.fullTextQuery(
          new QueryTableRequestFullTextQuery()
              .structuredQuery(
                  FullTextQueryConverter.toStructuredFtsQuery(
                      FullTextQueryUtils.stringToFullTextQuery(fullTextQueryJson))));
    } else {
      StringFtsQuery stringQuery = new StringFtsQuery().query(textQuery);
      if (!searchColumns.isEmpty()) {
        stringQuery.columns(searchColumns);
      }
      request.fullTextQuery(new QueryTableRequestFullTextQuery().stringQuery(stringQuery));
    }

    return request;
  }

  private static <T> List<T> immutableList(List<T> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(new ArrayList<>(values));
  }

  private static Map<String, String> immutableMap(Map<String, String> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new HashMap<>(values));
  }

  public static final class Builder {
    private final SearchType searchType;
    private List<String> tableId = Collections.emptyList();
    private String namespaceImpl;
    private Map<String, String> namespaceProperties = Collections.emptyMap();
    private List<String> outputColumns = Collections.emptyList();
    private Integer k = 10;
    private Integer offset;
    private Long version;
    private String filter;
    private Boolean withRowId;
    private List<Float> vector = Collections.emptyList();
    private String vectorColumn;
    private String distanceType;
    private Integer nprobes;
    private Integer ef;
    private Integer refineFactor;
    private Float lowerBound;
    private Float upperBound;
    private Boolean bypassVectorIndex;
    private Boolean fastSearch;
    private Boolean prefilter;
    private String textQuery;
    private List<String> searchColumns = Collections.emptyList();
    private String fullTextQueryJson;

    private Builder(SearchType searchType) {
      this.searchType = searchType;
    }

    public Builder tableId(List<String> tableId) {
      this.tableId = tableId;
      return this;
    }

    public Builder namespaceImpl(String namespaceImpl) {
      this.namespaceImpl = namespaceImpl;
      return this;
    }

    public Builder namespaceProperties(Map<String, String> namespaceProperties) {
      this.namespaceProperties = namespaceProperties;
      return this;
    }

    public Builder outputColumns(List<String> outputColumns) {
      this.outputColumns = outputColumns;
      return this;
    }

    public Builder topK(Integer k) {
      this.k = k;
      return this;
    }

    public Builder offset(Integer offset) {
      this.offset = offset;
      return this;
    }

    public Builder version(Long version) {
      this.version = version;
      return this;
    }

    public Builder filter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder withRowId(Boolean withRowId) {
      this.withRowId = withRowId;
      return this;
    }

    public Builder vector(List<Float> vector) {
      this.vector = vector;
      return this;
    }

    public Builder vectorColumn(String vectorColumn) {
      this.vectorColumn = vectorColumn;
      return this;
    }

    public Builder distanceType(String distanceType) {
      this.distanceType = distanceType;
      return this;
    }

    public Builder nprobes(Integer nprobes) {
      this.nprobes = nprobes;
      return this;
    }

    public Builder ef(Integer ef) {
      this.ef = ef;
      return this;
    }

    public Builder refineFactor(Integer refineFactor) {
      this.refineFactor = refineFactor;
      return this;
    }

    public Builder lowerBound(Float lowerBound) {
      this.lowerBound = lowerBound;
      return this;
    }

    public Builder upperBound(Float upperBound) {
      this.upperBound = upperBound;
      return this;
    }

    public Builder bypassVectorIndex(Boolean bypassVectorIndex) {
      this.bypassVectorIndex = bypassVectorIndex;
      return this;
    }

    public Builder fastSearch(Boolean fastSearch) {
      this.fastSearch = fastSearch;
      return this;
    }

    public Builder prefilter(Boolean prefilter) {
      this.prefilter = prefilter;
      return this;
    }

    public Builder textQuery(String textQuery) {
      this.textQuery = textQuery;
      return this;
    }

    public Builder searchColumns(List<String> searchColumns) {
      this.searchColumns = searchColumns;
      return this;
    }

    /** Serialized structured FullTextQuery for the unified scan path (see FullTextQueryUtils). */
    public Builder fullTextQueryJson(String fullTextQueryJson) {
      this.fullTextQueryJson = fullTextQueryJson;
      return this;
    }

    public LanceSearchQuery build() {
      if (searchType == null) {
        throw new IllegalArgumentException("search type is required");
      }
      if (tableId == null || tableId.isEmpty()) {
        throw new IllegalArgumentException("table id is required");
      }
      if (namespaceImpl == null || namespaceImpl.isEmpty()) {
        throw new IllegalArgumentException("namespace implementation is required");
      }
      if (k != null && k <= 0) {
        throw new IllegalArgumentException("k must be positive");
      }
      if (offset != null && offset < 0) {
        throw new IllegalArgumentException("offset must be non-negative");
      }
      if (searchType == SearchType.VECTOR && (vector == null || vector.isEmpty())) {
        throw new IllegalArgumentException("query_vector is required");
      }
      if (searchType == SearchType.FULL_TEXT
          && (textQuery == null || textQuery.isEmpty())
          && fullTextQueryJson == null) {
        throw new IllegalArgumentException("query is required");
      }
      return new LanceSearchQuery(this);
    }
  }
}
