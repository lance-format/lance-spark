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

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class LanceHybridSearchQuery implements Serializable {
  private static final long serialVersionUID = -2476891273912739812L;

  private final LanceSearchQuery vectorQuery;
  private final LanceSearchQuery fullTextQuery;
  private final StructType vectorSchema;
  private final StructType fullTextSchema;
  private final Integer k;
  private final Integer offset;
  private final Float rrfK;

  public LanceHybridSearchQuery(
      LanceSearchQuery vectorQuery,
      LanceSearchQuery fullTextQuery,
      StructType vectorSchema,
      StructType fullTextSchema,
      Integer k,
      Integer offset,
      Float rrfK) {
    if (vectorQuery == null) {
      throw new IllegalArgumentException("vector query is required");
    }
    if (fullTextQuery == null) {
      throw new IllegalArgumentException("full text query is required");
    }
    if (vectorSchema == null) {
      throw new IllegalArgumentException("vector schema is required");
    }
    if (fullTextSchema == null) {
      throw new IllegalArgumentException("full text schema is required");
    }
    if (k == null || k <= 0) {
      throw new IllegalArgumentException("k must be positive");
    }
    if (offset != null && offset < 0) {
      throw new IllegalArgumentException("offset must be non-negative");
    }
    if (rrfK == null || rrfK <= 0.0f) {
      throw new IllegalArgumentException("rrf_k must be positive");
    }
    this.vectorQuery = vectorQuery;
    this.fullTextQuery = fullTextQuery;
    this.vectorSchema = vectorSchema;
    this.fullTextSchema = fullTextSchema;
    this.k = k;
    this.offset = offset == null ? Integer.valueOf(0) : offset;
    this.rrfK = rrfK;
  }

  public LanceSearchQuery getVectorQuery() {
    return vectorQuery;
  }

  public LanceSearchQuery getFullTextQuery() {
    return fullTextQuery;
  }

  public StructType getVectorSchema() {
    return vectorSchema;
  }

  public StructType getFullTextSchema() {
    return fullTextSchema;
  }

  public Integer getK() {
    return k;
  }

  public Integer getOffset() {
    return offset;
  }

  public Float getRrfK() {
    return rrfK;
  }
}
