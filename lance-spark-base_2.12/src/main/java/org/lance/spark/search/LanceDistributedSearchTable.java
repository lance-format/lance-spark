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

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.Set;

/**
 * Vector search planned on the driver and executed by Spark tasks: each task opens the dataset
 * itself and searches one unit of it, so the namespace server never has to serve a query.
 *
 * <p>The counterpart is {@link LanceSearchTable}, which delegates the whole search to {@code
 * LanceNamespace.queryTable} and always reads a single partition.
 */
public class LanceDistributedSearchTable implements SupportsRead {
  private final String name;
  private final StructType schema;
  private final LanceSearchQuery query;

  public LanceDistributedSearchTable(String name, StructType schema, LanceSearchQuery query) {
    this.name = name;
    this.schema = schema;
    this.query = query;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new LanceDistributedSearchScanBuilder(schema, query);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }
}
