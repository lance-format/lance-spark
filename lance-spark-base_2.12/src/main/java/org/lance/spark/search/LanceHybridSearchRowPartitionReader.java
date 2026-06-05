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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LanceHybridSearchRowPartitionReader implements PartitionReader<InternalRow> {
  private static final String DISTANCE_COLUMN = "_distance";
  private static final String FTS_SCORE_COLUMN = "_score";
  private static final String HYBRID_SCORE_COLUMN = "_relevance_score";
  private static final String ROW_ID_COLUMN = "_rowid";

  private final LanceHybridSearchInputPartition inputPartition;
  private List<InternalRow> rows;
  private int rowIndex = -1;

  public LanceHybridSearchRowPartitionReader(LanceHybridSearchInputPartition inputPartition) {
    this.inputPartition = inputPartition;
  }

  @Override
  public boolean next() throws IOException {
    if (rows == null) {
      rows = executeHybridSearch();
    }
    rowIndex += 1;
    return rowIndex < rows.size();
  }

  @Override
  public InternalRow get() {
    return rows.get(rowIndex);
  }

  @Override
  public void close() {}

  private List<InternalRow> executeHybridSearch() throws IOException {
    LanceHybridSearchQuery query = inputPartition.getQuery();
    List<SideRow> vectorRows =
        readSide(query.getVectorQuery(), query.getVectorSchema(), DISTANCE_COLUMN);
    List<SideRow> fullTextRows =
        readSide(query.getFullTextQuery(), query.getFullTextSchema(), FTS_SCORE_COLUMN);

    Map<Long, HybridRow> mergedRows = new HashMap<>();
    float rrfK = query.getRrfK();
    for (int i = 0; i < vectorRows.size(); i++) {
      SideRow row = vectorRows.get(i);
      mergedRows.computeIfAbsent(row.rowId, HybridRow::new).addVector(row, i, rrfK);
    }
    for (int i = 0; i < fullTextRows.size(); i++) {
      SideRow row = fullTextRows.get(i);
      mergedRows.computeIfAbsent(row.rowId, HybridRow::new).addFullText(row, i, rrfK);
    }

    List<HybridRow> sortedRows = new ArrayList<>(mergedRows.values());
    sortedRows.sort(
        (left, right) -> {
          int relevanceCompare = Double.compare(right.relevance, left.relevance);
          if (relevanceCompare != 0) {
            return relevanceCompare;
          }
          int rankCompare = Integer.compare(left.bestRank(), right.bestRank());
          if (rankCompare != 0) {
            return rankCompare;
          }
          return Long.compare(left.rowId, right.rowId);
        });

    int fromIndex = Math.min(query.getOffset(), sortedRows.size());
    int toIndex = Math.min(fromIndex + query.getK(), sortedRows.size());
    List<InternalRow> resultRows = new ArrayList<>(toIndex - fromIndex);
    for (int i = fromIndex; i < toIndex; i++) {
      resultRows.add(toInternalRow(sortedRows.get(i), inputPartition.getSchema()));
    }
    return resultRows;
  }

  private List<SideRow> readSide(LanceSearchQuery query, StructType schema, String metricColumn)
      throws IOException {
    LanceSearchColumnarPartitionReader reader =
        new LanceSearchColumnarPartitionReader(new LanceSearchInputPartition(schema, query));
    try {
      return readSideRows(reader, schema, metricColumn);
    } finally {
      reader.close();
    }
  }

  private List<SideRow> readSideRows(
      LanceSearchColumnarPartitionReader reader, StructType schema, String metricColumn)
      throws IOException {
    int rowIdIndex = fieldIndex(schema, ROW_ID_COLUMN);
    int metricIndex = fieldIndex(schema, metricColumn);
    StructField[] fields = schema.fields();
    List<SideValueField> valueFields = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      String fieldName = fields[i].name();
      if (!fieldName.equalsIgnoreCase(ROW_ID_COLUMN) && !fieldName.equalsIgnoreCase(metricColumn)) {
        valueFields.add(new SideValueField(i, fieldName));
      }
    }

    List<SideRow> sideRows = new ArrayList<>();
    while (reader.next()) {
      ColumnarBatch batch = reader.get();
      Iterator<InternalRow> rowIterator = batch.rowIterator();
      while (rowIterator.hasNext()) {
        InternalRow copiedRow = rowIterator.next().copy();
        long rowId = copiedRow.getLong(rowIdIndex);
        Float metric = copiedRow.isNullAt(metricIndex) ? null : copiedRow.getFloat(metricIndex);
        Map<String, Object> values = new HashMap<>();
        for (SideValueField valueField : valueFields) {
          if (copiedRow.isNullAt(valueField.index)) {
            values.put(valueField.name, null);
          } else {
            values.put(
                valueField.name,
                copiedRow.get(valueField.index, fields[valueField.index].dataType()));
          }
        }
        sideRows.add(new SideRow(rowId, metric, values));
      }
    }
    return sideRows;
  }

  private InternalRow toInternalRow(HybridRow row, StructType outputSchema) {
    StructField[] fields = outputSchema.fields();
    Object[] values = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String fieldName = fields[i].name();
      if (fieldName.equalsIgnoreCase(DISTANCE_COLUMN)) {
        values[i] = row.distance;
      } else if (fieldName.equalsIgnoreCase(FTS_SCORE_COLUMN)) {
        values[i] = row.score;
      } else if (fieldName.equalsIgnoreCase(HYBRID_SCORE_COLUMN)) {
        values[i] = (float) row.relevance;
      } else if (fieldName.equalsIgnoreCase(ROW_ID_COLUMN)) {
        values[i] = row.rowId;
      } else {
        values[i] = row.values.get(fieldName);
      }
    }
    return new GenericInternalRow(values);
  }

  private int fieldIndex(StructType schema, String name) {
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().equalsIgnoreCase(name)) {
        return i;
      }
    }
    throw new IllegalStateException(
        "Expected field '" + name + "' in schema " + schema.treeString());
  }

  private static double rrfScore(int rank, float rrfK) {
    return 1.0d / (((double) rank) + (double) rrfK);
  }

  private static final class SideValueField {
    private final int index;
    private final String name;

    private SideValueField(int index, String name) {
      this.index = index;
      this.name = name;
    }
  }

  private static final class SideRow {
    private final long rowId;
    private final Float metric;
    private final Map<String, Object> values;

    private SideRow(long rowId, Float metric, Map<String, Object> values) {
      this.rowId = rowId;
      this.metric = metric;
      this.values = values;
    }
  }

  private static final class HybridRow {
    private final long rowId;
    private final Map<String, Object> values = new HashMap<>();
    private Float distance;
    private Float score;
    private double relevance;
    private int vectorRank = Integer.MAX_VALUE;
    private int fullTextRank = Integer.MAX_VALUE;

    private HybridRow(long rowId) {
      this.rowId = rowId;
    }

    private void addVector(SideRow row, int rank, float rrfK) {
      mergeValues(row);
      distance = row.metric;
      vectorRank = Math.min(vectorRank, rank);
      relevance += rrfScore(rank, rrfK);
    }

    private void addFullText(SideRow row, int rank, float rrfK) {
      mergeValues(row);
      score = row.metric;
      fullTextRank = Math.min(fullTextRank, rank);
      relevance += rrfScore(rank, rrfK);
    }

    private void mergeValues(SideRow row) {
      for (Map.Entry<String, Object> entry : row.values.entrySet()) {
        values.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }

    private int bestRank() {
      return Math.min(vectorRank, fullTextRank);
    }
  }
}
