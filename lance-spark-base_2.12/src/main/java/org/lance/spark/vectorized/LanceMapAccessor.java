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
package org.lance.spark.vectorized;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.vectorized.ColumnarMap;

public class LanceMapAccessor {
  private final MapVector accessor;
  private final LanceArrowColumnVector keys;
  private final LanceArrowColumnVector values;

  public LanceMapAccessor(MapVector vector) {
    this.accessor = vector;
    StructVector entries = (StructVector) vector.getDataVector();
    this.keys = new LanceArrowColumnVector(entries.getChildByOrdinal(0));
    this.values = new LanceArrowColumnVector(entries.getChildByOrdinal(1));
  }

  public boolean isNullAt(int rowId) {
    return accessor.isNull(rowId);
  }

  public int getNullCount() {
    return accessor.getNullCount();
  }

  public ColumnarMap getMap(int rowId) {
    int start = accessor.getElementStartIndex(rowId);
    int end = accessor.getElementEndIndex(rowId);
    return new ColumnarMap(keys, values, start, end - start);
  }

  public void close() {
    accessor.close();
  }
}
