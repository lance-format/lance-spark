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
package org.lance.spark;

public class LanceConstant {
  public static final String FRAGMENT_ID = "_fragid";
  public static final String ROW_ID = "_rowid";
  public static final String ROW_ADDRESS = "_rowaddr";

  // CDF (Change Data Feed) version tracking columns
  public static final String ROW_CREATED_AT_VERSION = "_row_created_at_version";
  public static final String ROW_LAST_UPDATED_AT_VERSION = "_row_last_updated_at_version";

  // Blob metadata column suffixes
  public static final String BLOB_POSITION_SUFFIX = "__blob_pos";
  public static final String BLOB_SIZE_SUFFIX = "__blob_size";

  public static final String BACKFILL_COLUMNS_KEY = "backfill_columns";
  public static final String UPDATE_COLUMNS_KEY = "update_columns";
}
