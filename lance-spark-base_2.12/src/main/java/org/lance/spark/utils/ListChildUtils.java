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
package org.lance.spark.utils;

import org.apache.spark.sql.types.Metadata;

/** Utility methods for preserving Arrow List child field names via Spark metadata. */
public class ListChildUtils {

  public static final String LANCE_LIST_CHILD_NAME_METADATA_KEY = "_lance.list.child.name";
  public static final String LIST_CHILD_NAME_DEFAULT = "item";

  /** Returns the persisted Arrow List child name, or the default if metadata is absent. */
  public static String listChildName(Metadata metadata) {
    if (metadata == null || !metadata.contains(LANCE_LIST_CHILD_NAME_METADATA_KEY)) {
      return LIST_CHILD_NAME_DEFAULT;
    }
    return metadata.getString(LANCE_LIST_CHILD_NAME_METADATA_KEY);
  }
}
