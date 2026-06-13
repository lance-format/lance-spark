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
package org.lance.spark.read.nativeplan;

/** Stable v1 reason codes for declining native Lance scan descriptors. */
public enum LanceNativeScanFallbackReasonCode {
  PUSHED_AGGREGATION,
  TOP_N,
  MISSING_RESOLVED_VERSION,
  MISSING_SPLIT_STATE,
  UNSAFE_V1_STATE
}
