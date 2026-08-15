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
package org.lance.spark.write;

import org.lance.Dataset;
import org.lance.index.scalar.ZoneStats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Identifies, from zonemap statistics alone, which fragments are <em>provably</em> covered in full
 * by a {@code REPLACE ... WHERE} equality predicate — so they can be dropped by id without scanning
 * their rows.
 *
 * <p>The predicate must be a pure conjunction of {@code column = literal} terms (encoded as JSON by
 * {@code ReplaceWhereExec}). A fragment is considered fully covered only when, for <b>every</b>
 * equality column, the fragment has at least one zonemap zone and <b>all</b> of its zones are
 * pinned to the required value (zone {@code min == max == value}) with no nulls. This is
 * deliberately conservative: any column without a zonemap, any zone whose bounds are not pinned to
 * the value, or any value/format mismatch simply excludes the fragment, which then falls back to
 * the exact scan-based deletion. The method therefore only ever avoids work; it never changes which
 * rows are replaced.
 */
final class ReplaceCoverage {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ReplaceCoverage() {}

  /**
   * Returns the ids of fragments provably covered in full by the equality predicate, or an empty
   * set when the fast path does not apply (no equality terms, missing zonemaps, or nothing proven).
   *
   * @param ds the dataset being replaced into
   * @param equalitiesJson JSON array of {@code {"column":..,"value":..}} terms, or null
   */
  static Set<Integer> fullyCoveredFragmentIds(Dataset ds, String equalitiesJson) {
    if (equalitiesJson == null || equalitiesJson.isEmpty()) {
      return Collections.emptySet();
    }

    List<String[]> equalities = parseEqualities(equalitiesJson);
    if (equalities.isEmpty()) {
      return Collections.emptySet();
    }

    Set<Integer> covered = null;
    for (String[] equality : equalities) {
      String column = equality[0];
      String value = equality[1];
      Set<Integer> pinned = fragmentsPinnedToValue(ds, column, value);
      if (pinned.isEmpty()) {
        // No fragment can be proven covered for this column (e.g. no zonemap on it), so the
        // conjunction cannot cover any fragment.
        return Collections.emptySet();
      }
      if (covered == null) {
        covered = pinned;
      } else {
        covered.retainAll(pinned);
      }
      if (covered.isEmpty()) {
        return Collections.emptySet();
      }
    }
    return covered == null ? Collections.emptySet() : covered;
  }

  /**
   * Returns the ids of fragments whose zonemap on {@code column} proves every live row equals
   * {@code value}: the fragment has at least one zone, and all of its zones have {@code min == max
   * == value} (by canonical string form) with zero nulls. Returns an empty set if the column has no
   * zonemap index.
   */
  private static Set<Integer> fragmentsPinnedToValue(Dataset ds, String column, String value) {
    List<ZoneStats> zones = ds.getZonemapStats(column);
    if (zones == null || zones.isEmpty()) {
      return Collections.emptySet();
    }

    // Group zones per fragment and track, per fragment, whether every zone is pinned to the value.
    Set<Integer> candidate = new HashSet<>();
    Set<Integer> disqualified = new HashSet<>();
    for (ZoneStats zone : zones) {
      int fragmentId = zone.getFragmentId();
      if (disqualified.contains(fragmentId)) {
        continue;
      }
      if (isZonePinnedToValue(zone, value)) {
        candidate.add(fragmentId);
      } else {
        candidate.remove(fragmentId);
        disqualified.add(fragmentId);
      }
    }
    candidate.removeAll(disqualified);
    return candidate;
  }

  /**
   * A zone is pinned to {@code value} when it contains only that value: min == max == value, no
   * nulls.
   */
  private static boolean isZonePinnedToValue(ZoneStats zone, String value) {
    if (zone.getNullCount() != 0) {
      return false;
    }
    Comparable<?> min = zone.getMin();
    Comparable<?> max = zone.getMax();
    if (min == null || max == null) {
      return false;
    }
    // Compare by canonical string form: zonemap min/max box as Long/Double/String via JNI, and the
    // required value is the literal's toString() from ReplaceWhereExec. A mismatch (including any
    // formatting difference) conservatively fails the proof and defers to the exact scan.
    return value.equals(min.toString()) && value.equals(max.toString());
  }

  private static List<String[]> parseEqualities(String json) {
    try {
      JsonNode array = MAPPER.readTree(json);
      java.util.List<String[]> result = new java.util.ArrayList<>();
      for (JsonNode node : array) {
        JsonNode column = node.get("column");
        JsonNode value = node.get("value");
        if (column == null || value == null) {
          return Collections.emptyList();
        }
        result.add(new String[] {column.asText(), value.asText()});
      }
      return result;
    } catch (Exception e) {
      // Malformed encoding: skip the fast path entirely and let the exact scan handle the delete.
      return Collections.emptyList();
    }
  }
}
