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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.{AnalyzeColumn, AnalyzeTable, LanceAnalyzeTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.lance.spark.LanceDataset

/**
 * Analyzer resolution rule that rewrites Spark's native `ANALYZE TABLE … COMPUTE STATISTICS` plans
 * into [[LanceAnalyzeTable]] when — and only when — the target resolves to a Lance table. This
 * removes the need for a custom ANALYZE grammar rule: Spark parses ANALYZE natively into
 * `AnalyzeColumn` / `AnalyzeTable`, and this rule intercepts the Lance ones during resolution.
 *
 * <p>Why a resolution rule works: `ResolveSessionCatalog` only rewrites ANALYZE for V1 (session-
 * catalog) tables; a V2 Lance table leaves `AnalyzeColumn(ResolvedTable, …)` unhandled, which would
 * otherwise fail in `CheckAnalysis` with NOT_SUPPORTED_COMMAND_FOR_V2_TABLE. `CheckAnalysis` runs
 * after the analyzer's fixed-point resolution batches, so converting the node here pre-empts the
 * rejection. Keying on `ResolvedTable.table instanceof LanceDataset` means ANALYZE on a non-Lance
 * table is left untouched for Spark to handle natively (no hijack).
 *
 * <p>Uses `transformDown` rather than `resolveOperators` deliberately: once `AnalyzeColumn`'s child
 * resolves to a `ResolvedTable`, the node is itself "analyzed", and `resolveOperators` would skip it
 * before this rule could fire. `transformDown` ignores the analyzed flag; the rewrite is idempotent
 * because the resulting `LanceAnalyzeTable` no longer matches either case.
 *
 * <p>Scope: `AnalyzeColumn` covers `FOR ALL COLUMNS` / `FOR COLUMNS …`. `AnalyzeTable` covers the
 * bare `COMPUTE STATISTICS` (no FOR clause) form, treated as FOR ALL COLUMNS — but only when it
 * carries no partition spec and no `NOSCAN`; those forms fall through to Spark (which rejects ANALYZE
 * for V2 tables), preserving the prior behavior that Lance does not support NOSCAN / partition-level
 * ANALYZE. NDV is always HLL-approximate (Spark's native ANALYZE behavior).
 */
object LanceAnalyzeTableResolution extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    // FOR [ALL] COLUMNS. columnNames is None for FOR ALL COLUMNS (allColumns=true) and
    // Some(cols) for FOR COLUMNS … (allColumns=false).
    case AnalyzeColumn(rt @ ResolvedTable(_, _, _: LanceDataset, _), columnNames, allColumns) =>
      LanceAnalyzeTable(
        ResolvedIdentifier(rt.catalog, rt.identifier),
        columnNames.getOrElse(Nil),
        allColumns)

    // Bare COMPUTE STATISTICS (no FOR clause) → FOR ALL COLUMNS. NOSCAN (noScan=true) and
    // partition-scoped ANALYZE are intentionally NOT intercepted; they fall through to Spark's
    // native (V2-rejecting) handling.
    case AnalyzeTable(rt @ ResolvedTable(_, _, _: LanceDataset, _), partitionSpec, noScan)
        if partitionSpec.isEmpty && !noScan =>
      LanceAnalyzeTable(ResolvedIdentifier(rt.catalog, rt.identifier), Nil, forAllColumns = true)
  }
}
