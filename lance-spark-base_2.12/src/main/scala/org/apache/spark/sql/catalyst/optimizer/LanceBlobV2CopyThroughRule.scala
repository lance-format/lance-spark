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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExprId, LanceBlobV2CopyRef, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.BlobPlanUtils.{LanceRelation, V2BlobWrite}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, GlobalLimit, LocalLimit, LogicalPlan, Offset, Project, ReplaceTableAsSelect, Sort, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.Metadata
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, LanceConstant}
import org.lance.spark.utils.BlobUtils

/**
 * Rewrites direct blob v2 reads in Lance writes into [[LanceBlobV2CopyRef]] tokens so writes see
 * BINARY instead of descriptor structs.
 *
 * Note that the rule stands down on DISTINCT/GROUP BY/UNION, ORDER BY blob, transformed
 * descriptors, and when the same source URI appears under different read identities.
 */
case class LanceBlobV2CopyThroughRule() extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case c: CreateTableAsSelect if c.query.resolved && isLanceCatalog(c.name) =>
      rewriteGuarded(c.query, None) match {
        case Some(q) =>
          c.copy(query = q, writeOptions = withBlobSourceContexts(c.query, c.writeOptions))
        case None => c
      }
    case r: ReplaceTableAsSelect if r.query.resolved && isLanceCatalog(r.name) =>
      rewriteGuarded(r.query, None) match {
        case Some(q) =>
          r.copy(query = q, writeOptions = withBlobSourceContexts(r.query, r.writeOptions))
        case None => r
      }
    case command @ V2BlobWrite(write) if write.query.resolved =>
      rewriteForExistingTarget(write.table, write.query).map(write.withQuery).getOrElse(command)
  }

  private def rewriteGuarded(
      query: LogicalPlan,
      targetBlobColumns: Option[Set[String]]): Option[LogicalPlan] =
    rewriteQuery(query, targetBlobColumns)
      .filterNot(rewritten => hasAmbiguousSource(query, copyTokenUris(rewritten)))

  private def copyTokenUris(rewritten: LogicalPlan): Set[String] =
    rewritten.collect { case p: Project =>
      p.projectList.collect { case Alias(ref: LanceBlobV2CopyRef, _) => ref.datasetUri }
    }.flatten.toSet

  private def hasAmbiguousSource(query: LogicalPlan, tokenUris: Set[String]): Boolean =
    BlobPlanUtils
      .collectLanceRelationsWithSubqueries(query)
      .filter { case (_, ds) => tokenUris.contains(ds.readOptions().getDatasetUri) }
      .groupBy { case (_, ds) => ds.readOptions().getDatasetUri }
      .exists { case (_, sameUri) =>
        sameUri.map { case (relation, ds) =>
          (ds.readOptions(), relation.options.asCaseSensitiveMap)
        }.distinct.size > 1
      }

  // The optimizer-phase LanceBlobSourceContextRule cannot see a CTAS query (an AnalysisOnlyCommand),
  // so inject the source context here. INSERT goes through that rule instead.
  private def withBlobSourceContexts(
      query: LogicalPlan,
      writeOptions: Map[String, String]): Map[String, String] =
    LanceBlobSourceContextRule.annotateWriteOptions(query, writeOptions)

  private def rewriteForExistingTarget(
      table: NamedRelation,
      query: LogicalPlan): Option[LogicalPlan] =
    targetBlobV2Columns(table).flatMap(names => rewriteGuarded(query, Some(names)))

  private def targetBlobV2Columns(table: NamedRelation): Option[Set[String]] = table match {
    case LanceRelation(_, ds) =>
      val names =
        ds.schema().fields.collect { case f if BlobUtils.isBlobV2SparkField(f) => f.name }.toSet
      if (names.isEmpty) None else Some(names)
    case _ => None
  }

  private def rewriteQuery(
      query: LogicalPlan,
      targetBlobColumns: Option[Set[String]]): Option[LogicalPlan] = query match {
    case p: Project => rewriteProject(p, targetBlobColumns)
    // Use field accessors rather than positional patterns. Sort gained a field in Spark 4.1 and a
    // positional pattern would break when the case class grows.
    case s: Sort =>
      rewriteQuery(s.child, targetBlobColumns)
        .filterNot(rewritten => ordersByRewrittenColumn(s.order, rewritten))
        .map(rewritten => s.copy(child = rewritten))
    case l: GlobalLimit =>
      rewriteQuery(l.child, targetBlobColumns).map(rewritten => l.copy(child = rewritten))
    case l: LocalLimit =>
      rewriteQuery(l.child, targetBlobColumns).map(rewritten => l.copy(child = rewritten))
    case o: Offset =>
      rewriteQuery(o.child, targetBlobColumns).map(rewritten => o.copy(child = rewritten))
    case a: SubqueryAlias =>
      rewriteQuery(a.child, targetBlobColumns).map(rewritten => a.copy(child = rewritten))
    // A bare table read (e.g. spark.table(...).writeTo(...).append()) has no projection to
    // rewrite. Synthesize the identity projection so the DataFrame API matches the SQL path.
    case LanceRelation(relation, _) =>
      rewriteProject(Project(relation.output, relation), targetBlobColumns)
    case _ => None
  }

  private def ordersByRewrittenColumn(order: Seq[SortOrder], rewritten: LogicalPlan): Boolean = {
    val tokenIds = rewritten.collect { case Project(projectList, _) =>
      projectList.collect { case a @ Alias(_: LanceBlobV2CopyRef, _) => a.exprId }
    }.flatten.toSet
    order.exists(_.references.exists(attr => tokenIds.contains(attr.exprId)))
  }

  private def rewriteProject(
      project: Project,
      targetBlobColumns: Option[Set[String]]): Option[LogicalPlan] = {
    val Project(projectList, child) = project
    val lanceRelations = child.collect { case LanceRelation(relation, ds) => relation -> ds }
    if (lanceRelations.isEmpty) {
      return None
    }
    val datasetByRelation = lanceRelations.toMap
    val blobColumns: Map[ExprId, DataSourceV2Relation] = lanceRelations.flatMap {
      case (relation, _) =>
        relation.output.collect {
          case a: AttributeReference if BlobUtils.isBlobV2SparkMetadata(a.metadata) =>
            a.exprId -> relation
        }
    }.toMap
    val rewrittenIds =
      projectList.flatMap(e => admittedSourceId(e, blobColumns.contains, targetBlobColumns)).toSet
    if (rewrittenIds.isEmpty) {
      return None
    }
    val contributingRelations =
      rewrittenIds.map(blobColumns).toSeq.distinct
    val bindingByRelation: Map[DataSourceV2Relation, SourceBlobBinding] =
      contributingRelations.map { relation =>
        // markAsAllowAnyAccess lets AddMetadataColumns thread _rowaddr through nested SELECTs.
        val rowAddress = relation.metadataOutput
          .find(_.name == LanceConstant.ROW_ADDRESS)
          .map(_.markAsAllowAnyAccess())
          .getOrElse(throw new IllegalStateException(
            s"blob v2 copy-through requires the '${LanceConstant.ROW_ADDRESS}' metadata column " +
              s"on '${relation.table.name()}', but the Lance relation did not expose it"))
        val datasetUri = datasetByRelation(relation).readOptions().getDatasetUri
        relation -> SourceBlobBinding(rowAddress, datasetUri)
      }.toMap
    val bindingById: Map[ExprId, SourceBlobBinding] =
      rewrittenIds.map(id => id -> bindingByRelation(blobColumns(id))).toMap
    val newChild = child.resolveOperatorsDown {
      case r: DataSourceV2Relation if bindingByRelation.contains(r) => r.withMetadataColumns()
    }
    val newProjectList =
      projectList.map(e => rewriteProjection(e, bindingById, targetBlobColumns))
    Some(Project(newProjectList, newChild))
  }

  private case class SourceBlobBinding(rowAddress: Expression, datasetUri: String)

  private def admittedSourceId(
      e: NamedExpression,
      admittedColumn: ExprId => Boolean,
      targetBlobColumns: Option[Set[String]]): Option[ExprId] =
    sourceColumnId(e).filter(id =>
      admittedColumn(id) && targetBlobColumns.forall(_.contains(e.name)))

  private def sourceColumnId(e: NamedExpression): Option[ExprId] = e match {
    case a: AttributeReference => Some(a.exprId)
    case Alias(a: AttributeReference, _) => Some(a.exprId)
    case _ => None
  }

  private def rewriteProjection(
      e: NamedExpression,
      bindingById: Map[ExprId, SourceBlobBinding],
      targetBlobColumns: Option[Set[String]]): NamedExpression = {
    if (admittedSourceId(e, bindingById.contains, targetBlobColumns).isEmpty) {
      return e
    }
    e match {
      case a: AttributeReference =>
        copyRefAlias(
          a,
          a.name,
          a.exprId,
          a.qualifier,
          a.metadata,
          bindingById(a.exprId).rowAddress,
          bindingById(a.exprId).datasetUri)
      case alias @ Alias(a: AttributeReference, name) =>
        copyRefAlias(
          a,
          name,
          alias.exprId,
          alias.qualifier,
          a.metadata,
          bindingById(a.exprId).rowAddress,
          bindingById(a.exprId).datasetUri)
      case other => other
    }
  }

  private def copyRefAlias(
      source: AttributeReference,
      outputName: String,
      exprId: ExprId,
      qualifier: Seq[String],
      metadata: Metadata,
      rowAddress: Expression,
      datasetUri: String): Alias =
    Alias(LanceBlobV2CopyRef(source, rowAddress, datasetUri, source.name), outputName)(
      exprId = exprId,
      qualifier = qualifier,
      explicitMetadata = Some(metadata))

  private def isLanceCatalog(name: LogicalPlan): Boolean = name match {
    case ResolvedIdentifier(catalog, _) => catalog.isInstanceOf[BaseLanceNamespaceSparkCatalog]
    case _ => false
  }
}
