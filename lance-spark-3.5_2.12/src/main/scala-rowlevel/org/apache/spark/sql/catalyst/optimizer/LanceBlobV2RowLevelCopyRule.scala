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

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExprId, LanceBlobV2CopyRef, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, MergeRows, Project, WriteDelta}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Instruction, Keep, Split}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.lance.spark.{LanceConstant, LanceDataset, LancePositionDeltaOperation}
import org.lance.spark.utils.BlobUtils

/**
 * Copy-through for blob v2 columns in lowered MERGE/UPDATE plans.
 *
 * Runs in the optimizer after the analyzer's DML rewrite. Descriptors become [[LanceBlobV2CopyRef]]
 * tokens; instruction conditions are left untouched (SQL null is a non-null sentinel descriptor).
 */
case class LanceBlobV2RowLevelCopyRule() extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case w: WriteDelta if maskedTarget(w.table).isDefined => rewriteWriteDelta(w)
    case a: AppendData if maskedTarget(a.table).isDefined => rewriteLoweredAppend(a)
  }

  private def maskedTarget(table: org.apache.spark.sql.catalyst.analysis.NamedRelation)
      : Option[LanceDataset] = table match {
    case r: DataSourceV2Relation =>
      r.table match {
        case op: RowLevelOperationTable =>
          op.table match {
            case masked: RowLevelMaskedLanceTable => Some(masked.delegate)
            case _ => None
          }
        case masked: RowLevelMaskedLanceTable => Some(masked.delegate)
        case _ => None
      }
    case _ => None
  }

  // Restore ACCEPT_ANY_SCHEMA before write (Spark 4.x re-validates after each optimizer rule).
  private def unmaskTarget(
      table: org.apache.spark.sql.catalyst.analysis.NamedRelation)
      : org.apache.spark.sql.catalyst.analysis.NamedRelation = table match {
    case r: DataSourceV2Relation =>
      r.table match {
        case op: RowLevelOperationTable =>
          op.table match {
            case masked: RowLevelMaskedLanceTable =>
              r.copy(table = op.copy(table = masked.delegate))
            case _ => r
          }
        case masked: RowLevelMaskedLanceTable => r.copy(table = masked.delegate)
        case _ => r
      }
    case other => other
  }

  private def lanceRelations(query: LogicalPlan): Seq[(DataSourceV2Relation, LanceDataset)] =
    query.collectWithSubqueries {
      case r: DataSourceV2Relation if relationDataset(r).isDefined => (r, relationDataset(r).get)
    }

  private def relationDataset(r: DataSourceV2Relation): Option[LanceDataset] = r.table match {
    case ds: LanceDataset => Some(ds)
    case op: RowLevelOperationTable =>
      op.table match {
        case masked: RowLevelMaskedLanceTable => Some(masked.delegate)
        case ds: LanceDataset => Some(ds)
        case _ => None
      }
    case masked: RowLevelMaskedLanceTable => Some(masked.delegate)
    case _ => None
  }

  private case class SourceBinding(
      rowAddress: Attribute,
      datasetUri: String,
      relation: DataSourceV2Relation,
      addressAlreadyProjected: Boolean)

  private def bindBlobSources(
      query: LogicalPlan,
      target: LanceDataset): Option[Map[ExprId, SourceBinding]] = {
    val relations = lanceRelations(query)
    val blobAttrs: Seq[(AttributeReference, DataSourceV2Relation, LanceDataset)] =
      relations.flatMap { case (relation, ds) =>
        relation.output.collect {
          case a: AttributeReference if BlobUtils.isBlobV2SparkMetadata(a.metadata) =>
            (a, relation, ds)
        }
      }
    if (blobAttrs.isEmpty) {
      return Some(Map.empty)
    }

    // Same URI under different read identities is ambiguous for copy tokens.
    val tokenUris = blobAttrs.map { case (_, _, ds) => ds.readOptions().getDatasetUri }.toSet
    val ambiguous = relations
      .filter { case (_, ds) => tokenUris.contains(ds.readOptions().getDatasetUri) }
      .groupBy { case (_, ds) => ds.readOptions().getDatasetUri }
      .exists { case (_, sameUri) =>
        sameUri.map { case (relation, ds) =>
          (ds.readOptions(), relation.options.asCaseSensitiveMap)
        }.distinct.size > 1
      }
    if (ambiguous) {
      return None
    }

    Some(blobAttrs.flatMap { case (a, relation, ds) =>
      val uri = ds.readOptions().getDatasetUri
      val fromOutput = relation.output.find(_.name == LanceConstant.ROW_ADDRESS)
      val fromMetadata = relation.metadataOutput
        .find(_.name == LanceConstant.ROW_ADDRESS)
        .map(_.markAsAllowAnyAccess())
      (fromOutput orElse fromMetadata).map { addr =>
        a.exprId -> SourceBinding(addr, uri, relation, fromOutput.isDefined)
      }
    }.toMap)
  }

  private def rewriteWriteDelta(w: WriteDelta): LogicalPlan = {
    val target = maskedTarget(w.table).get
    val unmasked = w.copy(table = unmaskTarget(w.table))
    val targetBlobColumns =
      target.schema().fields.collect { case f if BlobUtils.isBlobV2SparkField(f) => f.name }.toSet
    if (targetBlobColumns.isEmpty || alreadyRewritten(w.query)) {
      return unmasked
    }
    val bindings = bindBlobSources(w.query, target) match {
      case Some(b) if b.nonEmpty => b
      case _ => return unmasked
    }

    var rewrittenColumns = Set.empty[String]

    // Rewrite only the writer-facing project; join-side descriptors stay struct until consumed.
    val newQuery = w.query match {
      case p: Project =>
        val (rewritten, names) = rewriteProjectList(p.projectList, targetBlobColumns, bindings)
        rewrittenColumns ++= names
        if (names.isEmpty) p else p.copy(projectList = rewritten)
      case m: MergeRows =>
        val blobOrdinals = m.output.zipWithIndex.collect {
          case (attr, i) if targetBlobColumns.contains(attr.name) => i
        }.toSet
        if (blobOrdinals.isEmpty) {
          m
        } else {
          // Retype to BINARY only when every instruction row has a token or null (NOT MATCHED BY
          // SOURCE DELETE carries null literals for deleted rows).
          val rows = (m.matchedInstructions ++ m.notMatchedInstructions ++
            m.notMatchedBySourceInstructions).flatMap {
            case k: Keep => Seq(k.output)
            case s: Split => Seq(s.output, s.otherOutput)
            case _ => Seq.empty
          }
          def ordinalSafe(i: Int): Boolean = rows.forall { row =>
            row.lift(i).exists {
              case a: AttributeReference => bindings.contains(a.exprId)
              case lit: Literal => lit.value == null
              case _ => false
            }
          }
          val safeOrdinals = blobOrdinals.filter(ordinalSafe)
          if (safeOrdinals.isEmpty) {
            m
          } else {
            def rewriteRow(row: Seq[Expression]): Seq[Expression] = row.zipWithIndex.map {
              case (a: AttributeReference, i)
                  if safeOrdinals.contains(i) && bindings.contains(a.exprId) =>
                val b = bindings(a.exprId)
                LanceBlobV2CopyRef(a, b.rowAddress, b.datasetUri, a.name)
              case (lit: Literal, i) if safeOrdinals.contains(i) && lit.value == null =>
                Literal(null, BinaryType)
              case (e, _) => e
            }
            def rewriteOutputs(instruction: Instruction): Instruction = instruction match {
              case k: Keep => k.copy(output = rewriteRow(k.output))
              case s: Split =>
                s.copy(output = rewriteRow(s.output), otherOutput = rewriteRow(s.otherOutput))
              case other => other
            }
            safeOrdinals.foreach(i => rewrittenColumns += m.output(i).name)
            val updated = m.copy(
              matchedInstructions = m.matchedInstructions.map(rewriteOutputs),
              notMatchedInstructions = m.notMatchedInstructions.map(rewriteOutputs),
              notMatchedBySourceInstructions =
                m.notMatchedBySourceInstructions.map(rewriteOutputs))
            updated.copy(output = updated.output.zipWithIndex.map {
              case (a: AttributeReference, i) if safeOrdinals.contains(i) =>
                a.copy(dataType = BinaryType)(a.exprId, a.qualifier)
              case (attr, _) => attr
            })
          }
        }
      case other => other
    }

    if (rewrittenColumns.isEmpty) {
      return unmasked
    }
    unmasked.copy(
      table = attachBlobSourceContexts(unmasked.table, bindings),
      query = injectRowAddresses(newQuery, bindings),
      projections = retypeRowProjection(w.projections, rewrittenColumns))
  }

  // WriteDelta write options are empty; attach pinned source contexts on the operation instead.
  private def attachBlobSourceContexts(
      table: NamedRelation,
      bindings: Map[ExprId, SourceBinding]): NamedRelation = {
    val sourceDatasets = bindings.values.map(_.relation).toSeq.distinct.flatMap(relationDataset)
    val contexts = LanceBlobSourceContextRule.contextsForDatasets(sourceDatasets)
    if (contexts.isEmpty) {
      return table
    }
    table match {
      case r: DataSourceV2Relation =>
        r.table match {
          case op: RowLevelOperationTable =>
            op.operation match {
              case delta: LancePositionDeltaOperation =>
                r.copy(table = op.copy(operation = delta.withBlobSourceContexts(contexts)))
              case _ => table
            }
          case _ => table
        }
      case _ => table
    }
  }

  private def rewriteLoweredAppend(a: AppendData): LogicalPlan = {
    val target = maskedTarget(a.table).get
    val unmasked = a.copy(table = unmaskTarget(a.table))
    val targetBlobColumns =
      target.schema().fields.collect { case f if BlobUtils.isBlobV2SparkField(f) => f.name }.toSet
    if (targetBlobColumns.isEmpty || alreadyRewritten(a.query)) {
      return unmasked
    }
    val bindings = bindBlobSources(a.query, target) match {
      case Some(b) if b.nonEmpty => b
      case _ => return unmasked
    }

    a.query match {
      case p: Project =>
        val (rewritten, names) = rewriteProjectList(p.projectList, targetBlobColumns, bindings)
        if (names.isEmpty) {
          unmasked
        } else {
          val newChild = injectRowAddresses(p.child, bindings)
          unmasked.copy(query = p.copy(projectList = rewritten, child = newChild))
        }
      case q =>
        // Optimizer may drop a no-op project; rewrap outputs as copy tokens.
        val (rewritten, names) = rewriteProjectList(q.output, targetBlobColumns, bindings)
        if (names.isEmpty) {
          unmasked
        } else {
          unmasked.copy(query = Project(rewritten, injectRowAddresses(q, bindings)))
        }
    }
  }

  private def rewriteProjectList(
      projectList: Seq[NamedExpression],
      targetBlobColumns: Set[String],
      bindings: Map[ExprId, SourceBinding]): (Seq[NamedExpression], Set[String]) = {
    var names = Set.empty[String]
    val rewritten = projectList.map {
      case al @ Alias(a: AttributeReference, name)
          if targetBlobColumns.contains(name) && bindings.contains(a.exprId) =>
        val b = bindings(a.exprId)
        names += name
        Alias(LanceBlobV2CopyRef(a, b.rowAddress, b.datasetUri, a.name), name)(
          al.exprId,
          al.qualifier)
      case a: AttributeReference
          if targetBlobColumns.contains(a.name) && bindings.contains(a.exprId) =>
        val b = bindings(a.exprId)
        names += a.name
        Alias(LanceBlobV2CopyRef(a, b.rowAddress, b.datasetUri, a.name), a.name)(
          a.exprId,
          a.qualifier)
      case al @ Alias(lit: Literal, name)
          if targetBlobColumns.contains(name) && lit.value == null =>
        names += name
        Alias(Literal(null, BinaryType), name)(al.exprId, al.qualifier)
      case other => other
    }
    (rewritten, names)
  }

  private def alreadyRewritten(query: LogicalPlan): Boolean =
    query.exists(_.expressions.exists(_.exists(_.isInstanceOf[LanceBlobV2CopyRef])))

  private def injectRowAddresses(
      query: LogicalPlan,
      bindings: Map[ExprId, SourceBinding]): LogicalPlan = {
    val missing = bindings.values.filterNot(_.addressAlreadyProjected).toSeq
    if (missing.isEmpty) {
      return query
    }
    val neededAddresses = missing.map(_.rowAddress.exprId).toSet
    val relationsToRepair = missing.map(_.relation).toSet
    query.transformUp {
      case r: DataSourceV2Relation if relationsToRepair.contains(r) => r.withMetadataColumns()
      case p: Project =>
        val reachable = p.child.output.filter(out =>
          neededAddresses.contains(out.exprId) && !p.projectList.exists(_.exprId == out.exprId))
        if (reachable.isEmpty) p else p.copy(projectList = p.projectList ++ reachable)
    }
  }

  private def retypeRowProjection(
      projections: WriteDeltaProjections,
      rewrittenColumns: Set[String]): WriteDeltaProjections = {
    def retype(p: ProjectingInternalRow): ProjectingInternalRow = {
      val newSchema = StructType(p.schema.fields.map { f =>
        if (rewrittenColumns.contains(f.name)) f.copy(dataType = BinaryType) else f
      })
      ProjectingInternalRow(newSchema, p.colOrdinals)
    }
    projections.copy(rowProjection = projections.rowProjection.map(retype))
  }
}
