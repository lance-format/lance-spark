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
package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.spark.sql.catalyst.analysis.{UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{AddColumnsBackfill, AddIndex, CreateBranch, CreateTag, DropBranch, DropTag, LanceDropIndex, LanceNamedArgument, LogicalPlan, Optimize, SetUnenforcedPrimaryKey, ShowBranches, ShowIndexes, ShowTags, UpdateColumnsBackfill, Vacuum}
import org.lance.spark.utils.ParserUtils

import scala.jdk.CollectionConverters._

class LanceSqlExtensionsAstBuilder(delegate: ParserInterface)
  extends LanceSqlExtensionsBaseVisitor[AnyRef] {

  private def cleanIdentifier(text: String): String = ParserUtils.cleanIdentifier(text)

  override def visitSingleStatement(ctx: LanceSqlExtensionsParser.SingleStatementContext)
      : LogicalPlan = {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitAddColumnsBackfill(ctx: LanceSqlExtensionsParser.AddColumnsBackfillContext)
      : AddColumnsBackfill = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val columnNames = visitColumnList(ctx.columnList())
    val source = UnresolvedRelation(Seq(cleanIdentifier(ctx.identifier().getText)))
    AddColumnsBackfill(table, columnNames, source)
  }

  override def visitUpdateColumnsBackfill(
      ctx: LanceSqlExtensionsParser.UpdateColumnsBackfillContext): UpdateColumnsBackfill = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val columnNames = visitColumnList(ctx.columnList())
    val source = UnresolvedRelation(Seq(cleanIdentifier(ctx.identifier().getText)))
    UpdateColumnsBackfill(table, columnNames, source)
  }

  override def visitMultipartIdentifier(ctx: LanceSqlExtensionsParser.MultipartIdentifierContext)
      : Seq[String] = {
    ctx.parts.asScala.map(p => cleanIdentifier(p.getText)).toSeq
  }

  /**
   * Visit identifier list.
   */
  override def visitColumnList(ctx: LanceSqlExtensionsParser.ColumnListContext): Seq[String] = {
    ctx.columns.asScala.map(c => cleanIdentifier(c.getText)).toSeq
  }

  override def visitOptimize(ctx: LanceSqlExtensionsParser.OptimizeContext): Optimize = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val args = ctx.namedArgument().asScala.map(a =>
      LanceNamedArgument(
        cleanIdentifier(a.identifier().getText),
        a.constant().accept(this)))
      .toSeq

    Optimize(table, args)
  }

  override def visitVacuum(ctx: LanceSqlExtensionsParser.VacuumContext): Vacuum = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val args = ctx.namedArgument().asScala.map(a =>
      LanceNamedArgument(
        cleanIdentifier(a.identifier().getText),
        a.constant().accept(this)))
      .toSeq

    Vacuum(table, args)
  }

  override def visitCreateIndex(ctx: LanceSqlExtensionsParser.CreateIndexContext): AddIndex = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val indexName = cleanIdentifier(ctx.indexName.getText)
    val method = cleanIdentifier(ctx.method.getText)
    val columns = visitColumnList(ctx.columnList())
    val args = ctx.namedArgument().asScala.map(a =>
      LanceNamedArgument(
        cleanIdentifier(a.identifier().getText),
        a.constant().accept(this)))
      .toSeq

    AddIndex(table, indexName, method, columns, args)
  }

  override def visitShowIndexes(ctx: LanceSqlExtensionsParser.ShowIndexesContext): LogicalPlan = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    ShowIndexes(table)
  }

  override def visitDropIndex(ctx: LanceSqlExtensionsParser.DropIndexContext): LanceDropIndex = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val indexName = cleanIdentifier(ctx.indexName.getText)
    LanceDropIndex(table, indexName)
  }

  override def visitShowBranches(ctx: LanceSqlExtensionsParser.ShowBranchesContext)
      : ShowBranches = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    ShowBranches(table)
  }

  override def visitCreateBranch(ctx: LanceSqlExtensionsParser.CreateBranchContext)
      : CreateBranch = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val branchName = cleanIdentifier(ctx.branchName.getText)
    CreateBranch(table, branchName)
  }

  override def visitDropBranch(ctx: LanceSqlExtensionsParser.DropBranchContext): DropBranch = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val branchName = cleanIdentifier(ctx.branchName.getText)
    DropBranch(table, branchName)
  }

  override def visitShowTags(ctx: LanceSqlExtensionsParser.ShowTagsContext): ShowTags = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    ShowTags(table)
  }

  override def visitCreateTag(ctx: LanceSqlExtensionsParser.CreateTagContext): CreateTag = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val tagName = cleanIdentifier(ctx.tagName.getText)
    CreateTag(table, tagName)
  }

  override def visitDropTag(ctx: LanceSqlExtensionsParser.DropTagContext): DropTag = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val tagName = cleanIdentifier(ctx.tagName.getText)
    DropTag(table, tagName)
  }

  override def visitSetUnenforcedPrimaryKey(
      ctx: LanceSqlExtensionsParser.SetUnenforcedPrimaryKeyContext): SetUnenforcedPrimaryKey = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val columns = visitColumnList(ctx.columnList())
    SetUnenforcedPrimaryKey(table, columns)
  }

  override def visitStringLiteral(ctx: LanceSqlExtensionsParser.StringLiteralContext): String = {
    val text = ctx.getText
    text.stripPrefix("'").stripSuffix("'").stripPrefix("\"").stripSuffix("\"")
  }

  override def visitBooleanValue(ctx: LanceSqlExtensionsParser.BooleanValueContext)
      : java.lang.Boolean = {
    java.lang.Boolean.valueOf(ctx.getText)
  }

  override def visitBigIntLiteral(ctx: LanceSqlExtensionsParser.BigIntLiteralContext)
      : java.lang.Long = {
    java.lang.Long.valueOf(ctx.getText)
  }

  override def visitFloatLiteral(ctx: LanceSqlExtensionsParser.FloatLiteralContext)
      : java.lang.Float = {
    java.lang.Float.valueOf(ctx.getText)
  }

  override def visitDoubleLiteral(ctx: LanceSqlExtensionsParser.DoubleLiteralContext)
      : java.lang.Double = {
    java.lang.Double.valueOf(ctx.getText)
  }
}
