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
import org.apache.spark.sql.catalyst.plans.logical.{AddColumnsBackfill, AddIndex, CreateTag, DropTag, LogicalPlan, NamedArgument, Optimize, Vacuum}

import scala.jdk.CollectionConverters._

class LanceSqlExtensionsAstBuilder(delegate: ParserInterface)
  extends LanceSqlExtensionsBaseVisitor[AnyRef] {

  override def visitSingleStatement(ctx: LanceSqlExtensionsParser.SingleStatementContext)
      : LogicalPlan = {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitAddColumnsBackfill(ctx: LanceSqlExtensionsParser.AddColumnsBackfillContext)
      : AddColumnsBackfill = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val columnNames = visitColumnList(ctx.columnList())
    val source = UnresolvedRelation(Seq(ctx.identifier().getText))
    AddColumnsBackfill(table, columnNames, source)
  }

  override def visitMultipartIdentifier(ctx: LanceSqlExtensionsParser.MultipartIdentifierContext)
      : Seq[String] = {
    ctx.parts.asScala.map(_.getText).toSeq
  }

  /**
   * Visit identifier list.
   */
  override def visitColumnList(ctx: LanceSqlExtensionsParser.ColumnListContext): Seq[String] = {
    ctx.columns.asScala.map(_.getText).toSeq
  }

  override def visitOptimize(ctx: LanceSqlExtensionsParser.OptimizeContext): Optimize = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val args = ctx.namedArgument().asScala.map(a =>
      NamedArgument(
        a.identifier().getText,
        a.constant().accept(this)))
      .toSeq

    Optimize(table, args)
  }

  override def visitVacuum(ctx: LanceSqlExtensionsParser.VacuumContext): Vacuum = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val args = ctx.namedArgument().asScala.map(a =>
      NamedArgument(
        a.identifier().getText,
        a.constant().accept(this)))
      .toSeq

    Vacuum(table, args)
  }

  override def visitCreateIndex(ctx: LanceSqlExtensionsParser.CreateIndexContext): AddIndex = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val indexName = ctx.indexName.getText
    val method = ctx.method.getText
    val columns = visitColumnList(ctx.columnList())
    val args = ctx.namedArgument().asScala.map(a =>
      NamedArgument(
        a.identifier().getText,
        a.constant().accept(this)))
      .toSeq

    AddIndex(table, indexName, method, columns, args)
  }

  override def visitCreateTag(ctx: LanceSqlExtensionsParser.CreateTagContext): CreateTag = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val tagName = ctx.tagName.getText
    val version = Option(ctx.version).map(_.accept(this))
    CreateTag(table, tagName, version)
  }

  override def visitDropTag(ctx: LanceSqlExtensionsParser.DropTagContext): DropTag = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val tagName = ctx.tagName.getText
    DropTag(table, tagName)
  }

  override def visitNumericVersion(ctx: LanceSqlExtensionsParser.NumericVersionContext)
      : java.lang.Long = {
    ctx.number().accept(this).asInstanceOf[java.lang.Long]
  }

  override def visitStringVersion(ctx: LanceSqlExtensionsParser.StringVersionContext): String = {
    val text = ctx.STRING().getText
    // Remove surrounding quotes from the string literal
    text.substring(1, text.length - 1)
  }

  override def visitStringLiteral(ctx: LanceSqlExtensionsParser.StringLiteralContext): String = {
    ctx.getText
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
