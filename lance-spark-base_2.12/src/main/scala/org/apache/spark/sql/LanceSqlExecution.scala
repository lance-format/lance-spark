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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.lance.spark.{BaseLanceNamespaceSparkCatalog, TransactionProperties, TransactionPropertiesOperation}
import org.lance.spark.internal.TransactionPropertiesWriteTable

import java.util.{HashMap, Map => JMap}

object LanceSqlExecution {
  def execute(
      spark: SparkSession,
      sqlText: String,
      properties: JMap[String, String]): Unit = spark.withActive {
    val parsed = spark.sessionState.sqlParser.parsePlan(sqlText)
    def supportedStatement(plan: LogicalPlan): Boolean = plan match {
      case withClause: UnresolvedWith => supportedStatement(withClause.child)
      case _: UpdateTable | _: DeleteFromTable | _: MergeIntoTable | _: InsertIntoStatement |
          _: CreateTableAsSelect | _: ReplaceTableAsSelect | _: AddColumnsBackfill |
          _: UpdateColumnsBackfill => true
      case _ => false
    }
    require(
      supportedStatement(parsed),
      "Transaction properties require a supported Lance data-writing SQL statement")
    val encoded = TransactionProperties.encode(properties)
    def writeOptions(options: Map[String, String]): Map[String, String] = {
      require(
        !options.keys.exists(_.equalsIgnoreCase(TransactionProperties.option)),
        "Specify transaction properties only once")
      options + (TransactionProperties.option -> encoded)
    }
    def isLanceTarget(target: NamedRelation): Boolean = target match {
      case relation: DataSourceV2Relation =>
        relation.table.isInstanceOf[TransactionPropertiesWriteTable]
      case _ => false
    }
    def isLanceCatalog(name: LogicalPlan): Boolean = name match {
      case identifier: ResolvedIdentifier =>
        identifier.catalog.isInstanceOf[BaseLanceNamespaceSparkCatalog]
      case _ => false
    }
    var boundOperation = false
    def bindTarget(target: NamedRelation): NamedRelation = target match {
      case relation: DataSourceV2Relation =>
        relation.table match {
          case table: RowLevelOperationTable =>
            table.operation match {
              case operation: TransactionPropertiesOperation =>
                boundOperation = true
                relation.copy(table =
                  table.copy(operation = operation.withTransactionProperties(properties)))
              case _ => relation
            }
          case _ => relation
        }
      case other => other
    }
    val analyzed = spark.sessionState.executePlan(parsed).analyzed
    val bound = analyzed.transformDown {
      // RowLevelWrite's target is not a child traversed by transformDown.
      case write: RowLevelWrite => write.withNewTable(bindTarget(write.table))
      case append: AppendData if isLanceTarget(append.table) =>
        boundOperation = true
        append.copy(writeOptions = writeOptions(append.writeOptions))
      case overwrite: OverwriteByExpression if isLanceTarget(overwrite.table) =>
        boundOperation = true
        overwrite.copy(writeOptions = writeOptions(overwrite.writeOptions))
      case overwrite: OverwritePartitionsDynamic if isLanceTarget(overwrite.table) =>
        boundOperation = true
        overwrite.copy(writeOptions = writeOptions(overwrite.writeOptions))
      case write: V2WriteCommand if isLanceTarget(write.table) =>
        val relation = write.table.asInstanceOf[DataSourceV2Relation]
        val options = new HashMap[String, String](relation.options.asCaseSensitiveMap())
        require(
          !relation.options.containsKey(TransactionProperties.option),
          "Specify transaction properties only once")
        options.put(TransactionProperties.option, encoded)
        boundOperation = true
        write.withNewTable(relation.copy(options = new CaseInsensitiveStringMap(options)))
      case create: CreateTableAsSelect if isLanceCatalog(create.name) =>
        boundOperation = true
        create.copy(writeOptions = writeOptions(create.writeOptions))
      case replace: ReplaceTableAsSelect if isLanceCatalog(replace.name) =>
        boundOperation = true
        replace.copy(writeOptions = writeOptions(replace.writeOptions))
      case add: AddColumnsBackfill if isLanceCatalog(add.table) =>
        boundOperation = true
        add.copy(writeOptions = writeOptions(add.writeOptions))
      case update: UpdateColumnsBackfill if isLanceCatalog(update.table) =>
        boundOperation = true
        update.copy(writeOptions = writeOptions(update.writeOptions))
    }
    require(
      boundOperation,
      "Transaction properties require a supported Lance write target")
    spark.sessionState.executePlan(bound).assertCommandExecuted()
  }
}
