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
package org.lance.spark.update;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.extensions.LanceSqlExtensionsAstBuilder;
import org.apache.spark.sql.catalyst.parser.extensions.LanceSqlExtensionsLexer;
import org.apache.spark.sql.catalyst.parser.extensions.LanceSqlExtensionsParser;
import org.apache.spark.sql.catalyst.plans.logical.AddColumnsBackfill;
import org.apache.spark.sql.catalyst.plans.logical.AddIndex;
import org.apache.spark.sql.catalyst.plans.logical.Optimize;
import org.apache.spark.sql.catalyst.plans.logical.ShowIndexes;
import org.apache.spark.sql.catalyst.plans.logical.UpdateColumnsBackfill;
import org.apache.spark.sql.catalyst.plans.logical.Vacuum;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that the AST builder correctly strips backticks from BACKQUOTED_IDENTIFIER tokens. This
 * validates the fix for issue #225: table names with hyphens (e.g. {@code `my-table`}) must have
 * backticks removed during parsing.
 */
public class LanceSqlExtensionsAstBuilderTest {

  // Null delegate is safe: the visitor methods under test never invoke the delegate parser.
  private final LanceSqlExtensionsAstBuilder astBuilder = new LanceSqlExtensionsAstBuilder(null);

  private LanceSqlExtensionsParser createParser(String sql) {
    LanceSqlExtensionsLexer lexer = new LanceSqlExtensionsLexer(CharStreams.fromString(sql));
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    return new LanceSqlExtensionsParser(tokenStream);
  }

  // --- Unit tests for visitMultipartIdentifier / visitColumnList ---

  @Test
  public void testMultipartIdentifierWithoutBackticks() {
    // The grammar's IDENTIFIER rule only matches [A-Z0-9_]+
    LanceSqlExtensionsParser parser = createParser("CATALOG.SCHEMA.TABLE_NAME");
    List<String> parts =
        JavaConverters.seqAsJavaList(
            astBuilder.visitMultipartIdentifier(parser.multipartIdentifier()));
    assertEquals(List.of("CATALOG", "SCHEMA", "TABLE_NAME"), parts);
  }

  @Test
  public void testMultipartIdentifierWithBackticks() {
    // Backticks preserve case and allow hyphens
    LanceSqlExtensionsParser parser = createParser("`my-catalog`.`my-schema`.`my-table`");
    List<String> parts =
        JavaConverters.seqAsJavaList(
            astBuilder.visitMultipartIdentifier(parser.multipartIdentifier()));
    assertEquals(List.of("my-catalog", "my-schema", "my-table"), parts);
  }

  @Test
  public void testMultipartIdentifierMixedQuoting() {
    // The grammar's IDENTIFIER rule only matches [A-Z0-9_]+, so unquoted parts must be uppercase.
    // Backtick-quoted parts preserve their original case.
    LanceSqlExtensionsParser parser = createParser("CATALOG.`my-schema`.TABLE_NAME");
    List<String> parts =
        JavaConverters.seqAsJavaList(
            astBuilder.visitMultipartIdentifier(parser.multipartIdentifier()));
    assertEquals(List.of("CATALOG", "my-schema", "TABLE_NAME"), parts);
  }

  @Test
  public void testMultipartIdentifierEscapedBacktick() {
    // Doubled backtick (`` ``) inside a backtick-quoted identifier should be unescaped to a
    // single backtick
    LanceSqlExtensionsParser parser = createParser("`my``table`");
    List<String> parts =
        JavaConverters.seqAsJavaList(
            astBuilder.visitMultipartIdentifier(parser.multipartIdentifier()));
    assertEquals(List.of("my`table"), parts);
  }

  @Test
  public void testColumnListWithBackticks() {
    // Unquoted identifier must be uppercase to match the grammar's IDENTIFIER rule
    LanceSqlExtensionsParser parser = createParser("`col-a`, `col-b`, NORMAL_COL");
    List<String> columns =
        JavaConverters.seqAsJavaList(astBuilder.visitColumnList(parser.columnList()));
    assertEquals(List.of("col-a", "col-b", "NORMAL_COL"), columns);
  }

  // --- Full-statement parse tests ---

  @Test
  public void testAddColumnsBackfillWithBacktickedIdentifiers() {
    // ALTER TABLE `my-catalog`.`my-table` ADD COLUMNS `col-a` FROM `source-view`
    LanceSqlExtensionsParser parser =
        createParser("ALTER TABLE `my-catalog`.`my-table` ADD COLUMNS `col-a` FROM `source-view`");
    AddColumnsBackfill plan =
        (AddColumnsBackfill) astBuilder.visitSingleStatement(parser.singleStatement());

    // Table identifier: backticks stripped
    UnresolvedIdentifier table = (UnresolvedIdentifier) plan.table();
    assertEquals(
        List.of("my-catalog", "my-table"), JavaConverters.seqAsJavaList(table.nameParts()));

    // Column names: backticks stripped
    assertEquals(List.of("col-a"), JavaConverters.seqAsJavaList(plan.columnNames()));

    // Source view: backticks stripped
    UnresolvedRelation source = (UnresolvedRelation) plan.source();
    assertEquals(
        List.of("source-view"), JavaConverters.seqAsJavaList(source.multipartIdentifier()));
  }

  @Test
  public void testCreateIndexWithBacktickedIdentifiers() {
    // ALTER TABLE `my-catalog`.`my-table` CREATE INDEX `my-idx` USING BTREE (`col-a`)
    LanceSqlExtensionsParser parser =
        createParser(
            "ALTER TABLE `my-catalog`.`my-table` CREATE INDEX `my-idx` USING BTREE (`col-a`)");
    AddIndex plan = (AddIndex) astBuilder.visitSingleStatement(parser.singleStatement());

    UnresolvedIdentifier table = (UnresolvedIdentifier) plan.table();
    assertEquals(
        List.of("my-catalog", "my-table"), JavaConverters.seqAsJavaList(table.nameParts()));
    assertEquals("my-idx", plan.indexName());
    assertEquals("BTREE", plan.method());
    assertEquals(List.of("col-a"), JavaConverters.seqAsJavaList(plan.columns()));
  }

  @Test
  public void testOptimizeWithBacktickedTableName() {
    LanceSqlExtensionsParser parser = createParser("OPTIMIZE `my-catalog`.`my-table`");
    Optimize plan = (Optimize) astBuilder.visitSingleStatement(parser.singleStatement());

    UnresolvedIdentifier table = (UnresolvedIdentifier) plan.table();
    assertEquals(
        List.of("my-catalog", "my-table"), JavaConverters.seqAsJavaList(table.nameParts()));
  }

  @Test
  public void testShowIndexesWithBacktickedTableName() {
    LanceSqlExtensionsParser parser = createParser("SHOW INDEXES FROM `my-catalog`.`my-table`");
    ShowIndexes plan = (ShowIndexes) astBuilder.visitSingleStatement(parser.singleStatement());

    UnresolvedIdentifier table = (UnresolvedIdentifier) plan.table();
    assertEquals(
        List.of("my-catalog", "my-table"), JavaConverters.seqAsJavaList(table.nameParts()));
  }

  @Test
  public void testUpdateColumnsBackfillWithBacktickedIdentifiers() {
    LanceSqlExtensionsParser parser =
        createParser(
            "ALTER TABLE `my-catalog`.`my-table` UPDATE COLUMNS `col-a` FROM `source-view`");
    UpdateColumnsBackfill plan =
        (UpdateColumnsBackfill) astBuilder.visitSingleStatement(parser.singleStatement());

    UnresolvedIdentifier table = (UnresolvedIdentifier) plan.table();
    assertEquals(
        List.of("my-catalog", "my-table"), JavaConverters.seqAsJavaList(table.nameParts()));
    assertEquals(List.of("col-a"), JavaConverters.seqAsJavaList(plan.columnNames()));

    UnresolvedRelation source = (UnresolvedRelation) plan.source();
    assertEquals(
        List.of("source-view"), JavaConverters.seqAsJavaList(source.multipartIdentifier()));
  }

  @Test
  public void testVacuumWithBacktickedTableName() {
    LanceSqlExtensionsParser parser = createParser("VACUUM `my-catalog`.`my-table`");
    Vacuum plan = (Vacuum) astBuilder.visitSingleStatement(parser.singleStatement());

    UnresolvedIdentifier table = (UnresolvedIdentifier) plan.table();
    assertEquals(
        List.of("my-catalog", "my-table"), JavaConverters.seqAsJavaList(table.nameParts()));
  }
}
