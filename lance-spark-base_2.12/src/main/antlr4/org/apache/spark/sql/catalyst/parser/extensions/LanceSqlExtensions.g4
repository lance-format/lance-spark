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

grammar LanceSqlExtensions;

singleStatement
    : statement EOF
    ;

statement
    : ALTER TABLE multipartIdentifier ADD COLUMNS columnList FROM identifier                    #addColumnsBackfill
    | ALTER TABLE multipartIdentifier CREATE INDEX indexName=identifier USING method=identifier '(' columnList ')' (WITH '(' (namedArgument (',' namedArgument)*)? ')')? #createIndex
    | ALTER TABLE multipartIdentifier CREATE TAG tagName=identifier (VERSION AS OF version=versionValue)? #createTag
    | ALTER TABLE multipartIdentifier DROP TAG tagName=identifier                               #dropTag
    | OPTIMIZE multipartIdentifier (WITH '(' (namedArgument (',' namedArgument)*)? ')')?        #optimize
    | VACUUM multipartIdentifier (WITH '(' (namedArgument (',' namedArgument)*)? ')')?          #vacuum
    ;

versionValue
    : number                          #numericVersion
    | STRING                          #stringVersion
    ;

multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

columnList
    : columns+=identifier (',' columns+=identifier)*
    ;

namedArgument
    : identifier '=' constant
    ;

constant
    : number                          #numericLiteral
    | booleanValue                    #booleanLiteral
    | STRING+                         #stringLiteral
    ;

booleanValue
    : TRUE | FALSE
    ;

number
    : MINUS? BIGINT_LITERAL             #bigIntLiteral
    | MINUS? FLOAT_LITERAL              #floatLiteral
    | MINUS? DOUBLE_LITERAL             #doubleLiteral
    ;

ADD: 'ADD';
ALTER: 'ALTER';
AS: 'AS';
COLUMNS: 'COLUMNS';
CREATE: 'CREATE';
DROP: 'DROP';
FROM: 'FROM';
INDEX: 'INDEX';
OF: 'OF';
OPTIMIZE: 'OPTIMIZE';
TABLE: 'TABLE';
TAG: 'TAG';
USING: 'USING';
VACUUM: 'VACUUM';
VERSION: 'VERSION';
WITH: 'WITH';

TRUE: 'TRUE';
FALSE: 'FALSE';

PLUS: '+';
MINUS: '-';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+
    ;

FLOAT_LITERAL
    : FLOAT_FRAGMENT EXP? [fF]
    | FLOAT_FRAGMENT EXP
    | DIGIT+ '.' DIGIT+
    ;

DOUBLE_LITERAL
    : FLOAT_FRAGMENT EXP? [dD]
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment FLOAT_FRAGMENT
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXP : [eE] [+-]? DIGIT+ ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;


