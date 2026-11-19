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
package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.plans.logical.{AppendData, MergeIntoTable, OverwriteByExpression, OverwritePartitionsDynamic, Sort}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.xbean.asm9.{ClassReader, ClassVisitor, MethodVisitor, Opcodes}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Fails when Lance bytecode binds to the exact signature of a Spark Catalyst node, which breaks on
 * runtimes whose Catalyst classes have a different shape. See [[CatalystCompat]] for the
 * replacements.
 */
class CatalystBinaryCompatibilityTest {

  private val lanceClassesRoot: Path = codeSourceOf(CatalystCompat.getClass).get

  // Nodes whose constructor has already diverged between Spark releases or vendor runtimes.
  private val nodesWithUnstableConstructors: Set[String] = Set(
    classOf[Sort],
    classOf[AppendData],
    classOf[OverwriteByExpression],
    classOf[OverwritePartitionsDynamic],
    classOf[MergeIntoTable]).map(_.getName)

  private val companionFactories = Set("apply", "byName", "byPosition")

  @Test
  def lanceCodeDoesNotBindToSparkNodeSignatures(): Unit = {
    val classFiles = Files.walk(lanceClassesRoot).iterator().asScala
      .filter(_.toString.endsWith(".class"))
      .toList
    assertTrue(classFiles.nonEmpty, s"No classes found under $lanceClassesRoot")

    val violations = classFiles.flatMap(signatureBindingsIn)

    assertTrue(
      violations.isEmpty,
      "Use CatalystCompat instead of binding to these Spark node signatures:\n" +
        violations.mkString("\n"))
  }

  @Test
  def detectsACompiledCopyOfASparkNode(): Unit = {
    val testClass = classOf[CatalystBinaryCompatibilityTest]
    val thisClassFile = codeSourceOf(testClass).get
      .resolve(testClass.getName.replace('.', '/') + ".class")

    val violations = signatureBindingsIn(thisClassFile)

    assertTrue(
      violations.exists(_.contains(s"${classOf[Sort].getName}.copy")),
      violations.mkString("\n"))
  }

  private def bindsToSortCopy(sort: Sort): Sort = sort.copy(global = false)

  private def signatureBindingsIn(classFile: Path): Seq[String] = {
    val violations = ArrayBuffer.empty[String]
    val reader = new ClassReader(Files.readAllBytes(classFile))
    reader.accept(
      new ClassVisitor(Opcodes.ASM9) {
        override def visitMethod(
            access: Int,
            methodName: String,
            descriptor: String,
            signature: String,
            exceptions: Array[String]): MethodVisitor =
          new MethodVisitor(Opcodes.ASM9) {
            override def visitMethodInsn(
                opcode: Int,
                owner: String,
                calledName: String,
                calledDescriptor: String,
                isInterface: Boolean): Unit = {
              val ownerClassName = owner.replace('/', '.')
              if (bindsToSignature(ownerClassName, calledName)) {
                violations +=
                  s"${reader.getClassName.replace('/', '.')}.$methodName calls " +
                    s"$ownerClassName.$calledName"
              }
            }
          }
      },
      ClassReader.SKIP_DEBUG)
    violations.distinct.toSeq
  }

  private def bindsToSignature(ownerClassName: String, calledName: String): Boolean = {
    val isCopy = calledName == "copy"
    val isConstructor = calledName == "<init>" &&
      nodesWithUnstableConstructors.contains(ownerClassName)
    val isCompanionFactory = companionFactories.contains(calledName) &&
      ownerClassName.endsWith("$") &&
      nodesWithUnstableConstructors.contains(ownerClassName.stripSuffix("$"))
    (isCopy && isSparkTreeNode(ownerClassName)) || isConstructor || isCompanionFactory
  }

  private def isSparkTreeNode(className: String): Boolean =
    loadClass(className).exists(loaded =>
      classOf[TreeNode[_]].isAssignableFrom(loaded) &&
        !codeSourceOf(loaded).contains(lanceClassesRoot))

  private def loadClass(className: String): Option[Class[_]] =
    try Some(Class.forName(className, false, getClass.getClassLoader))
    catch { case _: ClassNotFoundException | _: LinkageError => None }

  private def codeSourceOf(loaded: Class[_]): Option[Path] =
    Option(loaded.getProtectionDomain.getCodeSource)
      .map(source => Paths.get(source.getLocation.toURI))
}
