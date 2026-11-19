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

// Run in a Spark shell with the Lance extension and catalog configured, then call run(spark).

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{ArrayType, FloatType, MetadataBuilder}
import org.apache.xbean.asm9.{ClassReader, ClassVisitor, MethodVisitor, Opcodes, Type}
import org.lance.spark.extensions.LanceSparkSessionExtensions

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.jar.JarFile

import scala.collection.JavaConverters._
import scala.collection.mutable

object CatalystCompatibilityValidation {

  private val sparkOwnerPrefix = "org/apache/spark/"
  private val connectorOwnerPrefixes = Seq("org/lance/spark/", sparkOwnerPrefix)
  private val unstableNodeClasses = Set(
    "org.apache.spark.sql.catalyst.plans.logical.AppendData",
    "org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable",
    "org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression",
    "org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic",
    "org.apache.spark.sql.catalyst.plans.logical.Sort")
  private val companionFactories = Set("apply", "byName", "byPosition")
  private val sparkVersionPattern = """^(\d+)\.(\d+)(?:\..*)?$""".r

  private case class MemberReference(kind: String, owner: String, name: String, descriptor: String)

  def run(spark: SparkSession): Unit = {
    val sources =
      Seq(classOf[LanceSparkSessionExtensions], CatalystCompat.getClass).map(codeSourceOf).distinct
    validateLinkage(sources, Thread.currentThread().getContextClassLoader)
    validateWriteOperations(spark)
  }

  private def validateLinkage(sources: Seq[Path], loader: ClassLoader): Unit = {
    val classNamesBySource = sources.map(source => source -> classNamesAt(source))
    val bundledClasses = classNamesBySource.flatMap(_._2).toSet
    val classesToAudit = classNamesBySource
      .flatMap { case (source, classNames) =>
        classNames.filter(name => connectorOwnerPrefixes.exists(name.startsWith)).map(source -> _)
      }
      .groupBy(_._2)
      .values
      .map(_.head)
      .toSeq
      .sortBy(_._2)
    require(classesToAudit.nonEmpty, s"No connector classes found in ${sources.mkString(", ")}")

    val referencesByCaller = mutable.Map.empty[String, mutable.Set[MemberReference]]
    classesToAudit.groupBy(_._1).foreach { case (source, classes) =>
      scanClassFiles(source, classes.map(_._2)) { (className, classBytes) =>
        val references = referencesByCaller.getOrElseUpdate(className, mutable.Set.empty)
        new ClassReader(classBytes).accept(
          new ClassVisitor(Opcodes.ASM9) {
            override def visitMethod(
                access: Int,
                name: String,
                descriptor: String,
                signature: String,
                exceptions: Array[String]): MethodVisitor =
              new MethodVisitor(Opcodes.ASM9) {
                override def visitMethodInsn(
                    opcode: Int,
                    owner: String,
                    calledName: String,
                    calledDescriptor: String,
                    isInterface: Boolean): Unit =
                  references +=
                    MemberReference("method", owner, calledName, calledDescriptor)

                override def visitFieldInsn(
                    opcode: Int,
                    owner: String,
                    fieldName: String,
                    fieldDescriptor: String): Unit =
                  references += MemberReference("field", owner, fieldName, fieldDescriptor)

                override def visitTypeInsn(opcode: Int, typeName: String): Unit =
                  references += MemberReference("class", typeName, "", "")
              }
          },
          ClassReader.SKIP_DEBUG)
      }
    }

    val classCache = mutable.Map.empty[String, Either[String, Class[_]]]
    def resolveClass(owner: String): Either[String, Class[_]] =
      classCache.getOrElseUpdate(
        owner,
        try Right(Class.forName(owner.replace('/', '.'), false, loader))
        catch {
          case error: ClassNotFoundException => Left(error.getClass.getSimpleName)
          case error: LinkageError => Left(error.getClass.getSimpleName)
        })

    val violations = mutable.ArrayBuffer.empty[String]
    referencesByCaller.toSeq.sortBy(_._1).foreach { case (caller, references) =>
      references
        .filter(reference =>
          reference.owner.startsWith(sparkOwnerPrefix) &&
            !bundledClasses.contains(reference.owner))
        .foreach { reference =>
          resolveClass(reference.owner) match {
            case Left(error) =>
              violations += s"MISSING_CLASS ${reference.owner} ($error) <- $caller"
            case Right(runtimeClass) =>
              reference.kind match {
                case "method"
                    if !allMethods(runtimeClass).contains(reference.name -> reference.descriptor) =>
                  violations +=
                    s"MISSING_METHOD ${reference.owner}.${reference.name}${reference.descriptor} <- $caller"
                case "field"
                    if !allFields(runtimeClass).contains(reference.name -> reference.descriptor) =>
                  violations +=
                    s"MISSING_FIELD ${reference.owner}.${reference.name}:${reference.descriptor} <- $caller"
                case "method" if bindsToUnstableSignature(reference, runtimeClass) =>
                  violations +=
                    s"UNSTABLE_SIGNATURE ${reference.owner}.${reference.name} <- $caller"
                case _ =>
              }
          }
        }
    }

    require(
      violations.isEmpty,
      s"Catalyst linkage audit found ${violations.size} issue(s):\n${violations.distinct.sorted.mkString("\n")}")
    println(s"Catalyst linkage audit passed (${classesToAudit.size} connector classes)")
  }

  private def bindsToUnstableSignature(
      reference: MemberReference,
      runtimeClass: Class[_]): Boolean = {
    val owner = reference.owner.replace('/', '.')
    val isTreeNodeCopy =
      reference.name == "copy" && classOf[TreeNode[_]].isAssignableFrom(runtimeClass)
    val isUnstableConstructor =
      reference.name == "<init>" && unstableNodeClasses.contains(owner)
    val isUnstableFactory =
      companionFactories.contains(reference.name) &&
        owner.endsWith("$") &&
        unstableNodeClasses.contains(owner.stripSuffix("$"))
    isTreeNodeCopy || isUnstableConstructor || isUnstableFactory
  }

  private def allMethods(runtimeClass: Class[_]): Set[(String, String)] = {
    val methods = mutable.Set.empty[(String, String)]
    val pending = mutable.Queue[Class[_]](runtimeClass)
    val visited = mutable.Set.empty[Class[_]]
    while (pending.nonEmpty) {
      val current = pending.dequeue()
      if (current != null && visited.add(current)) {
        current.getDeclaredMethods.foreach(method =>
          methods += method.getName -> Type.getMethodDescriptor(method))
        current.getDeclaredConstructors.foreach(constructor =>
          methods += "<init>" -> Type.getConstructorDescriptor(constructor))
        Option(current.getSuperclass).foreach(pending.enqueue(_))
        current.getInterfaces.foreach(pending.enqueue(_))
      }
    }
    methods.toSet
  }

  private def allFields(runtimeClass: Class[_]): Set[(String, String)] = {
    val fields = mutable.Set.empty[(String, String)]
    val pending = mutable.Queue[Class[_]](runtimeClass)
    val visited = mutable.Set.empty[Class[_]]
    while (pending.nonEmpty) {
      val current = pending.dequeue()
      if (current != null && visited.add(current)) {
        current.getDeclaredFields.foreach(field =>
          fields += field.getName -> Type.getDescriptor(field.getType))
        Option(current.getSuperclass).foreach(pending.enqueue(_))
        current.getInterfaces.foreach(pending.enqueue(_))
      }
    }
    fields.toSet
  }

  private def classNamesAt(source: Path): Set[String] =
    if (Files.isDirectory(source)) {
      val paths = Files.walk(source)
      try {
        paths.iterator().asScala
          .filter(path => path.toString.endsWith(".class"))
          .map(path =>
            source.relativize(path).toString
              .replace(File.separatorChar, '/')
              .stripSuffix(".class"))
          .toSet
      } finally {
        paths.close()
      }
    } else {
      val jar = new JarFile(source.toFile)
      try {
        jar.entries().asScala
          .map(_.getName)
          .filter(_.endsWith(".class"))
          .map(_.stripSuffix(".class"))
          .toSet
      } finally {
        jar.close()
      }
    }

  private def scanClassFiles(
      source: Path,
      classNames: Seq[String])(inspect: (String, Array[Byte]) => Unit): Unit =
    if (Files.isDirectory(source)) {
      classNames.foreach { className =>
        inspect(className, Files.readAllBytes(source.resolve(s"$className.class")))
      }
    } else {
      val jar = new JarFile(source.toFile)
      try {
        classNames.foreach { className =>
          val input = jar.getInputStream(jar.getJarEntry(s"$className.class"))
          val classBytes =
            try input.readAllBytes()
            finally input.close()
          inspect(className, classBytes)
        }
      } finally {
        jar.close()
      }
    }

  private def codeSourceOf(clazz: Class[_]): Path =
    Option(clazz.getProtectionDomain.getCodeSource)
      .map(source => Paths.get(source.getLocation.toURI))
      .getOrElse(throw new IllegalStateException(s"No code source for ${clazz.getName}"))

  private def validateWriteOperations(spark: SparkSession): Unit = {
    val configuredExtensions =
      spark.conf.getOption("spark.sql.extensions").toSeq.flatMap(_.split(",")).map(_.trim)
    require(
      configuredExtensions.contains(classOf[LanceSparkSessionExtensions].getName),
      "The Spark session must be created with LanceSparkSessionExtensions")
    require(
      spark.conf.getOption("spark.sql.catalog.lance").contains(
        "org.lance.spark.LanceNamespaceSparkCatalog"),
      "The Spark session must configure the Lance catalog")

    val namespace = s"compat_${java.util.UUID.randomUUID().toString.replace("-", "")}"
    val table = s"lance.$namespace.items"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS lance.$namespace")
    try {
      spark.sql(s"CREATE TABLE $table (id INT, name STRING) USING lance")
      spark.sql(s"INSERT INTO $table VALUES (1, 'one'), (2, 'two')")
      require(spark.sql(s"SELECT count(*) FROM $table").head().getLong(0) == 2L)

      spark.sql(s"INSERT OVERWRITE $table SELECT 3, 'three'")
      val metadata = new MetadataBuilder().putLong("arrow.fixed-size-list.size", 1024L).build()
      spark.table(table).select(
        col("_rowaddr"),
        col("_fragid"),
        lit(null).cast(ArrayType(FloatType, containsNull = false)).as("embedding", metadata))
        .createOrReplaceTempView(namespace)
      spark.sql(s"ALTER TABLE $table ADD COLUMNS embedding FROM $namespace")
      require(spark.sql(s"SELECT embedding FROM $table").collect().toSeq == Seq(Row(null)))
      val expectedRows =
        if (supportsRowLevelDml(spark.version)) {
          spark.sql(
            s"""MERGE INTO $table AS target
               |USING (
               |  SELECT 3 AS id, 'merged' AS name, array_repeat(CAST(0.25 AS FLOAT), 1024) AS embedding
               |  UNION ALL
               |  SELECT 4 AS id, 'four' AS name, array_repeat(CAST(0.5 AS FLOAT), 1024) AS embedding
               |) AS source
               |ON target.id = source.id
               |WHEN MATCHED THEN UPDATE SET *
               |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
          spark.sql(s"UPDATE $table SET name = 'updated' WHERE id = 3")
          spark.sql(s"DELETE FROM $table WHERE id = 4")
          require(
            spark.sql(s"SELECT embedding FROM $table").head().getSeq[Float](0) == Seq.fill(1024)(
              0.25f))
          Seq(Row(3, "updated"))
        } else {
          println(
            s"Skipping MERGE, UPDATE, and DELETE smoke tests on Spark ${spark.version}; " +
              "row-level DML requires Spark 3.5+")
          Seq(Row(3, "three"))
        }

      val rows = spark.sql(s"SELECT id, name FROM $table ORDER BY id").collect().toSeq
      require(rows == expectedRows, s"Unexpected rows: $rows")
      println("Catalyst write-operation smoke test passed")
    } finally {
      spark.catalog.dropTempView(namespace)
      spark.sql(s"DROP TABLE IF EXISTS $table")
      spark.sql(s"DROP NAMESPACE IF EXISTS lance.$namespace")
    }
  }

  private def supportsRowLevelDml(sparkVersion: String): Boolean =
    sparkVersion match {
      case sparkVersionPattern(majorVersion, minorVersion) =>
        majorVersion.toInt > 3 || (majorVersion.toInt == 3 && minorVersion.toInt >= 5)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported Spark version format: $sparkVersion")
    }
}
