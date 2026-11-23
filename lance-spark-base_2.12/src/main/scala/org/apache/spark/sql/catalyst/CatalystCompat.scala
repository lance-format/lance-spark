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

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.trees.TreeNode

import java.lang.invoke.MethodType
import java.lang.reflect.{Constructor, InvocationTargetException, Method}

/**
 * Builds and modifies Catalyst nodes without binding to their exact constructor signature.
 *
 * Spark releases and distributions can add fields to Catalyst case classes. A compiled `copy(...)`,
 * constructor or companion factory call binds to the exact JVM signature of the Spark build it was
 * compiled against and can fail with `NoSuchMethodError` when the runtime class has a different
 * shape. Lance code must therefore
 * prefer Spark's rewrite methods and use [[CatalystCompat.TreeNodeArgs.withArg]] for fields without
 * an equivalent API. Nodes with unstable constructors need runtime-aware factories.
 */
object CatalystCompat {

  implicit class TreeNodeArgs[T <: TreeNode[_]](node: T) {

    /**
     * Replaces the product argument read by `field` through Spark's reflective `makeCopy` path.
     * Reference identity and accessor checks disambiguate fields sharing values such as Nil or None.
     * Prefer Spark's rewrite methods when they provide the same operation.
     */
    def withArg[A <: AnyRef](field: T => A)(value: A): T = {
      val current = field(node)
      if (current eq value) {
        return node
      }
      val args = node.productIterator.map(_.asInstanceOf[AnyRef]).toArray
      val candidates = args.indices.filter(index => args(index) eq current)
      val constructors = node.getClass.getConstructors.filter(_.getParameterCount >= args.length)
      // Several arguments may share the same instance (e.g. Nil or None); keep the one that the
      // accessor reads back from the copy.
      val copies = candidates
        .filter(index => acceptsReplacement(constructors, args, index, value))
        .map(index => copyReplacing(args, index, value))
        .filter(copy => field(copy) eq value)
      copies match {
        case Seq(copy) => copy
        case Seq() =>
          throw new IllegalStateException(
            s"Cannot locate the constructor argument to replace in ${node.getClass.getName}")
        case _ =>
          throw new IllegalStateException(
            s"Ambiguous constructor argument to replace in ${node.getClass.getName}")
      }
    }

    private def copyReplacing(args: Array[AnyRef], index: Int, value: AnyRef): T =
      node.makeCopy(args.updated(index, value)).asInstanceOf[T]

    private def acceptsReplacement(
        constructors: Array[Constructor[_]],
        args: Array[AnyRef],
        index: Int,
        value: AnyRef): Boolean =
      constructors.exists { constructor =>
        constructor.getParameterTypes.take(args.length).zipWithIndex.forall {
          case (parameterType, argumentIndex) =>
            val argument = if (argumentIndex == index) value else args(argumentIndex)
            accepts(parameterType, argument)
        }
      }

    private def accepts(parameterType: Class[_], value: AnyRef): Boolean =
      if (value == null) {
        !parameterType.isPrimitive
      } else {
        MethodType.methodType(parameterType).wrap().returnType().isInstance(value)
      }
  }

  def sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan): Sort =
    invokeCompanionFactory(classOf[Sort], "apply", order, Boolean.box(global), child)

  /**
   * Invokes the companion method `factoryName` of `nodeClass` with `args` as its leading
   * arguments, filling any further parameters from their Scala default values. Of the matching
   * overloads, the one with the fewest parameters is used.
   */
  private[catalyst] def invokeCompanionFactory[T](
      nodeClass: Class[T],
      factoryName: String,
      args: AnyRef*): T = {
    require(args.forall(_ != null), s"Null argument for ${nodeClass.getName}.$factoryName")
    val companionClass = Class.forName(nodeClass.getName + "$", true, nodeClass.getClassLoader)
    val companion = companionClass.getField("MODULE$").get(null)
    val applicable = companionClass.getMethods.toSeq.filter { method =>
      method.getName == factoryName && !method.isBridge &&
      nodeClass.isAssignableFrom(method.getReturnType) &&
      acceptsLeadingArguments(method, args) &&
      (args.length until method.getParameterCount).forall(index =>
        defaultValueMethod(companionClass, factoryName, index).isDefined)
    }
    val factory = applicable.sortBy(_.getParameterCount) match {
      case Seq(only) => only
      case Seq(first, second, _*) if first.getParameterCount < second.getParameterCount => first
      case Seq() =>
        throw new IllegalStateException(
          s"No ${nodeClass.getName}.$factoryName accepts " +
            args.map(_.getClass.getName).mkString("(", ", ", ")"))
      case _ =>
        throw new IllegalStateException(s"Ambiguous ${nodeClass.getName}.$factoryName overloads")
    }
    val defaults = (args.length until factory.getParameterCount).map(index =>
      invoke(defaultValueMethod(companionClass, factoryName, index).get, companion))
    nodeClass.cast(invoke(factory, companion, args ++ defaults: _*))
  }

  private def acceptsLeadingArguments(method: Method, args: Seq[AnyRef]): Boolean =
    method.getParameterCount >= args.length &&
      method.getParameterTypes.zip(args).forall { case (parameterType, arg) =>
        MethodType.methodType(parameterType).wrap().returnType().isInstance(arg)
      }

  // Scala compiles the default of parameter i (0-based) to a companion method `<name>$default$<i+1>`.
  private def defaultValueMethod(
      companionClass: Class[_],
      factoryName: String,
      index: Int): Option[Method] =
    companionClass.getMethods.find(method =>
      method.getName == s"$factoryName$$default$$${index + 1}" && method.getParameterCount == 0)

  private def invoke(method: Method, target: AnyRef, args: AnyRef*): AnyRef =
    try {
      method.invoke(target, args: _*)
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
}
