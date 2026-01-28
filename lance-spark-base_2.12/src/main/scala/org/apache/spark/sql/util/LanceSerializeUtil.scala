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
package org.apache.spark.sql.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.util.Utils
import org.objenesis.strategy.StdInstantiatorStrategy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64

object LanceSerializeUtil {
  private val kryo: ThreadLocal[Kryo] = new ThreadLocal[Kryo] {
    override def initialValue(): Kryo = {
      val kryo = new Kryo()
      kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()))
      kryo.setClassLoader(Utils.getContextOrSparkClassLoader)
      kryo
    }
  }

  def encode[T](obj: T): String = {
    val k = kryo.get
    k.setClassLoader(Utils.getContextOrSparkClassLoader)
    val buffer = new ByteArrayOutputStream()
    val output = new Output(buffer)
    k.writeClassAndObject(output, obj)
    output.close()
    Base64.getEncoder.encodeToString(buffer.toByteArray)
  }

  def decode[T](obj: String): T = {
    val k = kryo.get
    k.setClassLoader(Utils.getContextOrSparkClassLoader)
    val array = Base64.getDecoder.decode(obj)
    val input = new Input(new ByteArrayInputStream(array))
    val o = k.readClassAndObject(input)
    input.close()
    o.asInstanceOf[T]
  }
}
